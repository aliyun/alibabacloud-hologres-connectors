"""Worker process for multi-process write mode.

Each worker process runs independently with its own:
- psycopg.Connection
- ActionCollector (1 shard)
- Background flush timer thread

The parent routes records to workers via shared memory ring buffers,
using distribution key hashing to ensure same-PK records go to the
same worker.
"""

from __future__ import annotations

import logging
import multiprocessing
import threading
import time
from typing import Any, Dict, List, Optional

import psycopg

from ._collector import ActionCollector
from ._sql import build_delete_sql, build_insert_sql
from .config import HoloConfig
from .exceptions import HoloClientException, HoloClientWithDetailsException
from .record import Record
from .table_name import TableName
from .table_schema import TableSchema
from .types import ExceptionCode, WriteFailStrategy

logger = logging.getLogger("hologres.worker")


# ── Internal helpers ──────────────────────────────────────────────────


def _connect(config: HoloConfig) -> psycopg.Connection:
    """Create a new psycopg connection."""
    ci = config.fixed_fe_conninfo if config.use_fixed_fe else config.conninfo
    conn = psycopg.connect(ci, autocommit=True)
    if not config.enable_generate_binlog:
        with conn.cursor() as cur:
            cur.execute("SET hg_experimental_generate_binlog = off")
    return conn


def _reconnect(
    config: HoloConfig, old_conn: Optional[psycopg.Connection]
) -> psycopg.Connection:
    """Close old connection and create a new one."""
    if old_conn is not None:
        try:
            old_conn.close()
        except Exception:
            pass
    return _connect(config)


def _do_flush(
    collector: ActionCollector,
    conn: Optional[psycopg.Connection],
    config: HoloConfig,
    force: bool,
) -> psycopg.Connection:
    """Flush ready batches from the collector to the database.

    Caller must hold flush_lock.
    Returns the connection (may be a new one if reconnection occurred).
    """
    tables = collector.get_flushable_tables(force=force)
    if not tables:
        return conn

    # Reconnect if connection was closed (e.g. by idle cleanup)
    if conn is None or conn.closed:
        conn = _connect(config)

    detail_exc: Optional[HoloClientWithDetailsException] = None
    for tn in tables:
        schema, deletes, inserts = collector.get_records(tn)
        if schema is not None and (deletes or inserts):
            try:
                conn = _execute_batch_with_retry(conn, config, schema, deletes, inserts)
            except HoloClientWithDetailsException as e:
                if e.is_dirty_data:
                    if detail_exc is None:
                        detail_exc = e
                    else:
                        detail_exc.merge(e)
                else:
                    raise
    if detail_exc is not None:
        raise detail_exc
    return conn


def _execute_batch_with_retry(
    conn: psycopg.Connection,
    config: HoloConfig,
    schema: Any,
    deletes: List[Record],
    inserts: List[Record],
) -> psycopg.Connection:
    """Execute a batch of deletes and inserts with retry logic.

    On dirty-data errors, falls back to one-by-one execution so that
    only the truly bad records fail and good records still get written.
    Returns the connection (may be a new one after reconnection).
    """
    try:
        if deletes:
            conn = _execute_sql_with_retry(
                conn, config, *build_delete_sql(schema, deletes)
            )

        if inserts:
            conn = _execute_sql_with_retry(
                conn,
                config,
                *build_insert_sql(
                    schema,
                    inserts,
                    config.on_conflict_action,
                    config.remove_u0000_in_text,
                ),
            )

        return conn
    except HoloClientException as e:
        if (
            e.is_dirty_data
            and config.write_fail_strategy == WriteFailStrategy.TRY_ONE_BY_ONE
        ):
            return _try_one_by_one(conn, config, schema, deletes, inserts)
        else:
            detail_exc = HoloClientWithDetailsException(e.code, str(e), cause=e)
            detail_exc.add_all(deletes + inserts, e)
            raise detail_exc from e


def _try_one_by_one(
    conn: psycopg.Connection,
    config: HoloConfig,
    schema: Any,
    deletes: List[Record],
    inserts: List[Record],
) -> psycopg.Connection:
    """Retry each record individually; raise only the failures."""
    detail_exc: Optional[HoloClientWithDetailsException] = None

    for record in deletes:
        try:
            conn = _execute_sql_with_retry(
                conn, config, *build_delete_sql(schema, [record])
            )
        except (HoloClientException, psycopg.Error) as e:
            holo_e = (
                e
                if isinstance(e, HoloClientException)
                else HoloClientException.from_pg_error(e)
            )
            if detail_exc is None:
                detail_exc = HoloClientWithDetailsException(
                    holo_e.code, str(holo_e), cause=e
                )
            detail_exc.add(record, holo_e)

    for record in inserts:
        try:
            conn = _execute_sql_with_retry(
                conn,
                config,
                *build_insert_sql(
                    schema,
                    [record],
                    config.on_conflict_action,
                    config.remove_u0000_in_text,
                ),
            )
        except (HoloClientException, psycopg.Error) as e:
            holo_e = (
                e
                if isinstance(e, HoloClientException)
                else HoloClientException.from_pg_error(e)
            )
            if detail_exc is None:
                detail_exc = HoloClientWithDetailsException(
                    holo_e.code, str(holo_e), cause=e
                )
            detail_exc.add(record, holo_e)

    if detail_exc is not None:
        raise detail_exc
    return conn


def _execute_sql_with_retry(
    conn: psycopg.Connection,
    config: HoloConfig,
    sql_text: str,
    params: List[Any],
) -> psycopg.Connection:
    """Execute SQL with retry logic, reconnecting on retryable errors.

    Returns the connection (may be a new one after reconnection).
    """
    if not sql_text:
        return conn

    last_exc: Optional[Exception] = None
    for attempt in range(config.retry_count):
        try:
            with conn.cursor() as cur:
                cur.execute(sql_text, params, prepare=True)
            return conn
        except psycopg.Error as e:
            last_exc = e
            holo_exc = HoloClientException.from_pg_error(e)
            if not holo_exc.is_retryable:
                raise holo_exc from e
            if attempt < config.retry_count - 1:
                sleep_ms = (
                    config.retry_sleep_init_ms + attempt * config.retry_sleep_step_ms
                )
                logger.warning(
                    "Retryable error (attempt %d/%d), sleeping %dms: %s",
                    attempt + 1,
                    config.retry_count,
                    sleep_ms,
                    e,
                )
                time.sleep(sleep_ms / 1000.0)
                conn = _reconnect(config, conn)

    if last_exc is not None:
        raise HoloClientException.from_pg_error(last_exc) from last_exc
    return conn


def _start_bg_flush(
    config: HoloConfig,
    collector: ActionCollector,
    conn_holder: List[Optional[psycopg.Connection]],
    last_active_holder: List[float],
    flush_lock: threading.Lock,
    result_queue: multiprocessing.Queue,
    worker_id: int,
    stop_event: threading.Event,
) -> threading.Thread:
    """Start a background flush timer thread within the worker process."""
    interval_s = config.write_max_interval_ms / 1000.0
    check_interval = max(0.5, interval_s / 4)
    max_idle_ms = config.connection_max_idle_ms

    def _bg_loop():
        while not stop_event.wait(check_interval):
            try:
                with flush_lock:
                    conn_holder[0] = _do_flush(
                        collector, conn_holder[0], config, force=False
                    )
                    # Close idle connection (check inside lock to avoid
                    # race with main loop). last_active is updated by the
                    # main loop and _do_flush callers only when actual
                    # I/O occurs, so this fires when the conn sits unused.
                    if (
                        conn_holder[0] is not None
                        and last_active_holder[0] > 0
                        and (time.monotonic() - last_active_holder[0]) * 1000
                        > max_idle_ms
                    ):
                        try:
                            conn_holder[0].close()
                        except Exception as e:
                            logger.warning(
                                "Worker-%d: failed to close idle connection: %s",
                                worker_id,
                                e,
                            )
                        conn_holder[0] = None
                        logger.debug("Worker-%d: closed idle connection", worker_id)
            except Exception as e:
                _send_error(result_queue, worker_id, e)
                logger.error("Worker-%d bg flush error: %s", worker_id, e)

    t = threading.Thread(
        target=_bg_loop, daemon=True, name=f"holo-worker-{worker_id}-bg-flush"
    )
    t.start()
    return t


def _send_error(
    result_queue: multiprocessing.Queue,
    worker_id: int,
    exc: Exception,
) -> None:
    """Send an error to the parent via result_queue."""
    if isinstance(exc, HoloClientException):
        code = exc.code.value
    else:
        code = ExceptionCode.INTERNAL_ERROR.value
    # Serialize as (type, worker_id, message, code) to avoid pickle issues
    # with the original exception (which may contain non-picklable cause chain)
    result_queue.put(("error", worker_id, str(exc), code))


# ── Shared memory worker entry point ─────────────────────────────────


def _writer_worker_shm(
    config: HoloConfig,
    worker_id: int,
    shm_name: str,
    shm_capacity: int,
    write_pos: multiprocessing.Value,
    read_pos: multiprocessing.Value,
    result_queue: multiprocessing.Queue,
) -> None:
    """Worker process using shared memory ring buffer for IPC.

    Reads binary-serialized records from shared memory instead of
    multiprocessing.Queue, eliminating pickle overhead entirely.
    """
    from ._shm_buffer import (
        MSG_CLOSE,
        MSG_FLUSH,
        MSG_RECORD,
        deserialize_record_msg,
    )
    from multiprocessing.shared_memory import SharedMemory
    import struct

    worker_name = f"Worker-{worker_id}"
    logger.info("%s: starting", worker_name)

    collector = ActionCollector(
        max_records=config.write_batch_size,
        max_byte_size=config.write_batch_byte_size,
        max_total_byte_size=config.write_batch_total_byte_size,
        max_wait_time_ms=config.write_max_interval_ms,
        on_conflict=config.on_conflict_action,
        enable_deduplication=config.enable_deduplication,
        num_shards=1,
    )

    flush_lock = threading.Lock()
    conn_holder: List[Optional[psycopg.Connection]] = [None]
    last_active_holder: List[float] = [time.monotonic()]

    try:
        conn_holder[0] = _connect(config)
    except Exception as e:
        _send_error(result_queue, worker_id, e)
        return

    bg_stop = threading.Event()
    bg_thread = _start_bg_flush(
        config,
        collector,
        conn_holder,
        last_active_holder,
        flush_lock,
        result_queue,
        worker_id,
        bg_stop,
    )

    # Attach to shared memory (created by parent)
    shm = SharedMemory(name=shm_name, create=False)
    buf = shm.buf
    capacity = shm_capacity

    # Schema/table name registries (keyed by UTF-8 table name bytes)
    schema_registry: Dict[bytes, TableSchema] = {}
    table_name_registry: Dict[bytes, TableName] = {}

    # Ring buffer read helpers
    _MSG_HEADER = 5  # msg_len(4) + msg_type(1)

    def _available_read() -> int:
        return write_pos.value - read_pos.value

    def _read_bytes_from_ring(pos: int, length: int) -> bytes:
        start = pos % capacity
        end = start + length
        if end <= capacity:
            return bytes(buf[start:end])
        first = capacity - start
        return bytes(buf[start:capacity]) + bytes(buf[0 : length - first])

    try:
        while True:
            # Wait for data
            while _available_read() < _MSG_HEADER:
                time.sleep(0.0001)

            rp = read_pos.value

            # Read header
            header_bytes = _read_bytes_from_ring(rp, _MSG_HEADER)
            payload_len, msg_type = struct.unpack("<IB", header_bytes)
            rp += _MSG_HEADER

            # Wait for full payload
            while _available_read() < _MSG_HEADER + payload_len:
                time.sleep(0.0001)

            # Read payload
            payload = _read_bytes_from_ring(rp, payload_len)
            rp += payload_len
            read_pos.value = rp

            if msg_type == MSG_CLOSE:
                try:
                    with flush_lock:
                        conn_holder[0] = _do_flush(
                            collector, conn_holder[0], config, force=True
                        )
                        last_active_holder[0] = time.monotonic()
                except Exception as e:
                    _send_error(result_queue, worker_id, e)
                result_queue.put(("close_ack", worker_id))
                break

            elif msg_type == MSG_FLUSH:
                ack_id = struct.unpack("<I", payload)[0]
                try:
                    with flush_lock:
                        conn_holder[0] = _do_flush(
                            collector, conn_holder[0], config, force=True
                        )
                        last_active_holder[0] = time.monotonic()
                except Exception as e:
                    _send_error(result_queue, worker_id, e)
                result_queue.put(("flush_ack", ack_id, worker_id))

            elif msg_type == MSG_RECORD:
                # Check if this is a schema registration (first 2 bytes = 0xFFFF)
                if len(payload) >= 2 and payload[0] == 0xFF and payload[1] == 0xFF:
                    # Schema registration: __schema__ marker (0xFFFF) +
                    # tn_key_len(2B) + tn_key + pickled schema
                    import pickle as _pickle

                    offset = 2  # skip marker
                    tn_key_len = struct.unpack_from("<H", payload, offset)[0]
                    offset += 2
                    tn_key = payload[offset : offset + tn_key_len]
                    offset += tn_key_len
                    schema, table_name = _pickle.loads(payload[offset:])
                    schema_registry[tn_key] = schema
                    table_name_registry[tn_key] = table_name
                else:
                    record = deserialize_record_msg(
                        memoryview(payload), schema_registry, table_name_registry
                    )
                    if record is None:
                        logger.error("%s: no schema for record", worker_name)
                        continue
                    try:
                        with flush_lock:
                            batch_ready = collector.append(record)
                            if batch_ready:
                                conn_holder[0] = _do_flush(
                                    collector, conn_holder[0], config, force=False
                                )
                                last_active_holder[0] = time.monotonic()
                    except Exception as e:
                        _send_error(result_queue, worker_id, e)

    except Exception as e:
        logger.error("%s: unexpected error in main loop: %s", worker_name, e)
        _send_error(result_queue, worker_id, e)
    finally:
        bg_stop.set()
        bg_thread.join(timeout=5.0)
        conn = conn_holder[0]
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        try:
            shm.close()
        except Exception:
            pass
        logger.info("%s: stopped", worker_name)
