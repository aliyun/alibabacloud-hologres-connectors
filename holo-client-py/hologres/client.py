"""Synchronous Hologres client."""

from __future__ import annotations

import concurrent.futures
import logging
import multiprocessing
import queue
import threading
import time
from typing import Any, Callable, Dict, List, Optional, TypeVar

import psycopg
import psycopg.errors

from ._collector import ActionCollector
from ._schema_loader import load_table_schema_sync
from ._sql import build_delete_sql, build_insert_sql
from ._stage_sql import build_create_stage_sql, build_drop_stage_sql
from ._shm_buffer import (
    MSG_CLOSE,
    MSG_FLUSH,
    MSG_RECORD,
    ShmRingBuffer,
    serialize_record_msg,
)
from ._reader_worker_thread import _cancel_conn, _reader_worker_thread
from ._writer_worker import _writer_worker_shm
from .config import HoloConfig
from .copy import CopyFormat, CopyMode, CopyWriter
from .copy_stage import CopyStageWriter
from .exceptions import HoloClientException, HoloClientWithDetailsException
from .get import Get
from .put import Put
from .record import Record
from .table_name import TableName
from .table_schema import TableSchema
from .column import TIMESTAMP_WITH_TIMEZONE
from .types import ExceptionCode, MutationType, WriteFailStrategy

logger = logging.getLogger("hologres.client")

T = TypeVar("T")


class HoloClient:
    """Synchronous client for reading and writing to Hologres.

    Usage::

        config = HoloConfig(host="...", port=80, database="...",
                            username="...", password="...")
        client = HoloClient(config)
        try:
            schema = client.get_table_schema("my_table")
            put = Put(schema)
            put.set_object("id", 1)
            put.set_object("name", "Alice")
            client.put(put)
            client.flush()
        finally:
            client.close()

    Or as a context manager::

        with HoloClient(config) as client:
            ...
    """

    def __init__(self, config: HoloConfig):
        config.validate()
        self._config = config
        self._closed = False
        self._use_multi_process = config.write_parallelism > 1

        # Schema cache: TableName -> (TableSchema, cache_time)
        self._schema_cache: Dict[TableName, tuple[TableSchema, float]] = {}
        self._schema_lock = threading.Lock()

        # Connections (lazily created)
        self._write_conn: Optional[psycopg.Connection] = None
        self._read_conn: Optional[psycopg.Connection] = None
        self._meta_conn: Optional[psycopg.Connection] = None
        self._sql_conn: Optional[psycopg.Connection] = None
        self._conn_lock = threading.Lock()
        # Track last active time for idle connection cleanup
        self._write_conn_last_active: float = 0.0
        self._read_conn_last_active: float = 0.0

        # Background flush thread (for sync write path and time-based flush)
        self._bg_exception: Optional[HoloClientException] = None
        self._bg_thread: Optional[threading.Thread] = None
        self._bg_stop = threading.Event()

        # Multi-process write infrastructure
        self._worker_processes: List[multiprocessing.Process] = []
        self._worker_result_queue: Optional[multiprocessing.Queue] = None
        self._error_monitor_thread: Optional[threading.Thread] = None
        self._error_monitor_stop = threading.Event()
        self._flush_ack_id = 0
        # Track which schemas have been sent to each worker (by worker index)
        self._schema_sent_to_worker: List[set] = []
        # Shared memory ring buffers (one per worker)
        self._shm_buffers: List[ShmRingBuffer] = []
        # Table name key cache for serialization
        self._table_name_keys: Dict[TableName, bytes] = {}

        if self._use_multi_process:
            self._start_worker_processes()
        else:
            # Action collector for batching (single-process path)
            self._collector = ActionCollector(
                max_records=config.write_batch_size,
                max_byte_size=config.write_batch_byte_size,
                max_total_byte_size=config.write_batch_total_byte_size,
                max_wait_time_ms=config.write_max_interval_ms,
                on_conflict=config.on_conflict_action,
                enable_deduplication=config.enable_deduplication,
                num_shards=1,
            )
            self._start_background_thread()

        self._collector_lock = threading.Lock()

        # Reader infrastructure (always started for get operations)
        self._batch_callback: Optional[Any] = None
        self._reader_threads: List[threading.Thread] = []
        self._reader_queues: List[queue.Queue] = []
        self._reader_stop_event = threading.Event()

        self._start_reader_threads()

        self._scan_guc_set = False

    # ── Multi-process write infrastructure ──────────────────────────

    def _start_worker_processes(self) -> None:
        """Eagerly spawn worker processes and error monitor thread."""
        n = self._config.write_parallelism
        self._worker_result_queue = multiprocessing.Queue()

        for i in range(n):
            self._schema_sent_to_worker.append(set())
            shm_buf = ShmRingBuffer(
                name=f"holo-shm-{id(self)}-{i}",
                capacity=self._config.shm_size,
            )
            self._shm_buffers.append(shm_buf)
            p = multiprocessing.Process(
                target=_writer_worker_shm,
                args=(
                    self._config,
                    i,
                    shm_buf.shm_name,
                    shm_buf.capacity,
                    shm_buf.write_pos,
                    shm_buf.read_pos,
                    self._worker_result_queue,
                ),
                name=f"holo-writer-{i}",
                daemon=True,
            )
            p.start()
            self._worker_processes.append(p)

        logger.info("Started %d writer processes", n)

        # Error monitor thread: polls result_queue for errors and dead workers
        def _error_monitor():
            while not self._error_monitor_stop.wait(0.5):
                # Check for error messages
                self._drain_errors()
                # Check for dead workers
                for i, p in enumerate(self._worker_processes):
                    if not p.is_alive():
                        self._bg_exception = HoloClientException(
                            ExceptionCode.INTERNAL_ERROR,
                            f"Writer process {i} died unexpectedly "
                            f"(exit code {p.exitcode})",
                        )
                        return

        self._error_monitor_thread = threading.Thread(
            target=_error_monitor, daemon=True, name="holo-error-monitor"
        )
        self._error_monitor_thread.start()

    def _drain_errors(self) -> None:
        """Drain all error messages from the result queue (non-blocking)."""
        while True:
            try:
                msg = self._worker_result_queue.get_nowait()
            except Exception:
                break
            if msg[0] == "error":
                _, worker_id, err_msg, err_code = msg
                code = ExceptionCode(err_code)
                self._bg_exception = HoloClientException(
                    code, f"Worker-{worker_id}: {err_msg}"
                )
                logger.error("Worker-%d error: %s", worker_id, err_msg)

    # ── Read infrastructure (thread-based) ─────────────────────────────

    def _start_reader_threads(self) -> None:
        """Spawn reader threads for get operations."""
        n = self._config.read_parallelism
        for i in range(n):
            q = queue.Queue(maxsize=self._config.read_batch_queue_size)
            self._reader_queues.append(q)
            t = threading.Thread(
                target=_reader_worker_thread,
                args=(
                    self._config,
                    i,
                    q,
                    self._config.read_batch_size,
                    self._reader_stop_event,
                    self._get_batch_callback,
                ),
                name=f"holo-reader-{i}",
                daemon=True,
            )
            t.start()
            self._reader_threads.append(t)
        logger.info("Started %d reader threads", n)

    def _get_batch_callback(self, ms: float) -> None:
        """Proxy for _batch_callback that checks if one is set."""
        cb = self._batch_callback
        if cb is not None:
            cb(ms)

    def _close_readers(self) -> None:
        """Send sentinel to all reader threads, join them."""
        self._reader_stop_event.set()
        for q in self._reader_queues:
            try:
                q.put(None, timeout=5.0)
            except queue.Full:
                pass
        for t in self._reader_threads:
            t.join(timeout=10.0)
            if t.is_alive():
                logger.warning("Reader thread %s did not terminate", t.name)
        self._reader_threads.clear()
        self._reader_queues.clear()

    def _start_background_thread(self) -> None:
        interval_s = self._config.write_max_interval_ms / 1000.0
        # Check more frequently than the flush interval
        check_interval = max(0.5, interval_s / 4)

        def _bg_loop():
            while not self._bg_stop.wait(check_interval):
                try:
                    self._try_flush()
                except HoloClientException as e:
                    self._bg_exception = e
                    logger.error("Background flush error: %s", e)
                except Exception as e:
                    self._bg_exception = HoloClientException(
                        ExceptionCode.INTERNAL_ERROR, f"Background flush error: {e}", e
                    )
                    logger.error("Background flush error: %s", e)

        self._bg_thread = threading.Thread(
            target=_bg_loop, daemon=True, name="holo-bg-flush"
        )
        self._bg_thread.start()

    def _data_conninfo(self) -> str:
        """Connection string for data operations (put/get/scan).

        Returns FixedFE conninfo when ``use_fixed_fe`` is enabled,
        regular conninfo otherwise.
        """
        if self._config.use_fixed_fe:
            return self._config.fixed_fe_conninfo
        return self._config.conninfo

    def _is_conn_idle_expired(self, last_active: float) -> bool:
        """Check if a connection has been idle longer than max idle time."""
        if last_active <= 0:
            return False
        idle_ms = (time.monotonic() - last_active) * 1000
        return idle_ms > self._config.connection_max_idle_ms

    def _ensure_write_conn(self) -> psycopg.Connection:
        # Close idle connection
        if (
            self._write_conn is not None
            and not self._write_conn.closed
            and self._is_conn_idle_expired(self._write_conn_last_active)
        ):
            with self._conn_lock:
                try:
                    self._write_conn.close()
                except Exception as e:
                    logger.warning("Failed to close idle write connection: %s", e)
                self._write_conn = None
        if self._write_conn is None or self._write_conn.closed:
            with self._conn_lock:
                if self._write_conn is None or self._write_conn.closed:
                    self._write_conn = psycopg.connect(
                        self._data_conninfo(), autocommit=True
                    )
                    self._init_write_conn(self._write_conn)
        self._write_conn_last_active = time.monotonic()
        return self._write_conn

    def _init_write_conn(self, conn: psycopg.Connection) -> None:
        """Set session GUCs on a newly created write connection."""
        if not self._config.enable_generate_binlog:
            with conn.cursor() as cur:
                cur.execute("SET hg_experimental_generate_binlog = off")

    def _ensure_read_conn(self) -> psycopg.Connection:
        # Close idle connection
        if (
            self._read_conn is not None
            and not self._read_conn.closed
            and self._is_conn_idle_expired(self._read_conn_last_active)
        ):
            with self._conn_lock:
                try:
                    self._read_conn.close()
                except Exception as e:
                    logger.warning("Failed to close idle read connection: %s", e)
                self._read_conn = None
        if self._read_conn is None or self._read_conn.closed:
            with self._conn_lock:
                if self._read_conn is None or self._read_conn.closed:
                    self._read_conn = psycopg.connect(
                        self._data_conninfo(), autocommit=True
                    )
                    self._init_read_conn(self._read_conn)
        self._read_conn_last_active = time.monotonic()
        return self._read_conn

    def _init_read_conn(self, conn: psycopg.Connection) -> None:
        """Set session GUCs on a newly created read connection."""
        if self._config.read_timeout_ms > 0 and not self._config.use_fixed_fe:
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {self._config.read_timeout_ms}")

    def _ensure_sql_conn(self) -> psycopg.Connection:
        """Regular FE connection (for sql(), copy, and non-FixedFE paths)."""
        if self._sql_conn is None or self._sql_conn.closed:
            with self._conn_lock:
                if self._sql_conn is None or self._sql_conn.closed:
                    self._sql_conn = psycopg.connect(
                        self._config.conninfo, autocommit=True
                    )
        return self._sql_conn

    def _ensure_meta_conn(self) -> psycopg.Connection:
        if self._meta_conn is None or self._meta_conn.closed:
            with self._conn_lock:
                if self._meta_conn is None or self._meta_conn.closed:
                    self._meta_conn = psycopg.connect(
                        self._config.conninfo, autocommit=True
                    )
        return self._meta_conn

    def _check_state(self) -> None:
        if self._closed:
            raise HoloClientException(ExceptionCode.ALREADY_CLOSE, "Client is closed")
        if self._bg_exception is not None:
            exc = self._bg_exception
            self._bg_exception = None
            raise exc

    def _check_fixed_fe_unsupported_types(self, schema: TableSchema) -> None:
        """Raise if use_fixed_fe is enabled and the table has unsupported column types.

        Unsupported types:
        - timestamptz: FixedFE does not send the TimeZone ParameterStatus,
          which causes psycopg's C extension to crash (SIGSEGV).
        - roaringbitmap: FixedFE does not support the roaringbitmap extension type.
        """
        if not self._config.use_fixed_fe:
            return
        for col in schema.columns:
            if col.type == TIMESTAMP_WITH_TIMEZONE:
                raise HoloClientException(
                    ExceptionCode.INVALID_REQUEST,
                    f"Table '{schema.table_name}' contains timestamptz column "
                    f"'{col.name}', which is not supported with FixedFE when using psycopg. "
                    f"Use regular FE (use_fixed_fe=False) instead.",
                )
            if col.type_name.lower() == "roaringbitmap":
                raise HoloClientException(
                    ExceptionCode.INVALID_REQUEST,
                    f"Table '{schema.table_name}' contains roaringbitmap column "
                    f"'{col.name}', which is not supported with FixedFE. "
                    f"Use regular FE (use_fixed_fe=False) instead.",
                )

    # ── Table Schema ──────────────────────────────────────────────────

    def get_table_schema(self, table_name: str, no_cache: bool = False) -> TableSchema:
        """Get the schema for a table, with caching.

        Args:
            table_name: Table name, optionally schema-qualified
                (e.g. "public.my_table").
            no_cache: If True, bypass the cache.
        """
        self._check_state()
        tn = TableName.valueOf(table_name)

        if not no_cache:
            with self._schema_lock:
                cached = self._schema_cache.get(tn)
                if cached is not None:
                    schema, cache_time = cached
                    age_ms = (time.monotonic() - cache_time) * 1000
                    if age_ms < self._config.meta_cache_ttl_ms:
                        return schema

        try:
            conn = self._ensure_meta_conn()
            schema = load_table_schema_sync(conn, tn)
        except psycopg.Error as e:
            raise HoloClientException.from_pg_error(e) from e

        self._check_fixed_fe_unsupported_types(schema)
        with self._schema_lock:
            self._schema_cache[tn] = (schema, time.monotonic())
        return schema

    # ── Put (Write) ───────────────────────────────────────────────────

    def put(self, put: Put) -> None:
        """Submit a write operation.

        The record is buffered and will be flushed automatically when
        batch thresholds are met, or when flush() is called.
        """
        self._check_state()
        record = put.record
        self._validate_put(record)

        if self._use_multi_process:
            self._put_to_worker(record)
        else:
            with self._collector_lock:
                batch_ready = self._collector.append(record)
            if batch_ready:
                self._do_flush(force=False)

    def _put_to_worker(self, record: Record) -> None:
        """Route a record to the appropriate worker process by dist key.

        Serializes the record into the worker's shared memory ring buffer.
        The schema is sent once per table per worker and cached on the
        worker side.
        """
        pk_indices = record.schema.pk_index
        n_workers = self._config.write_parallelism
        if len(pk_indices) == 1:
            idx = hash(record.values[pk_indices[0]]) % n_workers
        else:
            idx = hash(tuple(record.values[ki] for ki in pk_indices)) % n_workers
        table_name = record.table_name
        shm_buf = self._shm_buffers[idx]

        # Get or create table name key
        tn_key = self._table_name_keys.get(table_name)
        if tn_key is None:
            tn_key = str(table_name).encode("utf-8")
            self._table_name_keys[table_name] = tn_key

        # Send schema if not already sent to this worker
        sent = self._schema_sent_to_worker[idx]
        if table_name not in sent:
            import pickle
            import struct

            schema_data = pickle.dumps(
                (record.schema, table_name), protocol=pickle.HIGHEST_PROTOCOL
            )
            payload = (
                b"\xff\xff" + struct.pack("<H", len(tn_key)) + tn_key + schema_data
            )
            if not shm_buf.write_message(MSG_RECORD, payload, timeout=10.0):
                raise HoloClientException(
                    ExceptionCode.TIMEOUT, f"Timeout writing schema to worker {idx}"
                )
            sent.add(table_name)

        # Serialize and write record
        record_payload = serialize_record_msg(tn_key, record)
        while not shm_buf.write_message(MSG_RECORD, record_payload, timeout=1.0):
            logger.warning("Worker shm buffer %d full, retrying...", idx)
            self._check_state()

    def put_many(self, puts: List[Put]) -> None:
        """Submit multiple write operations."""
        self._check_state()

        if self._use_multi_process:
            for p in puts:
                self._validate_put(p.record)
                self._put_to_worker(p.record)
            return

        any_ready = False
        with self._collector_lock:
            for p in puts:
                self._validate_put(p.record)
                if self._collector.append(p.record):
                    any_ready = True
        if any_ready:
            self._do_flush(force=False)

    def _validate_put(self, record: Record) -> None:
        schema = record.schema
        if record.type == MutationType.DELETE:
            if not schema.has_primary_key:
                exc = HoloClientWithDetailsException(
                    ExceptionCode.INVALID_REQUEST,
                    "DELETE requires a table with primary key",
                )
                exc.add(record, exc)
                raise exc
            for ki in schema.pk_index:
                if not record.is_set(ki):
                    exc = HoloClientWithDetailsException(
                        ExceptionCode.INVALID_REQUEST,
                        f"DELETE requires all primary key columns to be set. "
                        f"Missing: {schema.get_column(ki).name}",
                    )
                    exc.add(record, exc)
                    raise exc

    # ── Stage Management ─────────────────────────────────────────────

    def create_stage(
        self,
        stage_name: str,
        group_name: str = "default",
        ttl_seconds: int = 3600,
    ) -> None:
        """Create an internal stage for stage-based COPY writes.

        Args:
            stage_name: Name of the stage to create.
            group_name: Resource group name. Defaults to "default".
            ttl_seconds: Time-to-live for data in the stage, in seconds.
                Defaults to 3600 (1 hour).
        """
        self._check_state()
        stmt = build_create_stage_sql(stage_name, group_name, ttl_seconds)
        conn = self._ensure_sql_conn()
        with conn.cursor() as cur:
            cur.execute(stmt)

    def drop_stage(self, stage_name: str) -> None:
        """Drop an internal stage.

        Args:
            stage_name: Name of the stage to drop.
        """
        self._check_state()
        stmt = build_drop_stage_sql(stage_name)
        conn = self._ensure_sql_conn()
        with conn.cursor() as cur:
            cur.execute(stmt)

    # ── COPY Writer ─────────────────────────────────────────────────

    def copy_writer(
        self,
        table_name: str,
        mode: CopyMode = CopyMode.STREAM,
        fmt: CopyFormat = CopyFormat.TEXT,
        columns: Optional[List[str]] = None,
    ) -> CopyWriter:
        """Create a COPY-based bulk writer for a table.

        Returns a CopyWriter context manager::

            with client.copy_writer("my_table", mode=CopyMode.STREAM) as writer:
                put = Put(writer.schema)
                put.set_object("id", 1)
                put.set_object("name", "Alice")
                writer.write(put)

        Args:
            table_name: Table name, optionally schema-qualified.
            mode: CopyMode.STREAM (row locks, supports on_conflict),
                CopyMode.BULK_LOAD (table lock, highest throughput),
                or CopyMode.BULK_LOAD_ON_CONFLICT (bulk with on_conflict).
            fmt: CopyFormat.TEXT or CopyFormat.BINARY.
            columns: Column names to include. Defaults to all non-generated.
        """
        self._check_state()
        schema = self.get_table_schema(table_name)
        if self._config.use_fixed_fe:
            if mode != CopyMode.STREAM:
                raise HoloClientException(
                    ExceptionCode.INVALID_REQUEST,
                    f"FixedFE only supports CopyMode.STREAM, got {mode.name}",
                )
            conn = psycopg.connect(self._data_conninfo(), autocommit=True)
            meta_conn = self._ensure_meta_conn()
            return CopyWriter(
                conn=conn,
                schema=schema,
                mode=mode,
                fmt=fmt,
                on_conflict=self._config.on_conflict_action,
                columns=columns,
                meta_conn=meta_conn,
                owns_conn=True,
            )
        conn = self._ensure_sql_conn()
        return CopyWriter(
            conn=conn,
            schema=schema,
            mode=mode,
            fmt=fmt,
            on_conflict=self._config.on_conflict_action,
            columns=columns,
        )

    def copy_stage_writer(
        self,
        table_name: str,
        stage_name: str,
        columns: Optional[List[str]] = None,
        file_size_limit: int = 64 * 1024 * 1024,
        max_batch_size: int = 4096,
        is_overwrite: bool = False,
    ) -> CopyStageWriter:
        """Create a stage-based COPY writer for bulk data loading (Hologres >= 4.1.0).

        Data is serialized as Arrow IPC, uploaded to the internal stage, then
        loaded into the target table via INSERT...SELECT on context manager exit.

        Requires pyarrow: pip install pyarrow

        The stage must be created beforehand via ``client.create_stage()``.

        Usage::

            client.create_stage("my_stage")
            with client.copy_stage_writer("my_table", "my_stage") as writer:
                put = Put(writer.schema)
                put.set_object("id", 1)
                put.set_object("name", "Alice")
                writer.write(put)
            client.drop_stage("my_stage")

        Args:
            table_name: Table name, optionally schema-qualified.
            stage_name: Name of the internal stage (must already exist).
            columns: Column names to include. Defaults to all non-generated.
            file_size_limit: Max Arrow file size in bytes before splitting.
            max_batch_size: Number of records per Arrow batch.
            is_overwrite: Use INSERT OVERWRITE instead of INSERT INTO.
        """
        self._check_state()
        schema = self.get_table_schema(table_name)
        return CopyStageWriter(
            config=self._config,
            schema=schema,
            stage_name=stage_name,
            on_conflict=self._config.on_conflict_action,
            columns=columns,
            file_size_limit=file_size_limit,
            max_batch_size=max_batch_size,
            is_overwrite=is_overwrite,
        )

    # ── Get (Read) ────────────────────────────────────────────────────

    def get(self, get: Get) -> Optional[Record]:
        """Execute a point query by primary key (blocking).

        Submits the get to a reader thread via queue. The reader
        accumulates gets up to read_batch_size before executing a batched SQL.
        Thread-safe — multiple threads may share one HoloClient for reads.

        Returns the matching Record, or None if not found.
        """
        return self.async_get(get).result()

    def async_get(self, get: Get) -> concurrent.futures.Future:
        """Submit a point query by primary key, return a Future immediately.

        Same pipeline as get() but non-blocking — the caller can submit many
        gets before blocking on any Future, allowing reader threads to batch
        them for higher throughput.
        Thread-safe — multiple threads may share one HoloClient for reads.
        """
        self._check_state()
        future: concurrent.futures.Future = concurrent.futures.Future()
        self._submit_get(get, future)
        get.submit_ns = time.monotonic_ns()
        return future

    def get_many(self, gets: List[Get]) -> List[Optional[Record]]:
        """Execute multiple point queries (blocking).

        Submits each get to reader threads via queue. Readers batch gets
        up to read_batch_size for optimal performance.
        Thread-safe — multiple threads may share one HoloClient for reads.

        Returns a list of Records (or None for not-found) in the same
        order as the input.
        """
        self._check_state()
        if not gets:
            return []

        futures = []
        for g in gets:
            futures.append(self.async_get(g))

        return [f.result() for f in futures]

    def _submit_get(self, get: Get, future: concurrent.futures.Future) -> None:
        """Submit a single get to the appropriate reader thread.

        Thread-safe — multiple threads may call this concurrently.
        """
        schema = get.schema
        n_readers = self._config.read_parallelism

        selected: set[int] = set(schema.pk_index)
        selected |= get.selected_columns
        sorted_sel = sorted(selected)

        pk_indices = schema.pk_index
        pk_values = [get.record.values[ki] for ki in pk_indices]

        reader_idx = hash(tuple(pk_values)) % n_readers

        self._reader_queues[reader_idx].put(
            (schema, sorted_sel, pk_values, future),
            timeout=30.0,
        )

    # ── Scan ─────────────────────────────────────────────────────────

    @staticmethod
    def _rows_to_scan_records(scan, rows) -> List[Record]:
        """Convert raw rows to Record objects for a scan result."""
        schema = scan.schema
        if scan.selected_columns is not None:
            sel_indices = sorted(scan.selected_columns)
        else:
            sel_indices = list(range(schema.column_count))

        records: List[Record] = []
        for row in rows:
            r = Record(schema)
            for col_pos, col_idx in enumerate(sel_indices):
                r.set_object(col_idx, row[col_pos])
            records.append(r)
        return records

    def scan(self, scan) -> List[Record]:
        """Execute a prefix/filter scan and return matching records.

        Args:
            scan: A Scan object built via ``Scan.builder(schema).build()``.
        """
        from ._sql import build_scan_sql

        self._check_state()
        try:
            conn = self._ensure_read_conn()
            if not self._scan_guc_set and not self._config.use_fixed_fe:
                with conn.cursor() as cur:
                    cur.execute(
                        "SET hg_experimental_enable_fixed_dispatcher_for_scan = on"
                    )
                self._scan_guc_set = True

            stmt, params = build_scan_sql(scan)
            timeout_ms = self._config.read_timeout_ms
            use_client_timeout = timeout_ms > 0 and self._config.use_fixed_fe
            timer: Optional[threading.Timer] = None
            if use_client_timeout:
                timer = threading.Timer(timeout_ms / 1000.0, _cancel_conn, args=(conn,))
                timer.daemon = True
                timer.start()
            try:
                with conn.cursor() as cur:
                    cur.execute(stmt, params, prepare=True)
                    rows = cur.fetchall()
            except psycopg.errors.QueryCanceled as e:
                if use_client_timeout:
                    raise HoloClientException(
                        ExceptionCode.TIMEOUT,
                        f"Scan query timed out after {timeout_ms}ms",
                    ) from e
                raise HoloClientException.from_pg_error(e) from e
            finally:
                if timer is not None:
                    timer.cancel()

            return self._rows_to_scan_records(scan, rows)
        except psycopg.Error as e:
            raise HoloClientException.from_pg_error(e) from e

    # ── SQL Execution ─────────────────────────────────────────────────

    def sql(self, func: Callable[[psycopg.Connection], T]) -> T:
        """Execute an arbitrary SQL operation using a raw connection.

        Args:
            func: A callable that receives a psycopg Connection and
                returns a result.
        """
        self._check_state()
        try:
            # sql() always uses regular FE — FixedFE does not support
            # arbitrary SQL (DDL, complex queries, etc.).
            if self._config.use_fixed_fe:
                conn = self._ensure_sql_conn()
            else:
                conn = self._ensure_read_conn()
            return func(conn)
        except psycopg.Error as e:
            raise HoloClientException.from_pg_error(e) from e

    # ── Flush ─────────────────────────────────────────────────────────

    def flush(self) -> None:
        """Force flush all buffered records."""
        self._check_state()
        if self._use_multi_process:
            self._flush_workers()
        else:
            self._do_flush(force=True)

    def _flush_workers(self) -> None:
        """Send flush sentinel to all workers and wait for acks."""
        import struct as _struct

        self._flush_ack_id += 1
        ack_id = self._flush_ack_id
        n = self._config.write_parallelism

        # Send flush sentinels via shared memory
        payload = _struct.pack("<I", ack_id)
        for shm_buf in self._shm_buffers:
            shm_buf.write_message(MSG_FLUSH, payload, timeout=10.0)

        # Collect acks
        acked = set()
        deadline = time.monotonic() + 60.0
        errors: List[str] = []

        while len(acked) < n:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                missing = set(range(n)) - acked
                raise HoloClientException(
                    ExceptionCode.TIMEOUT,
                    f"Flush timed out waiting for workers: {missing}",
                )
            try:
                msg = self._worker_result_queue.get(timeout=min(remaining, 1.0))
            except queue.Empty:
                self._check_state()
                continue

            if msg[0] == "flush_ack" and msg[1] == ack_id:
                acked.add(msg[2])
            elif msg[0] == "error":
                _, worker_id, err_msg, err_code = msg
                errors.append(f"Worker-{worker_id}: {err_msg}")
                logger.error("Worker-%d error during flush: %s", worker_id, err_msg)
            # flush_acks with stale ack_id are ignored

        if errors:
            raise HoloClientException(
                ExceptionCode.INTERNAL_ERROR,
                f"Errors during flush: {'; '.join(errors)}",
            )

    def _try_flush(self) -> None:
        """Called by the background thread; flush only ready batches."""
        self._do_flush(force=False)

    def _do_flush(self, force: bool) -> None:
        with self._collector_lock:
            tables = self._collector.get_flushable_tables(force=force)
            if not tables:
                return
            # Collect all pending records
            batches: list[tuple[TableSchema, list[Record], list[Record]]] = []
            for tn in tables:
                schema, deletes, inserts = self._collector.get_records(tn)
                if schema is not None and (deletes or inserts):
                    batches.append((schema, deletes, inserts))

        # Execute outside the lock; accumulate dirty-data errors
        detail_exc: Optional[HoloClientWithDetailsException] = None
        for schema, deletes, inserts in batches:
            try:
                self._execute_batch(schema, deletes, inserts)
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

    def _execute_batch(
        self,
        schema: TableSchema,
        deletes: List[Record],
        inserts: List[Record],
    ) -> None:
        """Execute a batch of inserts and deletes.

        On dirty-data errors, falls back to one-by-one execution so that
        only the truly bad records fail and good records still get written.
        """
        conn = self._ensure_write_conn()

        try:
            # Execute deletes first
            if deletes:
                sql, params = build_delete_sql(schema, deletes)
                if sql:
                    self._execute_with_retry(conn, sql, params)

            # Execute inserts
            if inserts:
                sql, params = build_insert_sql(
                    schema,
                    inserts,
                    self._config.on_conflict_action,
                    self._config.remove_u0000_in_text,
                )
                if sql:
                    self._execute_with_retry(conn, sql, params)

        except HoloClientException as e:
            if (
                e.is_dirty_data
                and self._config.write_fail_strategy == WriteFailStrategy.TRY_ONE_BY_ONE
            ):
                self._try_one_by_one(schema, deletes, inserts)
            else:
                detail_exc = HoloClientWithDetailsException(e.code, str(e), cause=e)
                detail_exc.add_all(deletes + inserts, e)
                raise detail_exc from e
        except psycopg.Error as e:
            holo_e = HoloClientException.from_pg_error(e)
            if (
                holo_e.is_dirty_data
                and self._config.write_fail_strategy == WriteFailStrategy.TRY_ONE_BY_ONE
            ):
                self._try_one_by_one(schema, deletes, inserts)
            else:
                detail_exc = HoloClientWithDetailsException(
                    holo_e.code, str(holo_e), cause=e
                )
                detail_exc.add_all(deletes + inserts, holo_e)
                raise detail_exc from e

    def _try_one_by_one(
        self,
        schema: TableSchema,
        deletes: List[Record],
        inserts: List[Record],
    ) -> None:
        """Retry each record individually; raise only the failures."""
        detail_exc: Optional[HoloClientWithDetailsException] = None

        for record in deletes:
            try:
                sql, params = build_delete_sql(schema, [record])
                if sql:
                    self._execute_with_retry(self._ensure_write_conn(), sql, params)
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
                sql, params = build_insert_sql(
                    schema,
                    [record],
                    self._config.on_conflict_action,
                    self._config.remove_u0000_in_text,
                )
                if sql:
                    self._execute_with_retry(self._ensure_write_conn(), sql, params)
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

    def _execute_with_retry(
        self,
        conn: psycopg.Connection,
        sql: str,
        params: List[Any],
    ) -> None:
        """Execute SQL with retry logic."""
        last_exc: Optional[Exception] = None
        for attempt in range(self._config.retry_count):
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, params, prepare=True)
                return
            except psycopg.Error as e:
                last_exc = e
                holo_exc = HoloClientException.from_pg_error(e)
                if not holo_exc.is_retryable:
                    raise holo_exc from e
                if attempt < self._config.retry_count - 1:
                    sleep_ms = (
                        self._config.retry_sleep_init_ms
                        + attempt * self._config.retry_sleep_step_ms
                    )
                    logger.warning(
                        "Retryable error (attempt %d/%d), sleeping %dms: %s",
                        attempt + 1,
                        self._config.retry_count,
                        sleep_ms,
                        e,
                    )
                    time.sleep(sleep_ms / 1000.0)
                    # Reconnect
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = self._ensure_write_conn()

        if last_exc is not None:
            raise HoloClientException.from_pg_error(last_exc) from last_exc

    # ── Close / Context Manager ───────────────────────────────────────

    def close(self) -> None:
        """Close the client, flushing all pending records."""
        if self._closed:
            return
        self._closed = True

        # Stop background thread (only exists for single-process paths)
        self._bg_stop.set()
        if self._bg_thread is not None:
            self._bg_thread.join(timeout=5.0)

        # Final flush and shutdown
        try:
            if self._use_multi_process:
                self._close_workers()
            else:
                self._do_flush(force=True)
        except Exception as e:
            logger.error("Error during final flush: %s", e)

        # Close readers
        try:
            self._close_readers()
        except Exception as e:
            logger.error("Error closing readers: %s", e)

        # Close parent connections (read/meta only in multi-process mode)
        for conn in (
            self._write_conn,
            self._read_conn,
            self._meta_conn,
            self._sql_conn,
        ):
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
        self._write_conn = None
        self._read_conn = None
        self._meta_conn = None
        self._sql_conn = None

    def _close_workers(self) -> None:
        """Send close sentinel to all workers, wait for acks, join."""
        # Stop error monitor first so it doesn't interfere
        self._error_monitor_stop.set()
        if self._error_monitor_thread is not None:
            self._error_monitor_thread.join(timeout=5.0)

        n = self._config.write_parallelism

        # Send close sentinels via shared memory
        for shm_buf in self._shm_buffers:
            shm_buf.write_message(MSG_CLOSE, b"", timeout=10.0)

        # Collect close acks
        acked = set()
        deadline = time.monotonic() + 30.0
        while len(acked) < n:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                logger.warning(
                    "Timed out waiting for worker close acks: missing %s",
                    set(range(n)) - acked,
                )
                break
            try:
                msg = self._worker_result_queue.get(timeout=min(remaining, 1.0))
            except queue.Empty:
                continue
            if msg[0] == "close_ack":
                acked.add(msg[1])
            elif msg[0] == "error":
                logger.error("Worker-%d error during close: %s", msg[1], msg[2])

        # Join all processes
        for p in self._worker_processes:
            p.join(timeout=10.0)
            if p.is_alive():
                logger.warning("Worker process %s did not terminate, killing", p.name)
                p.kill()

        # Cleanup shared memory buffers
        for shm_buf in self._shm_buffers:
            shm_buf.close()
        self._shm_buffers.clear()

    def __enter__(self) -> HoloClient:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            if not self._closed:
                self.close()
        except Exception:
            pass
