"""Thread-based reader worker for Get operations.

Each reader thread:
- Reads from a queue.Queue to receive Get requests as Python objects
- Has its own psycopg.Connection to execute SQL
- Resolves futures directly (no cross-process IPC needed)
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import psycopg
import psycopg.errors
from psycopg import sql

from .config import HoloConfig
from .exceptions import HoloClientException
from .record import Record
from .table_schema import TableSchema
from .types import ExceptionCode, MutationType

_EMPTY_SET: frozenset = frozenset()

logger = logging.getLogger("hologres.reader_worker_thread")


def _drain_and_fail_queue(
    request_queue: queue.Queue, worker_name: str, error: Exception
) -> None:
    """Drain remaining items from the queue and fail their futures."""
    count = 0
    while True:
        try:
            item = request_queue.get_nowait()
        except queue.Empty:
            break
        if item is None:
            continue
        _, _, _, future = item
        if not future.done():
            future.set_exception(error)
            count += 1
    if count > 0:
        logger.warning("%s: failed %d pending get(s)", worker_name, count)


# Type alias for a get request item on the queue
# (schema, sorted_sel, pk_values, future)
GetRequestItem = Tuple[TableSchema, List[int], List[Any], Any]


def _reader_worker_thread(
    config: HoloConfig,
    worker_id: int,
    request_queue: queue.Queue,
    batch_size: int,
    stop_event: threading.Event,
    batch_callback: Optional[Callable[[float], None]] = None,
) -> None:
    """Reader worker thread: batches get requests from queue and executes SQL.

    Args:
        config: HoloConfig with connection info and settings.
        worker_id: Identifier for this worker thread.
        request_queue: Queue of GetRequestItem tuples (or None sentinel).
        batch_size: Max number of gets to batch into one SQL query.
        stop_event: Event signaling the worker should stop.
        batch_callback: Optional callback(ms) called after each batch execution.
    """
    worker_name = f"Reader-Thread-{worker_id}"
    logger.info("%s: starting", worker_name)

    try:
        conninfo = config.fixed_fe_conninfo if config.use_fixed_fe else config.conninfo
        conn = psycopg.connect(conninfo, autocommit=True)
        if config.read_timeout_ms > 0 and not config.use_fixed_fe:
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {config.read_timeout_ms}")
    except Exception as e:
        logger.error("%s: failed to connect: %s", worker_name, e)
        _drain_and_fail_queue(
            request_queue,
            worker_name,
            HoloClientException(
                ExceptionCode.INTERNAL_ERROR,
                f"Reader thread failed to connect: {e}",
            ),
        )
        return

    sql_cache: Dict[tuple, str] = {}
    client_timeout_ms = config.read_timeout_ms if config.use_fixed_fe else 0

    try:
        while not stop_event.is_set():
            # Wait for the first item
            try:
                first = request_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            if first is None:
                break

            batch: List[GetRequestItem] = [first]

            # Non-blocking drain up to batch_size - 1 more items
            for _ in range(batch_size - 1):
                try:
                    item = request_queue.get_nowait()
                except queue.Empty:
                    break
                if item is None:
                    request_queue.put(None)
                    break
                batch.append(item)

            # Group by (schema_id, sorted_sel_tuple)
            groups: Dict[tuple, List[GetRequestItem]] = {}
            for item in batch:
                schema, sorted_sel, pk_values, future = item
                group_key = (id(schema), tuple(sorted_sel))
                groups.setdefault(group_key, []).append(item)

            t0 = time.monotonic_ns()

            try:
                for group_key, group_items in groups.items():
                    schema = group_items[0][0]
                    sorted_sel = group_items[0][1]
                    pk_values_list = [it[2] for it in group_items]
                    futures = [it[3] for it in group_items]

                    rows = _execute_get_batch(
                        conn,
                        schema,
                        sorted_sel,
                        pk_values_list,
                        sql_cache,
                        client_timeout_ms=client_timeout_ms,
                    )

                    # Pre-compute PK position mapping for this group
                    pk_indices = schema.pk_index
                    pk_positions = [
                        sorted_sel.index(ki) for ki in pk_indices if ki in sorted_sel
                    ]
                    n_cols = schema.column_count
                    table_name_obj = schema.table_name_obj
                    set_cols = set(sorted_sel)

                    # Map results by PK
                    result_by_pk: Dict[tuple, tuple] = {}
                    for row in rows:
                        pk_vals = tuple(row[p] for p in pk_positions)
                        result_by_pk[pk_vals] = row

                    # Resolve futures with fast Record construction
                    for pk_values, future in zip(pk_values_list, futures):
                        pk_tuple = tuple(pk_values)
                        row = result_by_pk.get(pk_tuple)
                        if row is None:
                            future.set_result(None)
                        else:
                            rec = Record.__new__(Record)
                            rec.schema = schema
                            rec.table_name = table_name_obj
                            values = [None] * n_cols
                            for col_pos, sel_idx in enumerate(sorted_sel):
                                values[sel_idx] = row[col_pos]
                            rec.values = values
                            rec._set_columns = set_cols
                            rec._only_insert_columns = _EMPTY_SET
                            rec.type = MutationType.INSERT
                            rec.byte_size = 0
                            rec._futures = []
                            future.set_result(rec)

            except Exception as e:
                exc = e
                for item in batch:
                    _, _, _, future = item
                    if not future.done():
                        future.set_exception(exc)
                # Reconnect
                try:
                    conn.close()
                except Exception:
                    pass
                try:
                    conninfo = (
                        config.fixed_fe_conninfo
                        if config.use_fixed_fe
                        else config.conninfo
                    )
                    conn = psycopg.connect(conninfo, autocommit=True)
                    if config.read_timeout_ms > 0 and not config.use_fixed_fe:
                        with conn.cursor() as cur:
                            cur.execute(
                                f"SET statement_timeout = {config.read_timeout_ms}"
                            )
                except Exception as reconnect_e:
                    logger.error("%s: reconnect failed: %s", worker_name, reconnect_e)
                    break

            if batch_callback is not None:
                batch_ms = (time.monotonic_ns() - t0) / 1_000_000
                batch_callback(batch_ms)

    except Exception as e:
        logger.error("%s: unexpected error: %s", worker_name, e)
    finally:
        _drain_and_fail_queue(
            request_queue,
            worker_name,
            HoloClientException(
                ExceptionCode.INTERNAL_ERROR,
                f"{worker_name} exited; pending gets cannot be served",
            ),
        )
        try:
            conn.close()
        except Exception:
            pass
        logger.info("%s: stopped", worker_name)


def _cancel_conn(conn: psycopg.Connection) -> None:
    try:
        conn.cancel()
    except Exception:
        pass


def _execute_get_batch(
    conn: psycopg.Connection,
    schema: TableSchema,
    sorted_sel: List[int],
    pk_values_list: List[List[Any]],
    sql_cache: Dict[tuple, str],
    client_timeout_ms: int = 0,
) -> list:
    """Execute a batched GET query and return rows."""
    n = len(pk_values_list)

    cache_key = (schema.schema_name, schema.table_name, tuple(sorted_sel), n)
    cached_sql = sql_cache.get(cache_key)
    if cached_sql is None:
        cached_sql = _build_get_sql(schema, sorted_sel, n, conn)
        sql_cache[cache_key] = cached_sql

    params: List[Any] = []
    for pk_values in pk_values_list:
        for val in pk_values:
            params.append(val)

    timer: Optional[threading.Timer] = None
    if client_timeout_ms > 0:
        timer = threading.Timer(client_timeout_ms / 1000.0, _cancel_conn, args=(conn,))
        timer.daemon = True
        timer.start()
    try:
        with conn.cursor() as cur:
            cur.execute(cached_sql, params, prepare=True)
            return cur.fetchall()
    except psycopg.errors.QueryCanceled as e:
        if timer is not None:
            raise HoloClientException(
                ExceptionCode.TIMEOUT,
                f"Read query timed out after {client_timeout_ms}ms",
            ) from e
        raise
    finally:
        if timer is not None:
            timer.cancel()


def _build_get_sql(
    schema: TableSchema,
    sorted_sel: List[int],
    batch_size: int,
    conn: psycopg.Connection,
) -> str:
    """Build a batched GET SQL query."""
    sel_names = [sql.Identifier(schema.get_column(i).name) for i in sorted_sel]
    pk_indices = schema.pk_index

    pk_col_names = [sql.Identifier(schema.get_column(i).name) for i in pk_indices]
    one_clause = sql.SQL(" AND ").join(
        sql.SQL("{}={}").format(pk, sql.Placeholder()) for pk in pk_col_names
    )
    where_clause = sql.SQL(" OR ").join(
        sql.SQL("({})").format(one_clause) for _ in range(batch_size)
    )

    table_ref = sql.SQL("{}.{}").format(
        sql.Identifier(schema.schema_name), sql.Identifier(schema.table_name)
    )

    query = sql.SQL("SELECT {} FROM {} WHERE {}").format(
        sql.SQL(", ").join(sel_names),
        table_ref,
        where_clause,
    )

    return query.as_string(conn)
