"""Asynchronous Hologres client."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple, TypeVar

import psycopg
from psycopg import sql as psycopg_sql

from ._collector import ActionCollector
from ._schema_loader import load_table_schema_async
from ._sql import build_delete_sql, build_insert_sql
from ._stage_sql import build_create_stage_sql, build_drop_stage_sql
from .column import TIMESTAMP_WITH_TIMEZONE
from .config import HoloConfig
from .copy import AsyncCopyWriter, CopyFormat, CopyMode
from .copy_stage import AsyncCopyStageWriter
from .exceptions import HoloClientException, HoloClientWithDetailsException
from .get import Get
from .put import Put
from .record import Record
from .table_name import TableName
from .table_schema import TableSchema
from .types import ExceptionCode, MutationType, WriteFailStrategy

logger = logging.getLogger("hologres.async_client")

T = TypeVar("T")


class AsyncHoloClient:
    """Asynchronous client for reading and writing to Hologres.

    Usage::

        config = HoloConfig(host="...", port=80, database="...",
                            username="...", password="...")
        async with AsyncHoloClient(config) as client:
            schema = await client.get_table_schema("my_table")

            tasks = []
            for i in range(100):
                put = Put(schema)
                put.set_object("id", i)
                put.set_object("name", f"name{i}")
                tasks.append(asyncio.create_task(client.put(put)))
            await asyncio.gather(*tasks)
            await client.flush()
    """

    def __init__(self, config: HoloConfig):
        config.validate()
        self._config = config
        self._closed = False

        # Schema cache
        self._schema_cache: Dict[TableName, tuple[TableSchema, float]] = {}

        # Multi-writer setup
        self._num_writers = config.write_parallelism
        self._writer_collectors: List[ActionCollector] = []
        self._writer_locks: List[asyncio.Lock] = []
        self._writer_conns: List[Optional[psycopg.AsyncConnection]] = []
        self._flush_tasks: List[Optional[asyncio.Task]] = []
        per_writer_total = config.write_batch_total_byte_size // self._num_writers
        for _ in range(self._num_writers):
            self._writer_collectors.append(
                ActionCollector(
                    max_records=config.write_batch_size,
                    max_byte_size=config.write_batch_byte_size,
                    max_total_byte_size=per_writer_total,
                    max_wait_time_ms=config.write_max_interval_ms,
                    on_conflict=config.on_conflict_action,
                    enable_deduplication=config.enable_deduplication,
                )
            )
            self._writer_locks.append(asyncio.Lock())
            self._writer_conns.append(None)
            self._flush_tasks.append(None)

        self._scan_guc_set = False

        # Connections (lazily created)
        self._read_conn: Optional[psycopg.AsyncConnection] = None
        self._meta_conn: Optional[psycopg.AsyncConnection] = None
        self._sql_conn: Optional[psycopg.AsyncConnection] = None

        # Background flush tasks
        self._bg_exception: Optional[HoloClientException] = None
        self._bg_tasks: List[asyncio.Task] = []
        self._bg_tasks_started = False

        # Queue-based get batching (N reader tasks)
        self._num_readers = config.read_parallelism
        self._get_queue: Optional[asyncio.Queue] = None
        self._reader_tasks: List[asyncio.Task] = []
        self._readers_started = False
        self._get_sql_cache: Dict[Tuple, str] = {}
        self._batch_callback: Optional[Any] = None

    async def _start_background_tasks(self) -> None:
        if self._bg_tasks_started:
            return
        self._bg_tasks_started = True
        for i in range(self._num_writers):
            task = asyncio.create_task(self._bg_loop_writer(i))
            self._bg_tasks.append(task)

    async def _bg_loop_writer(self, writer_id: int) -> None:
        interval_s = self._config.write_max_interval_ms / 1000.0
        check_interval = max(0.5, interval_s / 4)
        while not self._closed:
            await asyncio.sleep(check_interval)
            try:
                await self._do_flush_writer(writer_id, force=False)
            except HoloClientException as e:
                self._bg_exception = e
                logger.error("Background flush error (writer %d): %s", writer_id, e)
            except Exception as e:
                self._bg_exception = HoloClientException(
                    ExceptionCode.INTERNAL_ERROR,
                    f"Background flush error (writer {writer_id}): {e}",
                    e,
                )
                logger.error("Background flush error (writer %d): %s", writer_id, e)

    def _data_conninfo(self) -> str:
        """Connection string for data operations (put/get/scan).

        Returns FixedFE conninfo when ``use_fixed_fe`` is enabled,
        regular conninfo otherwise.
        """
        if self._config.use_fixed_fe:
            return self._config.fixed_fe_conninfo
        return self._config.conninfo

    async def _ensure_write_conn(self, writer_id: int = 0) -> psycopg.AsyncConnection:
        conn = self._writer_conns[writer_id]
        if conn is None or conn.closed:
            conn = await psycopg.AsyncConnection.connect(
                self._data_conninfo(), autocommit=True
            )
            await self._init_write_conn(conn)
            self._writer_conns[writer_id] = conn
        return conn

    async def _init_write_conn(self, conn: psycopg.AsyncConnection) -> None:
        """Set session GUCs on a newly created write connection."""
        if not self._config.enable_generate_binlog:
            async with conn.cursor() as cur:
                await cur.execute("SET hg_experimental_generate_binlog = off")

    async def _ensure_read_conn(self) -> psycopg.AsyncConnection:
        if self._read_conn is None or self._read_conn.closed:
            self._read_conn = await psycopg.AsyncConnection.connect(
                self._data_conninfo(), autocommit=True
            )
            await self._init_read_conn(self._read_conn)
        return self._read_conn

    async def _init_read_conn(self, conn: psycopg.AsyncConnection) -> None:
        """Set session GUCs on a newly created read connection."""
        if self._config.read_timeout_ms > 0 and not self._config.use_fixed_fe:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SET statement_timeout = {self._config.read_timeout_ms}"
                )

    async def _ensure_sql_conn(self) -> psycopg.AsyncConnection:
        """Regular FE connection for sql() command."""
        if self._sql_conn is None or self._sql_conn.closed:
            self._sql_conn = await psycopg.AsyncConnection.connect(
                self._config.conninfo, autocommit=True
            )
        return self._sql_conn

    async def _ensure_meta_conn(self) -> psycopg.AsyncConnection:
        if self._meta_conn is None or self._meta_conn.closed:
            self._meta_conn = await psycopg.AsyncConnection.connect(
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

    # ── Table Schema ──────────────────────────────────────────────────

    async def get_table_schema(
        self, table_name: str, no_cache: bool = False
    ) -> TableSchema:
        """Get the schema for a table, with caching."""
        self._check_state()
        await self._start_background_tasks()
        tn = TableName.valueOf(table_name)

        if not no_cache:
            cached = self._schema_cache.get(tn)
            if cached is not None:
                schema, cache_time = cached
                age_ms = (time.monotonic() - cache_time) * 1000
                if age_ms < self._config.meta_cache_ttl_ms:
                    return schema

        try:
            conn = await self._ensure_meta_conn()
            schema = await load_table_schema_async(conn, tn)
        except psycopg.Error as e:
            raise HoloClientException.from_pg_error(e) from e

        self._check_fixed_fe_unsupported_types(schema)
        self._schema_cache[tn] = (schema, time.monotonic())
        return schema

    # ── Put (Write) ───────────────────────────────────────────────────

    async def put(self, put: Put) -> None:
        """Submit a write operation."""
        self._check_state()
        await self._start_background_tasks()
        record = put.record
        self._validate_put(record)

        pk_indices = record.schema.pk_index
        if len(pk_indices) == 1:
            idx = hash(record.values[pk_indices[0]]) % self._num_writers
        else:
            idx = (
                hash(tuple(record.values[ki] for ki in pk_indices)) % self._num_writers
            )
        async with self._writer_locks[idx]:
            batch_ready = self._writer_collectors[idx].append(record)
        if batch_ready:
            self._schedule_flush(idx)

    def _schedule_flush(self, writer_id: int) -> None:
        """Schedule a flush for the given writer as a background task."""
        existing = self._flush_tasks[writer_id]
        if existing is not None and not existing.done():
            return
        task = asyncio.create_task(self._do_flush_writer_safe(writer_id))
        self._flush_tasks[writer_id] = task

    async def _do_flush_writer_safe(self, writer_id: int) -> None:
        """Flush wrapper that captures exceptions into _bg_exception."""
        try:
            await self._do_flush_writer(writer_id, force=False)
        except HoloClientException as e:
            self._bg_exception = e
            logger.error("Flush error (writer %d): %s", writer_id, e)
        except Exception as e:
            self._bg_exception = HoloClientException(
                ExceptionCode.INTERNAL_ERROR,
                f"Flush error (writer {writer_id}): {e}",
                e,
            )
            logger.error("Flush error (writer %d): %s", writer_id, e)

    async def put_many(self, puts: List[Put]) -> None:
        """Submit multiple write operations."""
        self._check_state()
        await self._start_background_tasks()

        # Group by writer shard
        ready_writers: set[int] = set()
        for p in puts:
            self._validate_put(p.record)
            pk_indices = p.record.schema.pk_index
            if len(pk_indices) == 1:
                idx = hash(p.record.values[pk_indices[0]]) % self._num_writers
            else:
                idx = (
                    hash(tuple(p.record.values[ki] for ki in pk_indices))
                    % self._num_writers
                )
            async with self._writer_locks[idx]:
                if self._writer_collectors[idx].append(p.record):
                    ready_writers.add(idx)

        for idx in ready_writers:
            self._schedule_flush(idx)

    def _check_fixed_fe_unsupported_types(self, schema: TableSchema) -> None:
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

    async def create_stage(
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
        conn = await self._ensure_sql_conn()
        async with conn.cursor() as cur:
            await cur.execute(stmt)

    async def drop_stage(self, stage_name: str) -> None:
        """Drop an internal stage.

        Args:
            stage_name: Name of the stage to drop.
        """
        self._check_state()
        stmt = build_drop_stage_sql(stage_name)
        conn = await self._ensure_sql_conn()
        async with conn.cursor() as cur:
            await cur.execute(stmt)

    # ── COPY Writer ─────────────────────────────────────────────────

    async def copy_writer(
        self,
        table_name: str,
        mode: CopyMode = CopyMode.STREAM,
        fmt: CopyFormat = CopyFormat.TEXT,
        columns: Optional[List[str]] = None,
    ) -> AsyncCopyWriter:
        """Create a COPY-based bulk writer for a table.

        Returns an AsyncCopyWriter context manager::

            async with await client.copy_writer("my_table") as writer:
                put = Put(writer.schema)
                put.set_object("id", 1)
                await writer.write(put)

        Args:
            table_name: Table name, optionally schema-qualified.
            mode: CopyMode.STREAM, CopyMode.BULK_LOAD, or
                CopyMode.BULK_LOAD_ON_CONFLICT.
            fmt: CopyFormat.TEXT or CopyFormat.BINARY.
            columns: Column names to include. Defaults to all non-generated.
        """
        self._check_state()
        schema = await self.get_table_schema(table_name)
        if self._config.use_fixed_fe:
            if mode != CopyMode.STREAM:
                raise HoloClientException(
                    ExceptionCode.INVALID_REQUEST,
                    f"FixedFE only supports CopyMode.STREAM, got {mode.name}",
                )
            conn = await psycopg.AsyncConnection.connect(
                self._data_conninfo(), autocommit=True
            )
            meta_conn = await self._ensure_meta_conn()
            return AsyncCopyWriter(
                conn=conn,
                schema=schema,
                mode=mode,
                fmt=fmt,
                on_conflict=self._config.on_conflict_action,
                columns=columns,
                meta_conn=meta_conn,
                owns_conn=True,
            )
        conn = await self._ensure_sql_conn()
        return AsyncCopyWriter(
            conn=conn,
            schema=schema,
            mode=mode,
            fmt=fmt,
            on_conflict=self._config.on_conflict_action,
            columns=columns,
        )

    async def copy_stage_writer(
        self,
        table_name: str,
        stage_name: str,
        columns: Optional[List[str]] = None,
        file_size_limit: int = 64 * 1024 * 1024,
        max_batch_size: int = 4096,
        is_overwrite: bool = False,
    ) -> AsyncCopyStageWriter:
        """Create a stage-based COPY writer for bulk data loading (Hologres >= 4.1.0).

        Requires pyarrow: pip install pyarrow

        The stage must be created beforehand via ``await client.create_stage()``.

        Usage::

            await client.create_stage("my_stage")
            async with await client.copy_stage_writer("my_table", "my_stage") as writer:
                put = Put(writer.schema)
                put.set_object("id", 1)
                await writer.write(put)
            await client.drop_stage("my_stage")

        Args:
            table_name: Table name, optionally schema-qualified.
            stage_name: Name of the internal stage (must already exist).
            columns: Column names to include. Defaults to all non-generated.
            file_size_limit: Max Arrow file size in bytes before splitting.
            max_batch_size: Number of records per Arrow batch.
            is_overwrite: Use INSERT OVERWRITE instead of INSERT INTO.
        """
        self._check_state()
        schema = await self.get_table_schema(table_name)
        return AsyncCopyStageWriter(
            config=self._config,
            schema=schema,
            stage_name=stage_name,
            on_conflict=self._config.on_conflict_action,
            columns=columns,
            file_size_limit=file_size_limit,
            max_batch_size=max_batch_size,
            is_overwrite=is_overwrite,
        )

    # ── Get (Read) — Queue-based batching ────────────────────────────

    async def _start_reader_tasks(self) -> None:
        """Lazily start reader tasks for queue-based get batching."""
        if self._readers_started:
            return
        self._readers_started = True
        self._get_queue = asyncio.Queue(maxsize=self._config.read_batch_queue_size)
        for i in range(self._num_readers):
            task = asyncio.create_task(self._reader_task(i))
            self._reader_tasks.append(task)

    async def _reader_task(self, reader_id: int) -> None:
        """Async reader: drain queue, batch, execute, complete futures."""
        batch_size = self._config.read_batch_size
        read_timeout_ms = self._config.read_timeout_ms
        conn = await psycopg.AsyncConnection.connect(
            self._data_conninfo(), autocommit=True
        )
        if read_timeout_ms > 0 and not self._config.use_fixed_fe:
            async with conn.cursor() as cur:
                await cur.execute(f"SET statement_timeout = {read_timeout_ms}")

        try:
            while True:
                first = await self._get_queue.get()
                if first is None:
                    # Sentinel — put it back so other readers see it.
                    await self._get_queue.put(None)
                    return

                batch: List[Get] = [first]
                # Non-blocking drain up to batch_size - 1
                for _ in range(batch_size - 1):
                    try:
                        item = self._get_queue.get_nowait()
                        if item is None:
                            await self._get_queue.put(None)
                            break
                        batch.append(item)
                    except asyncio.QueueEmpty:
                        break

                # Check queue wait timeout
                if read_timeout_ms > 0:
                    now_ns = time.monotonic_ns()
                    timeout_ns = read_timeout_ms * 1_000_000
                    live: List[Get] = []
                    for g in batch:
                        wait_ns = now_ns - g.submit_ns
                        if wait_ns > timeout_ns:
                            wait_ms = wait_ns / 1_000_000
                            if g.future is not None and not g.future.done():
                                g.future.set_exception(
                                    HoloClientException(
                                        ExceptionCode.TIMEOUT,
                                        f"get waiting timeout before submit "
                                        f"to holo, it cost {wait_ms:.0f} ms "
                                        f"greater than {read_timeout_ms} ms",
                                    )
                                )
                        else:
                            live.append(g)
                    batch = live
                    if not batch:
                        continue

                # Group by table
                groups: Dict[TableName, List[Get]] = {}
                for g in batch:
                    tn = g.schema.table_name_obj
                    groups.setdefault(tn, []).append(g)

                try:
                    for tn, gets in groups.items():
                        await self._execute_get_batch(conn, gets)
                except Exception as exc:
                    for g in batch:
                        if g.future is not None and not g.future.done():
                            g.future.set_exception(exc)
                    # Reconnect
                    try:
                        await conn.close()
                    except Exception:
                        pass
                    conn = await psycopg.AsyncConnection.connect(
                        self._data_conninfo(), autocommit=True
                    )
                    if read_timeout_ms > 0 and not self._config.use_fixed_fe:
                        async with conn.cursor() as cur:
                            await cur.execute(
                                f"SET statement_timeout = {read_timeout_ms}"
                            )
        finally:
            try:
                await conn.close()
            except Exception:
                pass

    async def _execute_get_batch(
        self, conn: psycopg.AsyncConnection, gets: List[Get]
    ) -> None:
        """Execute a batch of gets and complete their futures."""
        schema = gets[0].schema
        n = len(gets)

        # Compute union of selected columns (always include PKs)
        selected: set[int] = set(schema.pk_index)
        for g in gets:
            selected |= g.selected_columns

        # Get or build cached SQL
        cache_key = (
            schema.schema_name,
            schema.table_name,
            frozenset(selected),
            n,
        )
        cached_sql = self._get_sql_cache.get(cache_key)
        if cached_sql is None:
            cached_sql = self._build_get_sql_template(schema, selected, n, conn)
            self._get_sql_cache[cache_key] = cached_sql

        # Build params
        pk_indices = schema.pk_index
        params: List[Any] = []
        for g in gets:
            for i in pk_indices:
                params.append(g.record.values[i])

        # Execute
        t0 = time.monotonic_ns()
        rows = await self._execute_read_with_retry(conn, cached_sql, params)
        batch_ms = (time.monotonic_ns() - t0) / 1_000_000
        if self._batch_callback is not None:
            self._batch_callback(batch_ms)

        # Build result records indexed by PK
        sorted_sel = sorted(selected)
        result_by_pk: Dict[tuple, Record] = {}
        for row in rows:
            rec = Record(schema)
            for col_idx, sel_idx in enumerate(sorted_sel):
                rec.set_object(sel_idx, row[col_idx])
            pk_vals = tuple(rec.values[ki] for ki in pk_indices)
            result_by_pk[pk_vals] = rec

        # Complete futures
        for g in gets:
            pk_vals = g.record.get_key_values()
            result = result_by_pk.get(pk_vals)
            if g.future is not None and not g.future.done():
                g.future.set_result(result)

    def _build_get_sql_template(
        self,
        schema: TableSchema,
        selected: set[int],
        batch_size: int,
        conn,
    ) -> str:
        """Build and cache a SQL template string for batched GET."""
        sorted_sel = sorted(selected)
        sel_names = [
            psycopg_sql.Identifier(schema.get_column(i).name) for i in sorted_sel
        ]

        pk_indices = schema.pk_index
        pk_col_names = [
            psycopg_sql.Identifier(schema.get_column(i).name) for i in pk_indices
        ]
        one_clause = psycopg_sql.SQL(" AND ").join(
            psycopg_sql.SQL("{}={}").format(pk, psycopg_sql.Placeholder())
            for pk in pk_col_names
        )
        where_clause = psycopg_sql.SQL(" OR ").join(
            psycopg_sql.SQL("({})").format(one_clause) for _ in range(batch_size)
        )

        table_name = psycopg_sql.SQL("{}.{}").format(
            psycopg_sql.Identifier(schema.schema_name),
            psycopg_sql.Identifier(schema.table_name),
        )

        stmt = psycopg_sql.SQL("SELECT {} FROM {} WHERE {}").format(
            psycopg_sql.SQL(", ").join(sel_names),
            table_name,
            where_clause,
        )
        return stmt.as_string(conn)

    async def get(self, get: Get) -> Optional[Record]:
        """Execute a point query by primary key.

        The get is submitted to an internal queue and batched with other
        concurrent gets for efficient execution.
        """
        self._check_state()
        await self._start_reader_tasks()
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        get.future = future
        get.submit_ns = time.monotonic_ns()
        await self._get_queue.put(get)
        return await future

    async def get_many(self, gets: List[Get]) -> List[Optional[Record]]:
        """Execute multiple point queries.

        All gets are submitted to the internal queue and batched by
        reader tasks. Returns results in the same order as input.
        """
        self._check_state()
        if not gets:
            return []
        await self._start_reader_tasks()
        loop = asyncio.get_running_loop()
        futures = []
        for g in gets:
            future = loop.create_future()
            g.future = future
            g.submit_ns = time.monotonic_ns()
            await self._get_queue.put(g)
            futures.append(future)
        results = await asyncio.gather(*futures)
        return list(results)

    # ── Scan ──────────────────────────────────────────────────────────

    async def scan(self, scan) -> List[Record]:
        """Execute a prefix/filter scan and return matching records.

        Args:
            scan: A Scan object built via ``Scan.builder(schema).build()``.
        """
        from ._sql import build_scan_sql

        self._check_state()
        try:
            conn = await self._ensure_read_conn()
            if not self._scan_guc_set and not self._config.use_fixed_fe:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SET hg_experimental_enable_fixed_dispatcher_for_scan = on"
                    )
                self._scan_guc_set = True

            stmt, params = build_scan_sql(scan)
            schema = scan.schema
            async with conn.cursor() as cur:
                await cur.execute(stmt, params, prepare=True)
                rows = await cur.fetchall()

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
        except psycopg.Error as e:
            raise HoloClientException.from_pg_error(e) from e

    # ── SQL Execution ─────────────────────────────────────────────────

    async def sql(
        self, func: Callable[[psycopg.AsyncConnection], Coroutine[Any, Any, T]]
    ) -> T:
        """Execute an arbitrary async SQL operation."""
        self._check_state()
        try:
            if self._config.use_fixed_fe:
                conn = await self._ensure_sql_conn()
            else:
                conn = await self._ensure_read_conn()
            return await func(conn)
        except psycopg.Error as e:
            raise HoloClientException.from_pg_error(e) from e

    # ── Flush ─────────────────────────────────────────────────────────

    async def flush(self) -> None:
        """Force flush all buffered records."""
        self._check_state()
        await asyncio.gather(
            *(self._do_flush_writer(i, force=True) for i in range(self._num_writers))
        )

    async def _do_flush_writer(self, writer_id: int, force: bool) -> None:
        async with self._writer_locks[writer_id]:
            collector = self._writer_collectors[writer_id]
            tables = collector.get_flushable_tables(force=force)
            if not tables:
                return
            batches: list[tuple[TableSchema, list[Record], list[Record]]] = []
            for tn in tables:
                schema, deletes, inserts = collector.get_records(tn)
                if schema is not None and (deletes or inserts):
                    batches.append((schema, deletes, inserts))

        # Execute; accumulate dirty-data errors across tables
        detail_exc: Optional[HoloClientWithDetailsException] = None
        for schema, deletes, inserts in batches:
            try:
                await self._execute_batch(writer_id, schema, deletes, inserts)
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

    async def _execute_batch(
        self,
        writer_id: int,
        schema: TableSchema,
        deletes: List[Record],
        inserts: List[Record],
    ) -> None:
        """Execute a batch of inserts and deletes.

        On dirty-data errors, falls back to one-by-one execution so that
        only the truly bad records fail and good records still get written.
        """
        conn = await self._ensure_write_conn(writer_id)

        try:
            if deletes:
                sql, params = build_delete_sql(schema, deletes)
                if sql:
                    await self._execute_with_retry(conn, sql, params, writer_id)

            if inserts:
                sql, params = build_insert_sql(
                    schema,
                    inserts,
                    self._config.on_conflict_action,
                    self._config.remove_u0000_in_text,
                )
                if sql:
                    await self._execute_with_retry(conn, sql, params, writer_id)

        except HoloClientException as e:
            if (
                e.is_dirty_data
                and self._config.write_fail_strategy == WriteFailStrategy.TRY_ONE_BY_ONE
            ):
                await self._try_one_by_one(writer_id, schema, deletes, inserts)
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
                await self._try_one_by_one(writer_id, schema, deletes, inserts)
            else:
                detail_exc = HoloClientWithDetailsException(
                    holo_e.code, str(holo_e), cause=e
                )
                detail_exc.add_all(deletes + inserts, holo_e)
                raise detail_exc from e

    async def _try_one_by_one(
        self,
        writer_id: int,
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
                    await self._execute_with_retry(
                        await self._ensure_write_conn(writer_id), sql, params, writer_id
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
                sql, params = build_insert_sql(
                    schema,
                    [record],
                    self._config.on_conflict_action,
                    self._config.remove_u0000_in_text,
                )
                if sql:
                    await self._execute_with_retry(
                        await self._ensure_write_conn(writer_id), sql, params, writer_id
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

    async def _execute_with_retry(
        self,
        conn: psycopg.AsyncConnection,
        sql: str,
        params: List[Any],
        writer_id: int = 0,
    ) -> None:
        last_exc: Optional[Exception] = None
        for attempt in range(self._config.retry_count):
            try:
                async with conn.cursor() as cur:
                    await cur.execute(sql, params)
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
                    await asyncio.sleep(sleep_ms / 1000.0)
                    try:
                        await conn.close()
                    except Exception:
                        pass
                    conn = await self._ensure_write_conn(writer_id)

        if last_exc is not None:
            raise HoloClientException.from_pg_error(last_exc) from last_exc

    async def _execute_read_with_retry(
        self,
        conn: psycopg.AsyncConnection,
        sql: str,
        params: List[Any],
    ) -> list:
        """Execute a read SQL with retry logic using read_retry_count."""
        timeout_s = self._config.read_timeout_ms / 1000.0
        # When use_fixed_fe is enabled (GUCs not supported), asyncio.wait_for
        # enforces read_timeout_ms as a client-side timeout.
        use_async_timeout = timeout_s > 0 and self._config.use_fixed_fe

        last_exc: Optional[Exception] = None
        for attempt in range(self._config.read_retry_count):
            try:
                coro = self._read_query(conn, sql, params)
                if use_async_timeout:
                    return await asyncio.wait_for(coro, timeout=timeout_s)
                return await coro
            except asyncio.TimeoutError as e:
                raise HoloClientException(
                    ExceptionCode.TIMEOUT,
                    f"Read query timed out after {self._config.read_timeout_ms}ms",
                ) from e
            except psycopg.Error as e:
                last_exc = e
                holo_exc = HoloClientException.from_pg_error(e)
                if not holo_exc.is_retryable:
                    raise holo_exc from e
                if attempt < self._config.read_retry_count - 1:
                    sleep_ms = (
                        self._config.retry_sleep_init_ms
                        + attempt * self._config.retry_sleep_step_ms
                    )
                    logger.warning(
                        "Read retryable error (attempt %d/%d), sleeping %dms: %s",
                        attempt + 1,
                        self._config.read_retry_count,
                        sleep_ms,
                        e,
                    )
                    await asyncio.sleep(sleep_ms / 1000.0)
                    try:
                        await conn.close()
                    except Exception:
                        pass
                    conn = await self._ensure_read_conn()

        if last_exc is not None:
            raise HoloClientException.from_pg_error(last_exc) from last_exc
        return []

    @staticmethod
    async def _read_query(
        conn: psycopg.AsyncConnection,
        sql: str,
        params: List[Any],
    ) -> list:
        """Execute a read query and return rows."""
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            return await cur.fetchall()

    # ── Close / Context Manager ───────────────────────────────────────

    async def close(self, timeout: float = 30.0) -> None:
        """Close the client, flushing all pending records.

        Args:
            timeout: Maximum seconds to wait for flush and connection
                close before giving up. Default 30s.
        """
        if self._closed:
            return
        self._closed = True

        # Stop reader tasks
        if self._readers_started and self._get_queue is not None:
            await self._get_queue.put(None)  # sentinel
            for task in self._reader_tasks:
                try:
                    await asyncio.wait_for(task, timeout=5.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass

        # Stop background flush tasks
        for task in self._bg_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        try:
            flush_coro = asyncio.gather(
                *(
                    self._do_flush_writer(i, force=True)
                    for i in range(self._num_writers)
                )
            )
            await asyncio.wait_for(flush_coro, timeout=timeout)
        except asyncio.TimeoutError:
            logger.error("Final flush timed out after %.1fs", timeout)
        except Exception as e:
            logger.error("Error during final flush: %s", e)

        for conn in self._writer_conns:
            if conn is not None:
                try:
                    await asyncio.wait_for(conn.close(), timeout=5.0)
                except (asyncio.TimeoutError, Exception):
                    pass
        for conn in (self._read_conn, self._meta_conn, self._sql_conn):
            if conn is not None:
                try:
                    await asyncio.wait_for(conn.close(), timeout=5.0)
                except (asyncio.TimeoutError, Exception):
                    pass
        self._writer_conns = [None] * self._num_writers
        self._read_conn = None
        self._meta_conn = None
        self._sql_conn = None

    async def __aenter__(self) -> AsyncHoloClient:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
