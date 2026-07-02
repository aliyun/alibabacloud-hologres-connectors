"""Copy-using-stage bulk write operations for Hologres (>= 4.1.0).

A two-phase write approach: data is serialized as Arrow IPC, uploaded to a
Hologres internal stage, then loaded into the target table via INSERT...SELECT.

Requires pyarrow: pip install pyarrow

Usage (sync)::

    # Create the stage first
    with client._ensure_write_conn().cursor() as cur:
        cur.execute("call hologres.hg_create_internal_stage('my_stage', 'default', 3600)")

    with client.copy_stage_writer("my_table", "my_stage") as writer:
        put = Put(writer.schema)
        put.set_object("id", 1)
        put.set_object("name", "Alice")
        writer.write(put)
    # Data is committed to table on context manager exit

Usage (async)::

    async with await async_client.copy_stage_writer("my_table", "my_stage") as writer:
        put = Put(writer.schema)
        put.set_object("id", 1)
        await writer.write(put)
"""

from __future__ import annotations

import io
import logging
import uuid
from typing import TYPE_CHECKING, List, Optional, Union

import psycopg

from ._arrow import ArrowBatchWriter
from ._stage_sql import build_copy_in_stage_sql, build_insert_select_from_stage_sql
from .config import HoloConfig
from .put import Put
from .record import Record
from .table_schema import TableSchema
from .types import OnConflictAction

if TYPE_CHECKING:
    pass

logger = logging.getLogger("hologres.copy_stage")

_DEFAULT_FILE_SIZE_LIMIT = 64 * 1024 * 1024  # 64 MB


def _create_fixed_fe_conn(config: HoloConfig) -> psycopg.Connection:
    """Create a psycopg connection routed to the Hologres FixedFE backend."""
    return psycopg.connect(config.fixed_fe_conninfo, autocommit=True)


async def _create_fixed_fe_conn_async(config: HoloConfig) -> psycopg.AsyncConnection:
    """Create an async psycopg connection routed to the Hologres FixedFE backend."""
    return await psycopg.AsyncConnection.connect(
        config.fixed_fe_conninfo, autocommit=True
    )


class CopyStageWriter:
    """Synchronous stage-based COPY writer for bulk data loading.

    Data is serialized as Arrow IPC and uploaded to a Hologres internal stage,
    then loaded into the target table via INSERT...SELECT on context manager exit.

    Use as a context manager::

        with client.copy_stage_writer("table", "stage") as writer:
            put = Put(writer.schema)
            put.set_object("id", 1)
            writer.write(put)
    """

    def __init__(
        self,
        config: HoloConfig,
        schema: TableSchema,
        stage_name: str,
        on_conflict: OnConflictAction = OnConflictAction.INSERT_OR_REPLACE,
        columns: Optional[List[str]] = None,
        file_size_limit: int = _DEFAULT_FILE_SIZE_LIMIT,
        max_batch_size: int = 4096,
        is_overwrite: bool = False,
    ):
        self._config = config
        self._conn: Optional[psycopg.Connection] = None
        self._owns_conn = True
        self._schema = schema
        self._stage_name = stage_name
        self._on_conflict = on_conflict
        self._file_size_limit = file_size_limit
        self._is_overwrite = is_overwrite
        self._count = 0
        self._file_index = 0

        # Determine which columns to write
        if columns:
            self._column_names = columns
            self._column_indices: List[int] = []
            for name in columns:
                idx = schema.get_column_index(name)
                if idx is None:
                    raise ValueError(f"Column {name!r} not found in schema")
                self._column_indices.append(idx)
        else:
            self._column_names = []
            self._column_indices = []
            for i, col in enumerate(schema.columns):
                if not col.is_generated_column:
                    self._column_names.append(col.name)
                    self._column_indices.append(i)

        # Generate unique file prefix
        table_str = f"{schema.schema_name}_{schema.table_name}"
        self._file_prefix = f"{table_str}_{uuid.uuid4().hex[:8]}"

        self._arrow_writer = ArrowBatchWriter(
            table_schema=schema,
            column_names=self._column_names,
            column_indices=self._column_indices,
            max_batch_size=max_batch_size,
        )

    @property
    def schema(self) -> TableSchema:
        return self._schema

    @property
    def count(self) -> int:
        """Number of records written."""
        return self._count

    @property
    def stage_name(self) -> str:
        return self._stage_name

    def _ensure_conn(self) -> psycopg.Connection:
        """Lazily create the FixedFE connection."""
        if self._conn is None:
            self._conn = _create_fixed_fe_conn(self._config)
        return self._conn

    def __enter__(self) -> CopyStageWriter:
        self._ensure_conn()
        return self

    def write(self, put_or_record: Union[Put, Record]) -> None:
        """Write a single record."""
        record = (
            put_or_record.record if isinstance(put_or_record, Put) else put_or_record
        )
        self._arrow_writer.put(record)
        self._count += 1

        if self._arrow_writer.data_size >= self._file_size_limit:
            self._flush_to_stage()

    def write_many(self, records: List[Union[Put, Record]]) -> None:
        """Write multiple records."""
        for r in records:
            self.write(r)

    def flush(self) -> None:
        """Flush remaining Arrow data to the stage."""
        data = self._arrow_writer.end_and_get_bytes()
        if data:
            self._send_to_stage(data)

    def commit(self) -> None:
        """Execute INSERT...SELECT to load data from stage into the target table.

        Uses a regular FE connection (not FixedFE) since INSERT...SELECT is not
        supported on FixedFE.
        """
        sql = build_insert_select_from_stage_sql(
            schema=self._schema,
            column_names=self._column_names,
            stages=[self._stage_name],
            conflict_action=self._on_conflict,
            is_overwrite=self._is_overwrite,
        )
        logger.info("Stage commit SQL: %s", sql)
        # INSERT...SELECT must run on regular FE, not FixedFE
        regular_conn = psycopg.connect(self._config.conninfo, autocommit=True)
        try:
            with regular_conn.cursor() as cur:
                cur.execute(sql)
        finally:
            regular_conn.close()

    def _flush_to_stage(self) -> None:
        """Flush current Arrow buffer to stage as a new file."""
        data = self._arrow_writer.end_and_get_bytes()
        if data:
            self._send_to_stage(data)

    def _send_to_stage(self, data: bytes) -> None:
        """Send Arrow bytes to the stage via COPY protocol."""
        file_name = f"{self._file_prefix}_{self._file_index}.arrow"
        self._file_index += 1
        sql = build_copy_in_stage_sql(self._stage_name, file_name)
        logger.info("Stage COPY SQL: %s (%d bytes)", sql, len(data))

        conn = self._ensure_conn()
        with conn.cursor() as cur:
            with cur.copy(sql) as copy:
                copy.write(data)

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            if exc_type is None:
                self.flush()
                self.commit()
                logger.debug(
                    "CopyStageWriter completed: %d records in %d files",
                    self._count,
                    self._file_index,
                )
        finally:
            self._arrow_writer.close()
            if self._owns_conn and self._conn is not None:
                self._conn.close()
                self._conn = None


class AsyncCopyStageWriter:
    """Asynchronous stage-based COPY writer for bulk data loading.

    Use as an async context manager::

        async with await client.copy_stage_writer("table", "stage") as writer:
            put = Put(writer.schema)
            put.set_object("id", 1)
            await writer.write(put)
    """

    def __init__(
        self,
        config: HoloConfig,
        schema: TableSchema,
        stage_name: str,
        on_conflict: OnConflictAction = OnConflictAction.INSERT_OR_REPLACE,
        columns: Optional[List[str]] = None,
        file_size_limit: int = _DEFAULT_FILE_SIZE_LIMIT,
        max_batch_size: int = 4096,
        is_overwrite: bool = False,
    ):
        self._config = config
        self._conn: Optional[psycopg.AsyncConnection] = None
        self._owns_conn = True
        self._schema = schema
        self._stage_name = stage_name
        self._on_conflict = on_conflict
        self._file_size_limit = file_size_limit
        self._is_overwrite = is_overwrite
        self._count = 0
        self._file_index = 0

        if columns:
            self._column_names = columns
            self._column_indices: List[int] = []
            for name in columns:
                idx = schema.get_column_index(name)
                if idx is None:
                    raise ValueError(f"Column {name!r} not found in schema")
                self._column_indices.append(idx)
        else:
            self._column_names = []
            self._column_indices = []
            for i, col in enumerate(schema.columns):
                if not col.is_generated_column:
                    self._column_names.append(col.name)
                    self._column_indices.append(i)

        table_str = f"{schema.schema_name}_{schema.table_name}"
        self._file_prefix = f"{table_str}_{uuid.uuid4().hex[:8]}"

        self._arrow_writer = ArrowBatchWriter(
            table_schema=schema,
            column_names=self._column_names,
            column_indices=self._column_indices,
            max_batch_size=max_batch_size,
        )

    @property
    def schema(self) -> TableSchema:
        return self._schema

    @property
    def count(self) -> int:
        return self._count

    @property
    def stage_name(self) -> str:
        return self._stage_name

    async def _ensure_conn(self) -> psycopg.AsyncConnection:
        """Lazily create the FixedFE connection."""
        if self._conn is None:
            self._conn = await _create_fixed_fe_conn_async(self._config)
        return self._conn

    async def __aenter__(self) -> AsyncCopyStageWriter:
        await self._ensure_conn()
        return self

    async def write(self, put_or_record: Union[Put, Record]) -> None:
        """Write a single record."""
        record = (
            put_or_record.record if isinstance(put_or_record, Put) else put_or_record
        )
        self._arrow_writer.put(record)
        self._count += 1

        if self._arrow_writer.data_size >= self._file_size_limit:
            await self._flush_to_stage()

    async def write_many(self, records: List[Union[Put, Record]]) -> None:
        """Write multiple records."""
        for r in records:
            await self.write(r)

    async def flush(self) -> None:
        """Flush remaining Arrow data to the stage."""
        data = self._arrow_writer.end_and_get_bytes()
        if data:
            await self._send_to_stage(data)

    async def commit(self) -> None:
        """Execute INSERT...SELECT to load data from stage into the target table.

        Uses a regular FE connection (not FixedFE) since INSERT...SELECT is not
        supported on FixedFE.
        """
        sql = build_insert_select_from_stage_sql(
            schema=self._schema,
            column_names=self._column_names,
            stages=[self._stage_name],
            conflict_action=self._on_conflict,
            is_overwrite=self._is_overwrite,
        )
        logger.info("Stage commit SQL: %s", sql)
        # INSERT...SELECT must run on regular FE, not FixedFE
        regular_conn = await psycopg.AsyncConnection.connect(
            self._config.conninfo, autocommit=True
        )
        try:
            async with regular_conn.cursor() as cur:
                await cur.execute(sql)
        finally:
            await regular_conn.close()

    async def _flush_to_stage(self) -> None:
        """Flush current Arrow buffer to stage as a new file."""
        data = self._arrow_writer.end_and_get_bytes()
        if data:
            await self._send_to_stage(data)

    async def _send_to_stage(self, data: bytes) -> None:
        """Send Arrow bytes to the stage via COPY protocol."""
        file_name = f"{self._file_prefix}_{self._file_index}.arrow"
        self._file_index += 1
        sql = build_copy_in_stage_sql(self._stage_name, file_name)
        logger.info("Stage COPY SQL: %s (%d bytes)", sql, len(data))

        conn = await self._ensure_conn()
        async with conn.cursor() as cur:
            async with cur.copy(sql) as copy:
                await copy.write(data)

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            if exc_type is None:
                await self.flush()
                await self.commit()
                logger.debug(
                    "AsyncCopyStageWriter completed: %d records in %d files",
                    self._count,
                    self._file_index,
                )
        finally:
            self._arrow_writer.close()
            if self._owns_conn and self._conn is not None:
                await self._conn.close()
                self._conn = None
