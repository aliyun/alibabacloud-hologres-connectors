"""COPY-based bulk write operations for Hologres.

Supports two formats:
- text: Uses psycopg's write_row() for automatic text encoding
- binary: Uses psycopg dumpers for binary encoding per PG binary COPY protocol

Supports three copy modes:
- STREAM: Hologres streaming mode with row-level locks, supports on_conflict
- BULK_LOAD: Standard PostgreSQL batch COPY, table-level lock, highest throughput
- BULK_LOAD_ON_CONFLICT: Bulk load with on_conflict support (Hologres >= 3.1)

Usage (sync)::

    with client.copy_writer("my_table", mode=CopyMode.STREAM) as writer:
        put = Put(writer.schema)
        put.set_object("id", 1)
        put.set_object("name", "Alice")
        writer.write(put)

Usage (async)::

    async with await async_client.copy_writer("my_table") as writer:
        put = Put(writer.schema)
        put.set_object("id", 1)
        await writer.write(put)
"""

from __future__ import annotations

import logging
import struct
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import psycopg
import psycopg.pq
from psycopg import sql

from .put import Put
from .record import Record
from .table_name import TableName
from .table_schema import TableSchema
from .types import OnConflictAction

if TYPE_CHECKING:
    pass

logger = logging.getLogger("hologres.copy")

# PostgreSQL binary COPY header constants
_PG_BINARY_SIGNATURE = b"PGCOPY\n\xff\r\n\x00"
_PG_BINARY_FLAGS = 0
_PG_BINARY_EXTENSION_LENGTH = 0
_NUMERIC_OID = 1700
_BPCHAR_OID = 1042
_TEXT_OID = 25
_BYTEA_OID = 17

# Extension types whose binary wire format is raw bytes (like bytea).
# These have dynamic OIDs so we identify them by type name.
_BINARY_PASSTHROUGH_TYPES = {"roaringbitmap"}

# COPY SQL templates
_COPY_TEMPLATE = "COPY {0}.{1}({2}) FROM STDIN WITH (FORMAT {3}, STREAM_MODE {4})"
_COPY_TEMPLATE_PK = (
    "COPY {0}.{1}({2}) FROM STDIN WITH (FORMAT {3}, STREAM_MODE {4}, ON_CONFLICT {5})"
)


class CopyFormat(Enum):
    """Data format for COPY operations."""

    TEXT = "text"
    BINARY = "binary"


class CopyMode(Enum):
    """COPY write mode."""

    STREAM = "stream"
    BULK_LOAD = "bulk_load"
    BULK_LOAD_ON_CONFLICT = "bulk_load_on_conflict"


def _build_copy_sql(
    table_name: TableName,
    columns: List[str],
    fmt: CopyFormat,
    mode: CopyMode,
    on_conflict: OnConflictAction = OnConflictAction.INSERT_OR_REPLACE,
    has_pk: bool = False,
) -> sql.Composed:
    """Build a COPY ... FROM STDIN SQL statement."""
    col_ids = sql.SQL(", ").join(map(sql.Identifier, columns))
    stream_mode = "true" if mode == CopyMode.STREAM else "false"

    on_conflict_str = ""
    if has_pk and mode in (CopyMode.STREAM, CopyMode.BULK_LOAD_ON_CONFLICT):
        if on_conflict == OnConflictAction.INSERT_OR_IGNORE:
            on_conflict_str = "ignore"
        else:
            on_conflict_str = "update"

    if on_conflict_str:
        return sql.SQL(_COPY_TEMPLATE_PK).format(
            sql.Identifier(table_name.schema_name),
            sql.Identifier(table_name.table_name),
            col_ids,
            fmt.value,
            stream_mode,
            on_conflict_str,
        )
    else:
        return sql.SQL(_COPY_TEMPLATE).format(
            sql.Identifier(table_name.schema_name),
            sql.Identifier(table_name.table_name),
            col_ids,
            fmt.value,
            stream_mode,
        )


def _encode_binary_header() -> bytes:
    """Encode the PostgreSQL binary COPY header."""
    header = bytearray()
    header.extend(_PG_BINARY_SIGNATURE)
    header.extend(struct.pack("!I", _PG_BINARY_FLAGS))
    header.extend(struct.pack("!I", _PG_BINARY_EXTENSION_LENGTH))
    return bytes(header)


def _encode_binary_row(
    values: Tuple[Any, ...],
    column_names: List[str],
    column_type_oids: Dict[str, int],
    column_type_names: Dict[str, str],
    cur: psycopg.Cursor,
) -> bytes:
    """Encode a single row in PostgreSQL binary COPY format.

    Uses psycopg's dumper system for type-correct binary encoding.
    Extension types listed in _BINARY_PASSTHROUGH_TYPES are encoded as
    raw bytes using the bytea dumper.
    """
    row_data = bytearray()
    num_cols = len(values)
    row_data.extend(struct.pack("!h", num_cols))

    for idx, value in enumerate(values):
        if value is None:
            row_data.extend(struct.pack("!i", -1))
        else:
            col_name = column_names[idx]
            type_oid = column_type_oids[col_name]
            type_name = column_type_names.get(col_name, "")

            # Extension types with raw-bytes binary format (e.g. roaringbitmap):
            # use bytea dumper since their OID is not known to psycopg.
            if type_name in _BINARY_PASSTHROUGH_TYPES:
                lookup_oid = _BYTEA_OID
            # bpchar (char(n)) has no binary dumper in psycopg; use the
            # text OID dumper instead — binary wire format is identical.
            elif type_oid == _BPCHAR_OID:
                lookup_oid = _TEXT_OID
            else:
                lookup_oid = type_oid

            dumper_class = cur.adapters.get_dumper_by_oid(
                lookup_oid, psycopg.pq.Format.BINARY
            )
            dumper = dumper_class(value.__class__, cur)

            # NUMERIC: convert float to Decimal for correct encoding
            if type_oid == _NUMERIC_OID and isinstance(value, float):
                value = Decimal(str(value))

            binary_value = dumper.dump(value)
            row_data.extend(struct.pack("!i", len(binary_value)))
            row_data.extend(binary_value)

    return bytes(row_data)


_TYPE_OID_SQL = sql.SQL("""\
SELECT a.attname, a.atttypid, t.typname
FROM pg_attribute a
JOIN pg_type t ON t.oid = a.atttypid
WHERE a.attrelid = {0}::regclass
  AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
""")


def _load_column_type_info(
    conn: psycopg.Connection,
    table_name: TableName,
) -> Tuple[Dict[str, int], Dict[str, str]]:
    """Load column name -> (type OID, type name) mappings from pg_attribute."""
    qualified = sql.Literal(f"{table_name.schema_name}.{table_name.table_name}")
    with conn.cursor() as cur:
        cur.execute(_TYPE_OID_SQL.format(qualified))
        oids: Dict[str, int] = {}
        names: Dict[str, str] = {}
        for row in cur.fetchall():
            oids[row[0]] = int(row[1])
            names[row[0]] = row[2]
        return oids, names


async def _load_column_type_info_async(
    conn: psycopg.AsyncConnection,
    table_name: TableName,
) -> Tuple[Dict[str, int], Dict[str, str]]:
    """Load column name -> (type OID, type name) mappings (async)."""
    qualified = sql.Literal(f"{table_name.schema_name}.{table_name.table_name}")
    async with conn.cursor() as cur:
        await cur.execute(_TYPE_OID_SQL.format(qualified))
        oids: Dict[str, int] = {}
        names: Dict[str, str] = {}
        for row in await cur.fetchall():
            oids[row[0]] = int(row[1])
            names[row[0]] = row[2]
        return oids, names


def _record_to_row(record: Record, column_indices: List[int]) -> tuple:
    """Extract a tuple of values from a Record for the given column indices."""
    return tuple(record.values[i] if record.is_set(i) else None for i in column_indices)


class CopyWriter:
    """Synchronous COPY writer for bulk data loading.

    Use as a context manager::

        with client.copy_writer("table", mode=CopyMode.STREAM) as writer:
            put = Put(writer.schema)
            put.set_object("id", 1)
            writer.write(put)
    """

    def __init__(
        self,
        conn: psycopg.Connection,
        schema: TableSchema,
        mode: CopyMode = CopyMode.STREAM,
        fmt: CopyFormat = CopyFormat.TEXT,
        on_conflict: OnConflictAction = OnConflictAction.INSERT_OR_REPLACE,
        columns: Optional[List[str]] = None,
        meta_conn: Optional[psycopg.Connection] = None,
        owns_conn: bool = False,
    ):
        self._conn = conn
        self._meta_conn = meta_conn
        self._owns_conn = owns_conn
        self._schema = schema
        self._mode = mode
        self._fmt = fmt
        self._on_conflict = on_conflict
        self._copy = None
        self._copy_ctx = None
        self._cursor = None
        self._count = 0
        self._column_type_oids: Optional[Dict[str, int]] = None
        self._column_type_names: Optional[Dict[str, str]] = None

        # Determine which columns to write
        if columns:
            self._column_names = columns
            self._column_indices = []
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

    @property
    def schema(self) -> TableSchema:
        return self._schema

    @property
    def count(self) -> int:
        """Number of records written."""
        return self._count

    def _build_sql(self) -> str:
        return _build_copy_sql(
            table_name=self._schema.table_name_obj,
            columns=self._column_names,
            fmt=self._fmt,
            mode=self._mode,
            on_conflict=self._on_conflict,
            has_pk=self._schema.has_primary_key,
        )

    def __enter__(self) -> CopyWriter:
        # For binary format, load type OIDs and type names
        if self._fmt == CopyFormat.BINARY:
            oid_conn = self._meta_conn if self._meta_conn is not None else self._conn
            self._column_type_oids, self._column_type_names = _load_column_type_info(
                oid_conn, self._schema.table_name_obj
            )

        sql = self._build_sql()
        logger.info("COPY SQL: %s", sql)
        self._cursor = self._conn.cursor()
        self._copy_ctx = self._cursor.copy(sql)
        self._copy = self._copy_ctx.__enter__()

        # Write binary header
        if self._fmt == CopyFormat.BINARY:
            self._copy.write(_encode_binary_header())

        return self

    def write(self, put_or_record: Union[Put, Record]) -> None:
        """Write a single record to the COPY stream."""
        if self._copy is None:
            raise RuntimeError("CopyWriter not started; use as context manager")
        record = (
            put_or_record.record if isinstance(put_or_record, Put) else put_or_record
        )
        row = _record_to_row(record, self._column_indices)

        if self._fmt == CopyFormat.TEXT:
            self._copy.write_row(row)
        else:
            binary_row = _encode_binary_row(
                row,
                self._column_names,
                self._column_type_oids,
                self._column_type_names,
                self._cursor,
            )
            self._copy.write(binary_row)

        self._count += 1

    def write_many(self, records: List[Union[Put, Record]]) -> None:
        """Write multiple records to the COPY stream."""
        for r in records:
            self.write(r)

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            if self._copy_ctx is not None:
                self._copy_ctx.__exit__(exc_type, exc_val, exc_tb)
                self._copy = None
                self._copy_ctx = None
                self._cursor = None
                if exc_type is None:
                    logger.debug("CopyWriter completed: %d records", self._count)
        finally:
            if self._owns_conn:
                self._conn.close()


class AsyncCopyWriter:
    """Asynchronous COPY writer for bulk data loading.

    Use as a context manager::

        async with await async_client.copy_writer("table") as writer:
            put = Put(writer.schema)
            put.set_object("id", 1)
            await writer.write(put)
    """

    def __init__(
        self,
        conn: psycopg.AsyncConnection,
        schema: TableSchema,
        mode: CopyMode = CopyMode.STREAM,
        fmt: CopyFormat = CopyFormat.TEXT,
        on_conflict: OnConflictAction = OnConflictAction.INSERT_OR_REPLACE,
        columns: Optional[List[str]] = None,
        meta_conn: Optional[psycopg.AsyncConnection] = None,
        owns_conn: bool = False,
    ):
        self._conn = conn
        self._meta_conn = meta_conn
        self._owns_conn = owns_conn
        self._schema = schema
        self._mode = mode
        self._fmt = fmt
        self._on_conflict = on_conflict
        self._copy = None
        self._copy_ctx = None
        self._cursor = None
        self._count = 0
        self._column_type_oids: Optional[Dict[str, int]] = None
        self._column_type_names: Optional[Dict[str, str]] = None

        if columns:
            self._column_names = columns
            self._column_indices = []
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

    @property
    def schema(self) -> TableSchema:
        return self._schema

    @property
    def count(self) -> int:
        return self._count

    def _build_sql(self) -> str:
        return _build_copy_sql(
            table_name=self._schema.table_name_obj,
            columns=self._column_names,
            fmt=self._fmt,
            mode=self._mode,
            on_conflict=self._on_conflict,
            has_pk=self._schema.has_primary_key,
        )

    async def __aenter__(self) -> AsyncCopyWriter:
        if self._fmt == CopyFormat.BINARY:
            oid_conn = self._meta_conn if self._meta_conn is not None else self._conn
            (
                self._column_type_oids,
                self._column_type_names,
            ) = await _load_column_type_info_async(
                oid_conn, self._schema.table_name_obj
            )

        sql = self._build_sql()
        logger.info("COPY SQL: %s", sql)
        self._cursor = self._conn.cursor()
        self._copy_ctx = self._cursor.copy(sql)
        self._copy = await self._copy_ctx.__aenter__()

        if self._fmt == CopyFormat.BINARY:
            await self._copy.write(_encode_binary_header())

        return self

    async def write(self, put_or_record: Union[Put, Record]) -> None:
        """Write a single record to the COPY stream."""
        if self._copy is None:
            raise RuntimeError("AsyncCopyWriter not started; use as context manager")
        record = (
            put_or_record.record if isinstance(put_or_record, Put) else put_or_record
        )
        row = _record_to_row(record, self._column_indices)

        if self._fmt == CopyFormat.TEXT:
            await self._copy.write_row(row)
        else:
            binary_row = _encode_binary_row(
                row,
                self._column_names,
                self._column_type_oids,
                self._column_type_names,
                self._cursor,
            )
            await self._copy.write(binary_row)

        self._count += 1

    async def write_many(self, records: List[Union[Put, Record]]) -> None:
        """Write multiple records to the COPY stream."""
        for r in records:
            await self.write(r)

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            if self._copy_ctx is not None:
                await self._copy_ctx.__aexit__(exc_type, exc_val, exc_tb)
                self._copy = None
                self._copy_ctx = None
                self._cursor = None
                if exc_type is None:
                    logger.debug("AsyncCopyWriter completed: %d records", self._count)
        finally:
            if self._owns_conn:
                await self._conn.close()
