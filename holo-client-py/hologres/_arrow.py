"""Arrow type mapping and IPC serialization for Hologres stage copy.

This module handles conversion between Hologres column types and Apache Arrow
types, and provides an Arrow IPC batch writer for stage-based copy operations.

Requires pyarrow (optional dependency).
"""

from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING, List, Optional

from .column import (
    ARRAY,
    BIGINT,
    BINARY,
    BIT,
    BOOLEAN,
    CHAR,
    DATE,
    DECIMAL,
    DOUBLE,
    INTEGER,
    NUMERIC,
    OTHER,
    REAL,
    SMALLINT,
    TIME,
    TIMESTAMP,
    TIMESTAMP_WITH_TIMEZONE,
    VARCHAR,
)

if TYPE_CHECKING:
    import pyarrow as pa

    from .column import Column
    from .record import Record
    from .table_schema import TableSchema

logger = logging.getLogger("hologres.arrow")


def _import_pyarrow():
    """Import pyarrow, raising a clear error if not installed."""
    try:
        import pyarrow as pa

        return pa
    except ImportError:
        raise ImportError(
            "pyarrow is required for copy_stage operations. "
            "Install it with: pip install pyarrow"
        ) from None


def column_to_arrow_field(column: Column) -> pa.Field:
    """Map a Hologres Column to an Apache Arrow Field."""
    pa = _import_pyarrow()
    name = column.name
    col_type = column.type
    type_name = column.type_name.strip().lower()

    if col_type == SMALLINT:
        arrow_type = pa.int16()
    elif col_type == INTEGER:
        arrow_type = pa.int32()
    elif col_type == BIGINT:
        arrow_type = pa.int64()
    elif col_type in (REAL,):
        arrow_type = pa.float32()
    elif col_type == DOUBLE:
        arrow_type = pa.float64()
    elif col_type in (DECIMAL, NUMERIC):
        precision = column.precision if column.precision > 0 else 38
        scale = column.scale if column.scale >= 0 else 0
        arrow_type = pa.decimal128(precision, scale)
    elif col_type in (BOOLEAN, BIT):
        arrow_type = pa.bool_()
    elif col_type in (CHAR, VARCHAR):
        arrow_type = pa.utf8()
    elif col_type == BINARY:
        arrow_type = pa.binary()
    elif col_type in (TIMESTAMP, TIMESTAMP_WITH_TIMEZONE):
        if type_name == "timestamptz":
            arrow_type = pa.date64()  # millisecond, matching Java DateMilliVector
        else:
            arrow_type = pa.timestamp("us")
    elif col_type == TIME:
        if type_name == "timetz":
            arrow_type = pa.binary(16)  # fixed-size binary
        else:
            arrow_type = pa.time64("us")
    elif col_type == DATE:
        arrow_type = pa.date32()
    elif col_type == ARRAY:
        arrow_type = _array_arrow_type(pa, type_name)
    elif col_type == OTHER:
        if type_name == "roaringbitmap":
            arrow_type = pa.binary()
        else:
            arrow_type = pa.utf8()
    else:
        raise ValueError(f"Unsupported column type: {type_name} (type={col_type})")

    return pa.field(name, arrow_type, nullable=True)


def _array_arrow_type(pa, type_name: str):
    """Map array type_name to Arrow list type.

    Handles both PG internal names (_int4) and user-facing names (integer[]).
    """
    element_map = {
        "_int4": pa.int32(),
        "_int8": pa.int64(),
        "_int2": pa.int16(),
        "_float4": pa.float32(),
        "_float8": pa.float64(),
        "_bool": pa.bool_(),
        "_text": pa.utf8(),
        "_varchar": pa.utf8(),
        # User-facing type names (from schema loader)
        "integer[]": pa.int32(),
        "int[]": pa.int32(),
        "int4[]": pa.int32(),
        "bigint[]": pa.int64(),
        "int8[]": pa.int64(),
        "smallint[]": pa.int16(),
        "int2[]": pa.int16(),
        "real[]": pa.float32(),
        "float4[]": pa.float32(),
        "double precision[]": pa.float64(),
        "float8[]": pa.float64(),
        "boolean[]": pa.bool_(),
        "bool[]": pa.bool_(),
        "text[]": pa.utf8(),
        "varchar[]": pa.utf8(),
        "character varying[]": pa.utf8(),
    }
    element_type = element_map.get(type_name)
    if element_type is None:
        raise ValueError(f"Unsupported array element type: {type_name}")
    return pa.list_(pa.field("item", element_type, nullable=False))


def build_arrow_schema(
    table_schema: TableSchema,
    column_names: List[str],
) -> pa.Schema:
    """Build an Arrow schema from a Hologres TableSchema and column list."""
    pa = _import_pyarrow()
    fields = []
    for col_name in column_names:
        idx = table_schema.get_column_index(col_name)
        if idx is None:
            raise ValueError(f"Column {col_name!r} not found in schema")
        column = table_schema.get_column(idx)
        fields.append(column_to_arrow_field(column))
    return pa.schema(fields)


class ArrowBatchWriter:
    """Accumulates Record objects and serializes them as Arrow IPC stream.

    Records are batched and converted to Arrow RecordBatches, then written
    to an in-memory buffer using the Arrow IPC streaming format.
    """

    def __init__(
        self,
        table_schema: TableSchema,
        column_names: List[str],
        column_indices: List[int],
        max_batch_size: int = 4096,
    ):
        self._pa = _import_pyarrow()
        self._table_schema = table_schema
        self._column_names = column_names
        self._column_indices = column_indices
        self._max_batch_size = max_batch_size

        self._arrow_schema = build_arrow_schema(table_schema, column_names)
        self._batch: List[Record] = []
        self._buffer: Optional[io.BytesIO] = None
        self._writer = None
        self._data_size = 0

    @property
    def data_size(self) -> int:
        """Current size of accumulated Arrow data in bytes."""
        return self._data_size

    def put(self, record: Record) -> None:
        """Add a record to the current batch."""
        self._batch.append(record)
        if len(self._batch) >= self._max_batch_size:
            self._write_batch()

    def _write_batch(self) -> None:
        """Convert the current batch to an Arrow RecordBatch and write it."""
        if not self._batch:
            return

        if self._buffer is None:
            self._buffer = io.BytesIO()
            self._writer = self._pa.ipc.new_stream(self._buffer, self._arrow_schema)

        # Build column arrays
        arrays = []
        for i, col_idx in enumerate(self._column_indices):
            values = []
            for record in self._batch:
                if record.is_set(col_idx):
                    values.append(record.values[col_idx])
                else:
                    values.append(None)
            arrays.append(self._pa.array(values, type=self._arrow_schema.field(i).type))

        batch = self._pa.record_batch(arrays, schema=self._arrow_schema)
        self._writer.write_batch(batch)
        self._data_size = self._buffer.tell()
        self._batch.clear()

    def end_and_get_bytes(self) -> bytes:
        """Flush remaining records, close the IPC stream, and return all bytes.

        Resets the internal state so new data can be written.
        """
        self._write_batch()

        if self._writer is None:
            # No data was written at all
            return b""

        self._writer.close()
        data = self._buffer.getvalue()

        # Reset for next file
        self._buffer = None
        self._writer = None
        self._data_size = 0

        return data

    def close(self) -> None:
        """Clean up resources."""
        self._batch.clear()
        if self._writer is not None:
            try:
                self._writer.close()
            except Exception:
                pass
        self._buffer = None
        self._writer = None
        self._data_size = 0
