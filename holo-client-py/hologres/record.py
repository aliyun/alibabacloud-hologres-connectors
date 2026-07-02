from __future__ import annotations

import copy
from typing import Any, List, Optional

from .column import (
    BIGINT,
    BINARY,
    BOOLEAN,
    BIT,
    CHAR,
    DOUBLE,
    INTEGER,
    NUMERIC,
    DECIMAL,
    REAL,
    SMALLINT,
    TIMESTAMP,
    TIMESTAMP_WITH_TIMEZONE,
    VARCHAR,
    ARRAY,
)
from .table_name import TableName
from .table_schema import TableSchema
from .types import MutationType


class Record:
    """Represents a single data row for writing to or reading from Hologres.

    Tracks which columns have been set and computes an estimated byte size
    for batch threshold calculations.
    """

    __slots__ = (
        "schema",
        "table_name",
        "values",
        "_set_columns",
        "_only_insert_columns",
        "type",
        "byte_size",
        "_futures",
    )

    def __init__(self, schema: TableSchema):
        self.schema = schema
        self.table_name = schema.table_name_obj
        n = schema.column_count
        self.values: List[Any] = [None] * n
        self._set_columns: set[int] = set()
        self._only_insert_columns: set[int] = set()
        self.type = MutationType.INSERT
        self.byte_size: int = 0
        self._futures: list = []

    def set_object(self, index_or_name, value: Any, only_insert: bool = False) -> None:
        """Set a column value by index or name.

        Args:
            index_or_name: Column index (int) or name (str).
            value: The value to set.
            only_insert: If True, this column will only be inserted, not updated
                on conflict. Only applies when writeMode=INSERT_OR_UPDATE or
                INSERT_OR_REPLACE. Commonly used for columns like create_time
                that should only be set on insert, not on update.
        """
        if isinstance(index_or_name, str):
            idx = self.schema.get_column_index(index_or_name)
            if idx is None:
                raise KeyError(f"Column {index_or_name!r} not found")
        else:
            idx = index_or_name
        if idx < 0 or idx >= len(self.values):
            raise IndexError(f"Column index {idx} out of range [0, {len(self.values)})")
        old = self.values[idx]
        was_set = idx in self._set_columns
        self.values[idx] = value
        self._set_columns.add(idx)
        if only_insert:
            self._only_insert_columns.add(idx)
        # Update byte size (only subtract old if it was previously set)
        col = self.schema.get_column(idx)
        if was_set:
            self.byte_size -= self._estimate_size(old, col)
        self.byte_size += self._estimate_size(value, col)

    def get_object(self, index_or_name) -> Any:
        """Get a column value by index or name."""
        if isinstance(index_or_name, str):
            idx = self.schema.get_column_index(index_or_name)
            if idx is None:
                raise KeyError(f"Column {index_or_name!r} not found")
            return self.values[idx]
        return self.values[index_or_name]

    def is_set(self, index: int) -> bool:
        return index in self._set_columns

    @property
    def set_columns(self) -> set[int]:
        return self._set_columns

    @property
    def only_insert_columns(self) -> set[int]:
        return self._only_insert_columns

    @property
    def length(self) -> int:
        """Number of columns that have been set."""
        return len(self._set_columns)

    @property
    def size(self) -> int:
        """Total number of columns in the schema."""
        return len(self.values)

    def get_key_values(self) -> tuple:
        """Return the primary key values as a tuple (for use as dict key)."""
        return tuple(self.values[i] for i in self.schema.pk_index)

    def clone(self) -> Record:
        """Create a deep copy of this record."""
        r = Record(self.schema)
        r.table_name = self.table_name
        r.values = list(self.values)
        r._set_columns = set(self._set_columns)
        r._only_insert_columns = set(self._only_insert_columns)
        r.type = self.type
        r.byte_size = self.byte_size
        return r

    def merge(self, other: Record) -> None:
        """Merge another record into this one (for INSERT_OR_UPDATE).

        Only copies values where the other record has them set and they
        are not in the only-insert set.
        """
        for i in other._set_columns:
            if i not in other._only_insert_columns:
                self.set_object(i, other.values[i])
        self._futures.extend(other._futures)

    def cover(self, other: Record) -> None:
        """Keep this record's values, but prepend the other's futures."""
        self._futures = other._futures + self._futures

    @staticmethod
    def _estimate_size(value: Any, col) -> int:
        """Estimate the byte size of a value based on its SQL type."""
        if value is None:
            return 4
        t = col.type
        if t in (CHAR, VARCHAR):
            if isinstance(value, str):
                return len(value)
            return len(str(value))
        if t in (BOOLEAN, BIT):
            return 1
        if t == SMALLINT:
            return 2
        if t in (INTEGER, REAL):
            return 4
        if t in (BIGINT, DOUBLE):
            return 8
        if t in (TIMESTAMP, TIMESTAMP_WITH_TIMEZONE):
            return 12
        if t in (NUMERIC, DECIMAL):
            return 24
        if t == BINARY:
            if isinstance(value, (bytes, bytearray)):
                return len(value)
            return 4
        if t == ARRAY:
            if isinstance(value, (list, tuple)):
                elem_size = 4
                if col.array_element_type in (BIGINT, DOUBLE):
                    elem_size = 8
                elif col.array_element_type == SMALLINT:
                    elem_size = 2
                elif col.array_element_type in (VARCHAR, CHAR):
                    return sum(len(str(v)) for v in value)
                return elem_size * len(value)
            return 4
        return 4

    def __getstate__(self):
        """Pickle support: strip _futures (not picklable across processes)."""
        return {s: getattr(self, s) for s in self.__slots__ if s != "_futures"}

    def __setstate__(self, state):
        """Pickle support: restore slots, default _futures to empty list."""
        for s in self.__slots__:
            setattr(self, s, state.get(s, []))

    def __repr__(self) -> str:
        set_cols = [self.schema.get_column(i).name for i in sorted(self._set_columns)]
        return f"Record({self.table_name}, set={set_cols}, type={self.type.name})"
