from __future__ import annotations

from typing import Any, List, Optional

from .record import Record
from .table_schema import TableSchema


class Get:
    """Represents a point-query (GET) operation by primary key.

    Usage::

        schema = client.get_table_schema("my_table")
        get = Get.builder(schema).set_primary_key("id", 1).build()
        record = client.get(get)

    Or the simple constructor::

        get = Get(schema)
        get.set_primary_key("id", 1)
        record = client.get(get)
    """

    __slots__ = ("_record", "future", "full_column", "_selected_columns", "submit_ns")

    def __init__(self, schema: TableSchema):
        self._record = Record(schema)
        self.future: Optional[Any] = None  # asyncio.Future set by AsyncHoloClient
        self.full_column = True
        self._selected_columns: set[int] = set()
        self.submit_ns: int = 0  # timestamp when submitted to get queue

    @property
    def record(self) -> Record:
        return self._record

    @property
    def schema(self) -> TableSchema:
        return self._record.schema

    def set_primary_key(self, name_or_index, value: Any) -> Get:
        """Set a primary key column value."""
        if isinstance(name_or_index, str):
            idx = self._record.schema.get_column_index(name_or_index)
            if idx is None:
                raise ValueError(f"Column {name_or_index!r} not found in schema")
            if not self._record.schema.get_column(idx).is_primary_key:
                raise ValueError(f"Column {name_or_index!r} is not a primary key")
            index = idx
        else:
            index = name_or_index
        if value is None:
            raise ValueError("Primary key value cannot be None")
        self._record.set_object(index, value)
        return self

    def set_primary_key_fast(self, index: int, value: Any) -> None:
        """Set a primary key column value by index, skipping validation.

        Use when column index is already known and value is guaranteed
        non-None (e.g. in tight loops with pre-resolved indices).
        """
        self._record.values[index] = value
        self._record._set_columns.add(index)

    def add_select_column(self, name_or_index) -> Get:
        """Add a column to the projection (only return selected columns)."""
        if isinstance(name_or_index, str):
            idx = self._record.schema.get_column_index(name_or_index)
            if idx is None:
                raise ValueError(f"Column {name_or_index!r} not found in schema")
            index = idx
        else:
            index = name_or_index
        self._selected_columns.add(index)
        self.full_column = False
        return self

    def add_select_columns(self, names_or_indices) -> Get:
        """Add multiple columns to the projection."""
        for n in names_or_indices:
            self.add_select_column(n)
        return self

    @property
    def selected_columns(self) -> set[int]:
        """Columns to include in the result."""
        if self.full_column:
            return set(range(self._record.schema.column_count))
        return self._selected_columns

    @classmethod
    def builder(cls, schema: TableSchema) -> GetBuilder:
        """Create a builder for constructing a Get."""
        return GetBuilder(schema)

    def __repr__(self) -> str:
        pk_vals = {
            self._record.schema.get_column(i).name: self._record.values[i]
            for i in self._record.schema.pk_index
            if self._record.is_set(i)
        }
        return f"Get({self._record.table_name}, pk={pk_vals})"


class GetBuilder:
    """Builder for constructing Get objects with validation."""

    __slots__ = ("_get",)

    def __init__(self, schema: TableSchema):
        if not schema.has_primary_key:
            raise ValueError("Table must have a primary key for GET queries")
        self._get = Get(schema)

    def set_primary_key(self, name: str, value: Any) -> GetBuilder:
        self._get.set_primary_key(name, value)
        return self

    def with_selected_column(self, name: str) -> GetBuilder:
        self._get.add_select_column(name)
        return self

    def with_selected_columns(self, names: List[str]) -> GetBuilder:
        self._get.add_select_columns(names)
        return self

    def build(self) -> Get:
        """Build and validate the Get.

        Raises ValueError if not all primary key columns have been set.
        """
        schema = self._get.schema
        for ki in schema.pk_index:
            if not self._get._record.is_set(ki):
                col = schema.get_column(ki)
                raise ValueError(f"Primary key column {col.name!r} has not been set")
        return self._get
