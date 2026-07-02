from __future__ import annotations

from typing import Any, Optional

from .record import Record
from .table_schema import TableSchema
from .types import MutationType


class Put:
    """Represents a single write operation, wrapping a Record.

    Usage::

        schema = client.get_table_schema("my_table")
        put = Put(schema)
        put.set_object("id", 1)
        put.set_object("name", "Alice")
        client.put(put)
    """

    __slots__ = ("_record",)

    def __init__(self, schema_or_record):
        if isinstance(schema_or_record, TableSchema):
            self._record = Record(schema_or_record)
            self._record.type = MutationType.INSERT
        elif isinstance(schema_or_record, Record):
            self._record = schema_or_record
        else:
            raise TypeError(
                f"Expected TableSchema or Record, got {type(schema_or_record)}"
            )

    @property
    def record(self) -> Record:
        return self._record

    @property
    def schema(self) -> TableSchema:
        return self._record.schema

    def set_object(self, index_or_name, value: Any, only_insert: bool = False) -> Put:
        """Set a column value by index or name.

        Args:
            index_or_name: Column index (int) or column name (str).
            value: The value to set.
            only_insert: If True, this column will only be written on INSERT,
                not on conflict UPDATE. Useful for "created_at" style fields.

        Returns:
            self for method chaining.
        """
        if isinstance(index_or_name, str):
            idx = self._record.schema.get_column_index(index_or_name)
            if idx is None:
                raise ValueError(f"Column {index_or_name!r} not found in schema")
            index = idx
        else:
            index = index_or_name

        self._record.set_object(index, value)
        if only_insert:
            self._record.only_insert_columns.add(index)
        return self

    def get_object(self, index_or_name) -> Any:
        """Get a column value by index or name."""
        return self._record.get_object(index_or_name)

    def is_set(self, index: int) -> bool:
        """Check whether a column has been set."""
        return self._record.is_set(index)

    @property
    def mutation_type(self) -> MutationType:
        return self._record.type

    @mutation_type.setter
    def mutation_type(self, value: MutationType) -> None:
        self._record.type = value

    def __repr__(self) -> str:
        return f"Put({self._record!r})"
