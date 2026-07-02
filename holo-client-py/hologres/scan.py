"""Scan API for prefix/filter-based queries.

Mirrors Java's Scan, EqualsFilter, RangeFilter, and SortKeys.

Usage::

    schema = client.get_table_schema("my_table")
    scan = (Scan.builder(schema)
            .add_equal_filter("id", 42)
            .set_sort_keys(SortKeys.NONE)
            .build())
    records = client.scan(scan)
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, List, Optional

from .table_schema import TableSchema


class SortKeys(enum.Enum):
    """Sort order for scan results."""

    PRIMARY_KEY = "PRIMARY_KEY"
    CLUSTERING_KEY = "CLUSTERING_KEY"
    NONE = "NONE"


@dataclass(frozen=True)
class EqualsFilter:
    """Equality filter: column = value."""

    column_index: int
    value: Any


@dataclass(frozen=True)
class RangeFilter:
    """Range filter: column >= start AND column < end (by default).

    Args:
        column_index: Index of the column in the schema.
        start: Start value (None for no lower bound).
        end: End value (None for no upper bound).
        start_inclusive: If True, use >= ; otherwise >.
        end_inclusive: If True, use <= ; otherwise <.
    """

    column_index: int
    start: Any = None
    end: Any = None
    start_inclusive: bool = True
    end_inclusive: bool = False


class Scan:
    """Represents a scan (prefix/filter query) operation.

    Use ``Scan.builder(schema)`` to create instances.
    """

    __slots__ = ("schema", "filters", "selected_columns", "sort_keys", "fetch_size")

    def __init__(
        self,
        schema: TableSchema,
        filters: List[EqualsFilter | RangeFilter],
        selected_columns: Optional[set[int]],
        sort_keys: SortKeys,
        fetch_size: int,
    ):
        self.schema = schema
        self.filters = filters
        self.selected_columns = selected_columns
        self.sort_keys = sort_keys
        self.fetch_size = fetch_size

    @classmethod
    def builder(cls, schema: TableSchema) -> ScanBuilder:
        """Create a builder for constructing a Scan."""
        return ScanBuilder(schema)

    def __repr__(self) -> str:
        return (
            f"Scan(table={self.schema.table_name}, "
            f"filters={len(self.filters)}, "
            f"sort_keys={self.sort_keys.name})"
        )


class ScanBuilder:
    """Builder for constructing Scan objects with validation."""

    __slots__ = (
        "_schema",
        "_filters",
        "_selected_columns",
        "_sort_keys",
        "_fetch_size",
    )

    def __init__(self, schema: TableSchema):
        self._schema = schema
        self._filters: List[EqualsFilter | RangeFilter] = []
        self._selected_columns: Optional[set[int]] = None
        self._sort_keys = SortKeys.PRIMARY_KEY
        self._fetch_size = 256

    def add_equal_filter(self, name: str, value: Any) -> ScanBuilder:
        """Add an equality filter (column = value)."""
        idx = self._schema.get_column_index(name)
        if idx is None:
            raise ValueError(f"Column {name!r} not found in schema")
        self._filters.append(EqualsFilter(column_index=idx, value=value))
        return self

    def add_range_filter(
        self,
        name: str,
        start: Any = None,
        end: Any = None,
        start_inclusive: bool = True,
        end_inclusive: bool = False,
    ) -> ScanBuilder:
        """Add a range filter."""
        idx = self._schema.get_column_index(name)
        if idx is None:
            raise ValueError(f"Column {name!r} not found in schema")
        self._filters.append(
            RangeFilter(
                column_index=idx,
                start=start,
                end=end,
                start_inclusive=start_inclusive,
                end_inclusive=end_inclusive,
            )
        )
        return self

    def with_selected_column(self, name: str) -> ScanBuilder:
        """Add a column to the projection."""
        idx = self._schema.get_column_index(name)
        if idx is None:
            raise ValueError(f"Column {name!r} not found in schema")
        if self._selected_columns is None:
            self._selected_columns = set()
        self._selected_columns.add(idx)
        return self

    def with_selected_columns(self, names: List[str]) -> ScanBuilder:
        """Add multiple columns to the projection."""
        for name in names:
            self.with_selected_column(name)
        return self

    def set_sort_keys(self, sort_keys: SortKeys) -> ScanBuilder:
        """Set the sort order for results."""
        self._sort_keys = sort_keys
        return self

    def set_fetch_size(self, fetch_size: int) -> ScanBuilder:
        """Set the fetch size for cursor-based iteration."""
        self._fetch_size = fetch_size
        return self

    def build(self) -> Scan:
        """Build and return the Scan."""
        return Scan(
            schema=self._schema,
            filters=list(self._filters),
            selected_columns=self._selected_columns,
            sort_keys=self._sort_keys,
            fetch_size=self._fetch_size,
        )
