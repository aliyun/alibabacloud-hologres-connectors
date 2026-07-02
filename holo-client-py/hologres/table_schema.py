from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from .column import Column
from .table_name import TableName


class TableSchema:
    """Describes the full schema of a Hologres table."""

    def __init__(
        self,
        table_name: TableName,
        columns: List[Column],
        table_id: str = "",
        schema_version: str = "",
        distribution_keys: Optional[List[str]] = None,
        partition_column: Optional[str] = None,
        clustering_keys: Optional[List[str]] = None,
    ):
        self.table_name_obj = table_name
        self.columns = columns
        self.table_id = table_id
        self.schema_version = schema_version
        self.distribution_keys = distribution_keys or []
        self.partition_column = partition_column
        self.clustering_keys = clustering_keys or []

        # Derived properties (computed once)
        self._column_name_to_index: Dict[str, int] = {}
        self._primary_keys: List[str] = []
        self._pk_index: List[int] = []
        self._primary_key_set: Set[str] = set()
        self._distribution_key_index: List[int] = []
        self._partition_index: int = -1

        self._calculate_properties()

    def _calculate_properties(self) -> None:
        for i, col in enumerate(self.columns):
            self._column_name_to_index[col.name] = i
            if col.is_primary_key:
                self._primary_keys.append(col.name)
                self._pk_index.append(i)
                self._primary_key_set.add(col.name)

        for dk in self.distribution_keys:
            idx = self.get_column_index(dk)
            if idx is not None:
                self._distribution_key_index.append(idx)

        if self.partition_column:
            idx = self.get_column_index(self.partition_column)
            if idx is not None:
                self._partition_index = idx

    def get_column_index(self, name: str) -> Optional[int]:
        """Get column index by exact name (case sensitive)."""
        return self._column_name_to_index.get(name)

    def get_column(self, index: int) -> Column:
        return self.columns[index]

    @property
    def column_count(self) -> int:
        return len(self.columns)

    @property
    def primary_keys(self) -> List[str]:
        return self._primary_keys

    @property
    def pk_index(self) -> List[int]:
        return self._pk_index

    @property
    def distribution_key_index(self) -> List[int]:
        return self._distribution_key_index

    @property
    def primary_key_set(self) -> Set[str]:
        return self._primary_key_set

    @property
    def partition_index(self) -> int:
        return self._partition_index

    @property
    def has_primary_key(self) -> bool:
        return len(self._pk_index) > 0

    @property
    def is_partition_parent_table(self) -> bool:
        return self._partition_index >= 0

    def is_primary_key(self, name: str) -> bool:
        return name in self._primary_key_set

    @property
    def schema_name(self) -> str:
        return self.table_name_obj.schema_name

    @property
    def table_name(self) -> str:
        return self.table_name_obj.table_name

    @property
    def column_names(self) -> List[str]:
        return [c.name for c in self.columns]

    @property
    def column_types(self) -> List[int]:
        return [c.type for c in self.columns]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TableSchema):
            return NotImplemented
        return (
            self.table_id == other.table_id
            and self.schema_version == other.schema_version
        )

    def __hash__(self) -> int:
        return hash((self.table_id, self.schema_version))

    def __repr__(self) -> str:
        return (
            f"TableSchema({self.table_name_obj}, "
            f"columns={len(self.columns)}, "
            f"pk={self._primary_keys})"
        )
