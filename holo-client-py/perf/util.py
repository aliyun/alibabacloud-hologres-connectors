"""Utility helpers for the performance test tool."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from .config import PutTestConf
    from hologres.table_schema import TableSchema


class AtomicLong:
    """Thread-safe counter, mirrors Java's AtomicLong."""

    __slots__ = ("_value", "_lock")

    def __init__(self, initial: int = 0):
        self._value = initial
        self._lock = threading.Lock()

    def increment_and_get(self) -> int:
        with self._lock:
            self._value += 1
            return self._value

    def add_and_get(self, delta: int) -> int:
        with self._lock:
            self._value += delta
            return self._value

    def get(self) -> int:
        with self._lock:
            return self._value


def align_with_column_size(value: int, column_size: int) -> str:
    """Left-pad a number with zeros to reach column_size, or truncate.

    Mirrors Java Util.alignWithColumnSize().
    """
    s = str(value)
    if len(s) >= column_size:
        return s[:column_size]
    return "0" * (column_size - len(s)) + s


def get_write_columns(conf: PutTestConf, schema: TableSchema) -> List[str]:
    """Return the list of column names to write.

    If conf.write_column_count == -1, write all columns.
    Otherwise, write the first N data columns plus PK and ts.
    Mirrors Java Util.getWriteColumnsName().
    """
    all_names = schema.column_names
    if conf.write_column_count < 0 or conf.write_column_count >= conf.column_count:
        return list(all_names)

    result: List[str] = []
    for name in all_names:
        # Always include PK, ts, and partition columns
        if name == "id" or name == "id1" or name == "ts" or name == "ds":
            result.append(name)
            continue
        # Include data columns up to write_column_count
        if name.startswith("name"):
            try:
                idx = int(name[4:])
                if idx < conf.write_column_count:
                    result.append(name)
            except ValueError:
                result.append(name)
    return result
