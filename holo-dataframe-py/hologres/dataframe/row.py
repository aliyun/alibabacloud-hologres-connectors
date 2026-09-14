"""The :class:`Row` returned by DataFrame actions.

A ``Row`` is one record of a query result. It behaves like a tuple *and* like a
read-only mapping, so query results can be accessed by position, key, or
attribute:

    >>> row = Row(("id", "name"), (1, "alice"))
    >>> row[0], row["name"], row.name
    (1, 'alice', 'alice')

Rows are produced by ``collect()``, ``first()``, ``take()`` and ``iter_rows()``.
"""

from __future__ import annotations

from typing import Any, Iterator

__all__ = ["Row"]


class Row:
    """One row of a query result, accessible by position, key, or attribute.

    Immutable: assigning to a field raises ``AttributeError``. Equality and
    hashing consider both the field names and the values, so two rows with the
    same values but different column names are not equal.

    Args:
        fields: Column names, in order.
        values: Column values, in the same order as ``fields``.

    Raises:
        ValueError: If ``fields`` and ``values`` have different lengths.

    Example:
        >>> row = Row(("id", "amount"), (7, 12.5))
        >>> row[1]
        12.5
        >>> row["id"]
        7
        >>> row.amount
        12.5
        >>> row.as_dict()
        {'id': 7, 'amount': 12.5}
        >>> len(row)
        2
        >>> list(row)
        [7, 12.5]
        >>> row
        Row(id=7, amount=12.5)
    """

    __slots__ = ("_fields", "_values")

    def __init__(self, fields: tuple[str, ...], values: tuple[Any, ...]) -> None:
        fields = tuple(fields)
        values = tuple(values)
        if len(fields) != len(values):
            raise ValueError(
                f"Row has {len(fields)} field name(s) but {len(values)} value(s)"
            )
        object.__setattr__(self, "_fields", fields)
        object.__setattr__(self, "_values", values)

    @property
    def fields(self) -> tuple[str, ...]:
        """The column names, in order."""
        return self._fields

    @property
    def values(self) -> tuple[Any, ...]:
        """The column values, in order."""
        return self._values

    def as_dict(self) -> dict[str, Any]:
        """Return the row as a plain ``dict``.

        Handy for ``json.dumps`` or for feeding ``pandas.DataFrame(...)``.

        Returns:
            A new dict mapping column name to value. If duplicate column names
            are present (possible after a join), later columns win.

        Example:
            >>> Row(("a", "b"), (1, 2)).as_dict()
            {'a': 1, 'b': 2}
        """
        return dict(zip(self._fields, self._values))

    def __getitem__(self, key: int | slice | str) -> Any:
        """Look up a value by position, slice, or column name.

        Args:
            key: A zero-based index, a slice, or a column name.

        Returns:
            The value, or a tuple of values when ``key`` is a slice.

        Raises:
            IndexError: If an integer index is out of range.
            KeyError: If no column has the given name.
        """
        if isinstance(key, (int, slice)):
            return self._values[key]
        try:
            return self._values[self._fields.index(key)]
        except ValueError:
            raise KeyError(f"No column named {key!r} in {self._fields}") from None

    def __getattr__(self, name: str) -> Any:
        """Expose columns as attributes, e.g. ``row.amount``.

        Only called for names not found normally, so it never shadows real
        attributes or methods.

        Raises:
            AttributeError: If no column has that name. Columns whose names are
                not valid Python identifiers must be read via ``row["..."]``.
        """
        try:
            fields = object.__getattribute__(self, "_fields")
            values = object.__getattribute__(self, "_values")
            return values[fields.index(name)]
        except (ValueError, AttributeError):
            raise AttributeError(
                f"{type(self).__name__!r} object has no attribute {name!r}"
            ) from None

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("Row is immutable")

    def __reduce__(self):
        """Support ``copy`` and ``pickle`` despite ``__slots__`` immutability.

        Rebuilding through the constructor avoids the blocked ``__setattr__``,
        so rows survive being cached or sent across processes (e.g. Ray).
        """
        return (Row, (self._fields, self._values))

    def __len__(self) -> int:
        return len(self._values)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._values)

    def __contains__(self, item: Any) -> bool:
        return item in self._values

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Row):
            return NotImplemented
        return self._fields == other._fields and self._values == other._values

    def __hash__(self) -> int:
        return hash((self._fields, self._values))

    def __repr__(self) -> str:
        inner = ", ".join(f"{f}={v!r}" for f, v in zip(self._fields, self._values))
        return f"Row({inner})"
