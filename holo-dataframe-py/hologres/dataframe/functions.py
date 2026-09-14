"""Top-level functions for building column expressions.

Imported from the package root::

    import hologres.dataframe as hg

Provided entry points:

* ``hg.col(name)`` / ``hg.lit(value)`` -- reference a column / a constant.
* ``hg.sum``/``hg.avg``/``hg.max``/``hg.min``/``hg.count`` -- aggregate helpers.
* ``hg.function(name, *cols)`` -- general named function call.
* ``hg.expr(sql_text)`` -- the single escape hatch for raw SQL fragments;
  callers own the injection risk.
"""

from __future__ import annotations

import re
from typing import Any

from . import _expr
from .column import Column
from .exceptions import InvalidArgumentError

__all__ = [
    "avg",
    "col",
    "count",
    "expr",
    "function",
    "lit",
    "max",
    "min",
    "sum",
]


# A SQL function name, optionally schema-qualified. Deliberately conservative
# to keep `hg.function()` safe from injection: FunctionCall renders the name
# via sql.SQL (no escaping), so the public builder must guarantee it looks like
# a bare identifier.
_FN_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z_0-9$]*(?:\.[A-Za-z_][A-Za-z_0-9$]*)?")


def col(name: str) -> Column:
    """Reference a column by name.

    The returned :class:`~hologres.dataframe.column.Column` holds no data; it is
    a symbol that compiles to a SQL identifier and is evaluated by Hologres.

    Args:
        name: The column name. Emitted as a quoted SQL identifier, so it is
            case-sensitive and injection-safe.

    Returns:
        A ``Column`` referencing that column.

    Raises:
        InvalidArgumentError: If ``name`` is not a non-empty string.

    Example:
        >>> import hologres.dataframe as hg
        >>> hg.col("amount")._to_sql().as_string()
        '"amount"'
        >>> (hg.col("amount") * 0.06)._to_sql().as_string()
        '("amount" * 0.06)'
    """
    if not isinstance(name, str) or not name:
        raise InvalidArgumentError("col() name must be a non-empty string")
    return Column(_expr.ColumnRef(name))


def lit(value: Any) -> Column:
    """Wrap a Python value as a literal column expression.

    You rarely need this in arithmetic or comparisons, where bare values are
    auto-promoted (``col("x") * 0.06`` works). Reach for ``lit`` when there is
    no column to attach to, or to disambiguate a string value from a column
    name:

    * standalone constant column -- ``select(lit(1).alias("const"))``;
    * string value vs. column name -- ``lit("region")`` is the text
      ``'region'``, while ``col("region")`` / the bare string ``"region"`` in a
      projection is the *column*.

    Args:
        value: Any Python value. ``None`` becomes ``NULL``, ``bool`` becomes
            ``true``/``false``, strings are quoted and escaped, and lists become
            array literals.

    Returns:
        A ``Column`` wrapping the literal.

    Example:
        >>> import hologres.dataframe as hg
        >>> hg.lit(100)._to_sql().as_string()
        '100'
        >>> hg.lit("region")._to_sql().as_string()
        "'region'"
        >>> hg.lit(None)._to_sql().as_string()
        'NULL'
    """
    return Column(_expr.Literal(value))


# ---------------------------------------------------------------------------
# Aggregate helpers and general function/expr escape hatches.
# ---------------------------------------------------------------------------


def _to_column_arg(value: Any, fn_name: str) -> Column:
    """Coerce an aggregate-function argument to a Column.

    Bare strings become column references (``str → col(value)``); an already-
    built Column passes through; anything else is a build-time error.
    """
    if isinstance(value, Column):
        return value
    if isinstance(value, str):
        if not value:
            raise InvalidArgumentError(f"{fn_name}() column name must be non-empty")
        return col(value)
    raise InvalidArgumentError(
        f"{fn_name}() expects a column name (str) or Column, got {type(value).__name__}"
    )


def _simple_agg(fn_name: str, arg: Any) -> Column:
    """Build a one-argument aggregate FunctionCall."""
    column = _to_column_arg(arg, fn_name)
    return Column(_expr.FunctionCall(fn_name, [column._expr]))


def sum(arg: str | Column) -> Column:  # noqa: A001 - shadow of built-in is intentional
    """Build the aggregate expression ``SUM(arg)``.

    Args:
        arg: Column name (str) or :class:`~hologres.dataframe.column.Column`.
            ``"*"`` is not accepted -- use :func:`count` for count-of-rows.

    Returns:
        A ``Column`` wrapping the ``sum(...)`` aggregate. Its auto-name is
        ``"sum(<col>)"`` when the argument is a bare column reference; otherwise
        the caller must supply ``.alias(...)`` in ``agg()``.

    Raises:
        InvalidArgumentError: If ``arg`` is not a str or Column.

    Example:
        >>> import hologres.dataframe as hg
        >>> hg.sum("amount")._to_sql().as_string()
        'sum("amount")'
        >>> hg.sum(hg.col("amount")).output_name
        'sum(amount)'
    """
    return _simple_agg("sum", arg)


def avg(arg: str | Column) -> Column:
    """Build the aggregate expression ``AVG(arg)``.

    Args, Returns, Raises: same shape as :func:`sum`.

    Example:
        >>> import hologres.dataframe as hg
        >>> hg.avg("amount")._to_sql().as_string()
        'avg("amount")'
    """
    return _simple_agg("avg", arg)


def max(arg: str | Column) -> Column:  # noqa: A001 - shadow of built-in is intentional
    """Build the aggregate expression ``MAX(arg)``.

    Args, Returns, Raises: same shape as :func:`sum`.

    Example:
        >>> import hologres.dataframe as hg
        >>> hg.max("amount")._to_sql().as_string()
        'max("amount")'
    """
    return _simple_agg("max", arg)


def min(arg: str | Column) -> Column:  # noqa: A001 - shadow of built-in is intentional
    """Build the aggregate expression ``MIN(arg)``.

    Args, Returns, Raises: same shape as :func:`sum`.

    Example:
        >>> import hologres.dataframe as hg
        >>> hg.min("amount")._to_sql().as_string()
        'min("amount")'
    """
    return _simple_agg("min", arg)


def count(arg: str | Column) -> Column:
    """Build the aggregate expression ``COUNT(arg)`` (or ``COUNT(*)``).

    Special case: ``hg.count("*")`` renders as ``COUNT(*)`` (wildcard, not a
    column named ``"*"``). Any other string is treated as a column name.

    Args:
        arg: ``"*"`` for a row count, or a column name / :class:`Column`.

    Returns:
        A ``Column`` wrapping the count aggregate. The auto-name is
        ``"count"`` for ``count("*")`` and ``"count(<col>)"`` for a bare column.

    Raises:
        InvalidArgumentError: If ``arg`` is not a str or Column.

    Example:
        >>> import hologres.dataframe as hg
        >>> hg.count("*")._to_sql().as_string()
        'count(*)'
        >>> hg.count("*").output_name
        'count'
        >>> hg.count("region")._to_sql().as_string()
        'count("region")'
    """
    if isinstance(arg, str) and arg == "*":
        return Column(_expr.FunctionCall("count", [], is_star=True))
    if (
        isinstance(arg, Column)
        and isinstance(arg._expr, _expr.ColumnRef)
        and arg._alias is None
        and arg._expr.name == "*"
    ):
        return Column(_expr.FunctionCall("count", [], is_star=True))
    return _simple_agg("count", arg)


def function(name: str, *cols: str | Column) -> Column:
    """Call any named SQL function (scalar or aggregate).

    Use this for PostgreSQL builtins we do not wrap explicitly (``stddev``,
    ``array_agg``, ``string_agg``, ``percentile_cont``, ...) or for user
    functions / UDFs. Arguments are always escaped; the function name itself is
    controlled by the caller (a Python string literal in your code) and is
    embedded as-is, so do not build it from user input.

    Args:
        name: The SQL function name.
        *cols: Positional argument expressions. Bare strings become column
            references; use :func:`lit` for a literal argument.

    Returns:
        A ``Column`` wrapping the function call. Auto-name is
        ``"name(<col>)"`` for a single bare-column argument; otherwise the
        caller must supply ``.alias(...)`` in ``agg()``.

    Raises:
        InvalidArgumentError: If ``name`` is not a non-empty string, or an
            argument is neither a str nor a Column.

    Example:
        >>> import hologres.dataframe as hg
        >>> hg.function("stddev", hg.col("x"))._to_sql().as_string()
        'stddev("x")'
        >>> hg.function("string_agg", hg.col("name"), hg.lit(","))._to_sql().as_string()
        'string_agg("name", \\',\\')'
    """
    if not isinstance(name, str) or not name:
        raise InvalidArgumentError("function() name must be a non-empty string")
    if not _FN_NAME_RE.fullmatch(name):
        raise InvalidArgumentError(
            "function() name must be a bare SQL identifier (optionally "
            f"schema-qualified), got {name!r}"
        )
    args = [_to_column_arg(c, name)._expr for c in cols]
    return Column(_expr.FunctionCall(name, args))


def expr(sql_text: str) -> Column:
    """Wrap a raw SQL fragment as a Column (**injection escape hatch**).

    Reach for this only when the fragment cannot be expressed via the other
    builders (e.g. ``count(*) filter (where paid)``, ``percentile_cont(0.5)
    within group (order by amount)``, ``count(distinct user_id)``). Prefer
    :func:`function` and the aggregate helpers whenever they suffice.

    Warning:
        The fragment is emitted **verbatim**, without escaping or
        parameterization. It is the caller's responsibility to ensure the
        fragment is safe -- **never build it by concatenating user input**.

    Args:
        sql_text: A SQL fragment valid inside a SELECT projection.

    Returns:
        A ``Column`` wrapping the raw fragment. It has no auto-name; supply
        ``.alias(...)`` when using it inside ``agg()`` or ``select()``.

    Raises:
        InvalidArgumentError: If ``sql_text`` is not a non-empty string.

    Example:
        >>> import hologres.dataframe as hg
        >>> hg.expr("count(*) filter (where paid)")._to_sql().as_string()
        'count(*) filter (where paid)'
    """
    if not isinstance(sql_text, str) or not sql_text:
        raise InvalidArgumentError("expr() sql_text must be a non-empty string")
    return Column(_expr.RawExpr(sql_text))
