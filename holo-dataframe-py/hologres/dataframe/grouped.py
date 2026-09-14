"""The :class:`GroupedDataFrame` returned by :meth:`DataFrame.group_by`.

Users do not construct this directly; they get one by calling ``df.group_by(...)``
and then call either the generic :meth:`agg` or one of the shortcut methods.

All output columns get a name:

* An explicit ``.alias(...)`` always wins.
* Otherwise, single-argument aggregates over a bare column reference auto-name
  themselves ``fn(col)`` (e.g. ``hg.sum(hg.col("amount"))`` -> ``sum(amount)``);
  ``hg.count("*")`` auto-names to ``count``.
* Anything else (``hg.expr(...)``, multi-argument function calls) must be
  aliased or the call raises at build time.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from . import _expr, _plan
from .column import Column
from .exceptions import InvalidArgumentError

if TYPE_CHECKING:
    from .dataframe import DataFrame

__all__ = ["GroupedDataFrame"]


class GroupedDataFrame:
    """A DataFrame grouped by zero or more keys, awaiting aggregation.

    A ``GroupedDataFrame`` is inert until :meth:`agg` (or one of the shortcut
    methods) is called; that call produces a new :class:`DataFrame`.

    Example:
        >>> from hologres.table_name import TableName
        >>> from hologres.dataframe import _plan
        >>> from hologres.dataframe.dataframe import DataFrame
        >>> import hologres.dataframe as hg
        >>> df = DataFrame(_plan.TableScan(TableName.valueOf("t")))
        >>> out = df.group_by("region").agg(hg.sum("amount").alias("total"))
        >>> out._to_sql().as_string()
        'SELECT "region", sum("amount") AS "total" FROM "public"."t" GROUP BY "region"'
    """

    __slots__ = ("_df", "_keys")

    def __init__(self, dataframe: DataFrame, keys: list[Column]) -> None:
        self._df = dataframe
        self._keys = list(keys)

    # -- generic aggregation -----------------------------------------------

    def agg(self, *exprs: Column) -> DataFrame:
        """Produce a DataFrame from one or more aggregate expressions.

        Naming rule for each aggregation expression:

        * An explicit ``.alias(name)`` wins.
        * Otherwise, if the expression can auto-name itself (see the module
          docstring), that name is used.
        * Otherwise a build-time :class:`InvalidArgumentError` asks for
          ``.alias(...)``.

        Args:
            *exprs: One or more :class:`~hologres.dataframe.column.Column`
                aggregate expressions.

        Returns:
            A new ``DataFrame`` with columns ``[*group_keys, *aggregations]``.

        Raises:
            InvalidArgumentError: If no expressions are given, an argument is
                not a :class:`Column`, or an aggregation cannot be named.

        Example:
            >>> from hologres.table_name import TableName
            >>> from hologres.dataframe import _plan
            >>> from hologres.dataframe.dataframe import DataFrame
            >>> import hologres.dataframe as hg
            >>> df = DataFrame(_plan.TableScan(TableName.valueOf("orders")))
            >>> sql_text = df.group_by("region").agg(
            ...     hg.sum("amount").alias("total"),
            ...     hg.count("*").alias("n"),
            ... )._to_sql().as_string()
            >>> "GROUP BY " in sql_text and 'AS "total"' in sql_text
            True
        """
        if not exprs:
            raise InvalidArgumentError(
                "agg() requires at least one aggregate expression"
            )
        aggregations: list[Column] = []
        for column in exprs:
            if not isinstance(column, Column):
                raise InvalidArgumentError(
                    "agg() arguments must be Column instances, got "
                    f"{type(column).__name__}"
                )
            named = self._require_named(column)
            aggregations.append(named)
        # Both paths (pinned or not) should reject duplicate output names so
        # the emitted SQL and the derived schema agree.
        names = [a._alias for a in aggregations]
        duplicates = sorted({n for n in names if names.count(n) > 1})
        if duplicates:
            raise InvalidArgumentError(
                f"agg() produced duplicate output column name(s) "
                f"{duplicates}; call .alias(name) to distinguish them"
            )
        plan = _plan.Aggregate(self._df._plan, self._keys, aggregations)
        return self._df._derive(plan, new_schema=self._derive_schema(aggregations))

    # -- shortcuts ---------------------------------------------------------

    def count(self) -> DataFrame:
        """Aggregate to a row count per group, as ``COUNT(*) AS count``.

        Shortcut for ``.agg(hg.count("*").alias("count"))``.

        Example:
            >>> from hologres.table_name import TableName
            >>> from hologres.dataframe import _plan
            >>> from hologres.dataframe.dataframe import DataFrame
            >>> df = DataFrame(_plan.TableScan(TableName.valueOf("t")))
            >>> df.group_by("region").count()._to_sql().as_string()
            'SELECT "region", count(*) AS "count" FROM "public"."t" GROUP BY "region"'
        """
        from .functions import count as _count

        return self.agg(_count("*"))

    def sum(self, arg: str | Column) -> DataFrame:  # noqa: A003
        """Aggregate to ``SUM(arg)`` per group, aliased ``sum(<arg>)``.

        Example:
            >>> from hologres.table_name import TableName
            >>> from hologres.dataframe import _plan
            >>> from hologres.dataframe.dataframe import DataFrame
            >>> df = DataFrame(_plan.TableScan(TableName.valueOf("t")))
            >>> out = df.group_by("r").sum("a")._to_sql().as_string()
            >>> "sum(\\"a\\") AS \\"sum(a)\\"" in out and 'GROUP BY "r"' in out
            True
        """
        from .functions import sum as _sum

        return self.agg(_sum(arg))

    def avg(self, arg: str | Column) -> DataFrame:
        """Aggregate to ``AVG(arg)`` per group, aliased ``avg(<arg>)``."""
        from .functions import avg as _avg

        return self.agg(_avg(arg))

    def max(self, arg: str | Column) -> DataFrame:  # noqa: A003
        """Aggregate to ``MAX(arg)`` per group, aliased ``max(<arg>)``."""
        from .functions import max as _max

        return self.agg(_max(arg))

    def min(self, arg: str | Column) -> DataFrame:  # noqa: A003
        """Aggregate to ``MIN(arg)`` per group, aliased ``min(<arg>)``."""
        from .functions import min as _min

        return self.agg(_min(arg))

    # -- internals ---------------------------------------------------------

    @staticmethod
    def _require_named(column: Column) -> Column:
        """Ensure ``column`` has a name (explicit alias or auto-name)."""
        if isinstance(column._expr, _expr.SortExpr):
            raise InvalidArgumentError(
                "agg() does not accept a sort column (from asc()/desc()); "
                "attach direction inside sort()/order_by() instead"
            )
        if column._alias is not None:
            return column
        auto = column._expr.output_name
        if auto is not None:
            return column.alias(auto)
        raise InvalidArgumentError(
            "aggregate expression must have a name; call .alias(name) on it. "
            "Only single-column aggregates auto-name themselves (e.g. "
            "hg.sum(hg.col('x')) -> 'sum(x)')."
        )

    def _derive_schema(self, aggregations: list[Column]):
        """Compute the derived schema after aggregation.

        Keys keep their upstream types; aggregation columns get a placeholder
        type (as with derived projections elsewhere) because full type
        inference over arbitrary SQL functions is out of scope.
        """
        parent = self._df._schema
        if parent is None:
            return None
        from .types import StringType, StructField, StructType

        fields = []
        for key in self._keys:
            source_name = None
            if isinstance(key._expr, _expr.ColumnRef):
                source_name = key._expr.name
            out_name = key._alias or (
                source_name if source_name is not None else key._expr.output_name
            )
            if out_name is None:
                # A derived group key without an alias -- rare, but we handle it
                # symmetrically with select() semantics by requiring a name.
                raise InvalidArgumentError(
                    "group_by() derived expressions need .alias(name)"
                )
            if source_name is not None and source_name in parent.names:
                source = parent[source_name]
                fields.append(StructField(out_name, source.datatype, source.nullable))
            else:
                fields.append(StructField(out_name, StringType()))
        for agg in aggregations:
            # After _require_named, every aggregation has a stable alias.
            fields.append(StructField(agg._alias, StringType()))
        return StructType(fields)

    def __repr__(self) -> str:
        keys = ", ".join(repr(k) for k in self._keys) or "<global>"
        return f"GroupedDataFrame(keys=[{keys}])"
