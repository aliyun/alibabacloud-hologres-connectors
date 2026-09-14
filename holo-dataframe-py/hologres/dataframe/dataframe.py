"""The lazy :class:`DataFrame` and its transformation operators.

A ``DataFrame`` wraps an immutable logical plan. Every transformation
(``select``, ``filter``, ``sort``, ...) returns a **new** ``DataFrame`` with an
extended plan; nothing runs until an action (added in a later commit) compiles
the plan to SQL and executes it. Transformations are therefore lazy, immutable,
and freely chainable.

    import hologres.dataframe as hg

    result = (
        df.filter(hg.col("amount") > 100)
          .with_column("tax", hg.col("amount") * 0.06)
          .select("order_id", "region", "amount", "tax")
          .sort(hg.col("amount"), ascending=False)
    )

Column arguments follow the design's naming rule: a bare string in an operator
position is a **column name** (``select("region")`` selects the column), which
is the opposite of a bare string inside an expression, where it is a literal.
"""

from __future__ import annotations

import functools
from typing import Any

from psycopg import sql

from . import _expr, _plan
from ._compiler import compile_plan
from .column import Column
from .exceptions import InvalidArgumentError
from .functions import col, function, lit
from .grouped import GroupedDataFrame
from .row import Row
from .types import (
    ArrayType,
    DecimalType,
    DoubleType,
    FloatType,
    StructField,
    StructType,
)

__all__ = ["DataFrame"]

# Sentinel for optional args where None is a meaningful value.
_MISSING = object()

# Internal column name for the row number drop_duplicates filters on. Prefixed
# so it cannot collide with a real column.
_DEDUP_RANK = "__hg_dedup_rank"


def _to_column(value: Any) -> Column:
    """Resolve an operator argument to a Column (bare string = column name)."""
    if isinstance(value, Column):
        return value
    if isinstance(value, str):
        return col(value)
    raise InvalidArgumentError(
        f"expected a column name (str) or Column, got {type(value).__name__}"
    )


class DataFrame:
    """A lazy, immutable table expression that compiles to a single SQL query.

    You do not construct this directly; obtain one from a session
    (``session.table(...)`` / ``session.sql(...)``, added later) and chain
    transformations. Each method returns a new ``DataFrame``.

    Example:
        >>> from hologres.table_name import TableName
        >>> from hologres.dataframe import _plan
        >>> import hologres.dataframe as hg
        >>> df = DataFrame(_plan.TableScan(TableName.valueOf("public.orders")))
        >>> df.filter(hg.col("amount") > 100).select("order_id")._to_sql().as_string()
        'SELECT "order_id" FROM "public"."orders" WHERE ("amount" > 100)'
    """

    __slots__ = (
        "_plan",
        "_session",
        "_schema",
        "_cached_schema",
        "_execution_options",
    )

    def __init__(
        self,
        plan: _plan.LogicalPlan,
        session: Any = None,
        schema: Any = None,
        execution_options: Any = None,
    ) -> None:
        self._plan = plan
        self._session = session
        # _schema is pinned (from session.table) when known at construction
        # time; None means it must be fetched lazily via session.describe.
        self._schema = schema
        self._cached_schema = None
        self._execution_options = execution_options

    # -- internals ----------------------------------------------------------

    def _derive(self, plan: _plan.LogicalPlan, new_schema: Any = _MISSING) -> DataFrame:
        """Build a new DataFrame from a derived plan, keeping the session.

        If ``new_schema`` is supplied it becomes the pinned schema of the
        result; otherwise the parent's pin propagates unchanged (which is
        correct for operators that don't reshape the columns).
        """
        schema = self._schema if new_schema is _MISSING else new_schema
        return DataFrame(
            plan,
            self._session,
            schema=schema,
            execution_options=self._execution_options,
        )

    def _to_sql(self) -> sql.Composable:
        """Compile this DataFrame's plan to a single PostgreSQL statement.

        Returns:
            A :class:`psycopg.sql.Composed`; render with ``.as_string()``.
        """
        return compile_plan(self._plan)

    def _prepare_sql(self) -> sql.Composable:
        """Validate capabilities, deploy remote functions, then compile."""
        if self._session is None:
            raise InvalidArgumentError(
                "this DataFrame has no session; obtain it from "
                "session.table() or session.sql() first"
            )
        ensure_plan_capabilities = getattr(
            self._session, "_ensure_plan_capabilities", None
        )
        if ensure_plan_capabilities is not None:
            ensure_plan_capabilities(self._plan)
        ensure_remote_functions = getattr(
            self._session, "_ensure_remote_functions", None
        )
        if ensure_remote_functions is not None:
            ensure_remote_functions(self._plan)
        return self._to_sql()

    # -- schema access ------------------------------------------------------

    @property
    def schema(self):
        """Result schema as a :class:`~hologres.dataframe.types.StructType`.

        For a DataFrame built from :meth:`Session.table`, this is free (the
        schema is pinned at construction and propagated through transformations
        that preserve or narrow it). For one built from :meth:`Session.sql`, the
        first access issues a ``SELECT * FROM (<sql>) LIMIT 0`` describe query
        against Hologres and caches the result on the DataFrame.

        Raises:
            InvalidArgumentError: If this DataFrame has no session (so the
                describe cannot run) and no pinned schema.

        Example:
            >>> t = session.table("public.orders")  # doctest: +SKIP
            >>> t.select("order_id").schema.names  # doctest: +SKIP
            ('order_id',)
        """
        if self._schema is not None:
            return self._schema
        if self._cached_schema is not None:
            return self._cached_schema
        if self._session is None:
            raise InvalidArgumentError(
                "cannot resolve schema without a session; "
                "obtain this DataFrame from session.table() or session.sql()"
            )
        self._cached_schema = self._session.describe(self._prepare_sql())
        return self._cached_schema

    @property
    def columns(self) -> list[str]:
        """Result column names, in order.

        Convenience for ``list(df.schema.names)`` -- see :attr:`schema` for the
        cost model.
        """
        return list(self.schema.names)

    def __getitem__(self, name: str) -> Column:
        """Reference a column of *this* DataFrame: ``df["amount"]``.

        The returned column is bound to this relation, which is what makes join
        predicates unambiguous: in ``a.join(b, on=a["uid"] == b["id"])`` each
        side renders with the alias the compiler gives it (``"t0"."uid" =
        "t1"."id"``). Outside a join the binding is inert and the column renders
        as a bare name, exactly like :func:`hologres.dataframe.col`.

        Use ``hg.col(name)`` instead to reference an *output* column of an
        already-computed result (for example after a join has been wrapped in a
        subquery).

        Args:
            name: The column name.

        Returns:
            A :class:`~hologres.dataframe.column.Column`.

        Raises:
            InvalidArgumentError: If this DataFrame has a pinned schema and
                ``name`` is not in it.

        Example:
            >>> from hologres.table_name import TableName
            >>> from hologres.dataframe import _plan
            >>> df = DataFrame(_plan.TableScan(TableName.valueOf("t")))
            >>> df["amount"]._to_sql().as_string()
            '"amount"'
        """
        if self._schema is not None and name not in self._schema.names:
            raise InvalidArgumentError(
                f"column {name!r} is not in schema {tuple(self._schema.names)}"
            )
        return Column(_expr.ColumnRef(name, relation=self._plan))

    def __repr__(self) -> str:
        pending = next(
            (
                node
                for node in _plan.iter_nodes(self._plan)
                if isinstance(node, _plan.PendingSource) and node.resolved is None
            ),
            None,
        )
        if pending is not None:
            columns = "unknown"
            if pending.schema is not None:
                columns = ", ".join(
                    f"{field.name} {field.datatype.sql_name}"
                    for field in pending.schema.fields
                )
            return f"DataFrame<{type(self._plan).__name__} pending_source=({columns})>"
        return f"DataFrame<{self._to_sql().as_string()}>"

    # -- projection ---------------------------------------------------------

    def select(self, *cols: str | Column) -> DataFrame:
        """Choose or derive the output columns (a projection).

        Args:
            *cols: Column names (str) or :class:`~hologres.dataframe.column.Column`
                expressions, in output order. At least one is required.

        Returns:
            A new ``DataFrame`` projecting those columns.

        Raises:
            InvalidArgumentError: If no columns are given, or an argument is
                neither a str nor a Column.

        Example:
            >>> from hologres.table_name import TableName
            >>> from hologres.dataframe import _plan
            >>> import hologres.dataframe as hg
            >>> df = DataFrame(_plan.TableScan(TableName.valueOf("orders")))
            >>> df.select("order_id", hg.col("amount") * 0.06)._to_sql().as_string()
            'SELECT "order_id", ("amount" * 0.06) FROM "public"."orders"'
        """
        if not cols:
            raise InvalidArgumentError("select() requires at least one column")
        columns = [_to_column(c) for c in cols]
        return self._derive(
            _plan.Project(self._plan, columns),
            new_schema=(
                _derive_project_schema(self._schema, columns)
                if self._schema is not None
                else None
            ),
        )

    def with_column(self, name: str, column: Column) -> DataFrame:
        """Add a derived column, keeping all existing columns.

        Compiles to ``SELECT *, <expr> AS name``.

        Note:
            This appends; it does not replace. Passing the name of an existing
            column adds a second column with the same name (``SELECT *, expr AS
            name``), so a later reference to that name is ambiguous. True
            in-place replacement needs the table schema and arrives in a later
            commit.

        Args:
            name: Name for the new column.
            column: A :class:`~hologres.dataframe.column.Column` expression. Use
                ``hg.lit(...)`` for a constant.

        Returns:
            A new ``DataFrame`` with the added column.

        Raises:
            InvalidArgumentError: If ``name`` is not a non-empty string or
                ``column`` is not a Column.

        Example:
            >>> from hologres.table_name import TableName
            >>> from hologres.dataframe import _plan
            >>> import hologres.dataframe as hg
            >>> df = DataFrame(_plan.TableScan(TableName.valueOf("orders")))
            >>> df.with_column("tax", hg.col("amount") * 0.06)._to_sql().as_string()
            'SELECT *, ("amount" * 0.06) AS "tax" FROM "public"."orders"'
        """
        if not isinstance(name, str) or not name:
            raise InvalidArgumentError("with_column name must be a non-empty string")
        if not isinstance(column, Column):
            raise InvalidArgumentError(
                f"with_column value must be a Column, got {type(column).__name__}; "
                f"use hg.lit(...) for a constant"
            )
        star = Column(_expr.Star())
        return self._derive(
            _plan.Project(self._plan, [star, column.alias(name)]),
            new_schema=(
                _extend_schema(self._schema, [name], [column])
                if self._schema is not None
                else None
            ),
        )

    def with_columns(self, mapping: dict[str, Column]) -> DataFrame:
        """Add several derived columns at once, keeping existing columns.

        Compiles to ``SELECT *, expr1 AS k1, expr2 AS k2, ...`` in the order of
        the mapping.

        Note:
            Like :meth:`with_column`, this appends rather than replaces; reusing
            an existing column name produces a duplicate output column. True
            in-place replacement needs the table schema and arrives in a later
            commit.

        Args:
            mapping: An ordered mapping of new column name to
                :class:`~hologres.dataframe.column.Column` expression.

        Returns:
            A new ``DataFrame`` with the added columns.

        Raises:
            InvalidArgumentError: If ``mapping`` is not a non-empty dict, or any
                key is not a non-empty string, or any value is not a Column.

        Example:
            >>> from hologres.table_name import TableName
            >>> from hologres.dataframe import _plan
            >>> import hologres.dataframe as hg
            >>> df = DataFrame(_plan.TableScan(TableName.valueOf("t")))
            >>> df.with_columns(
            ...     {"x": hg.col("a") + 1, "y": hg.col("a") > 1}
            ... )._to_sql().as_string()
            'SELECT *, ("a" + 1) AS "x", ("a" > 1) AS "y" FROM "public"."t"'
        """
        if not isinstance(mapping, dict) or not mapping:
            raise InvalidArgumentError("with_columns requires a non-empty dict")
        projected: list[Column] = [Column(_expr.Star())]
        for key, value in mapping.items():
            if not isinstance(key, str) or not key:
                raise InvalidArgumentError(
                    "with_columns keys must be non-empty strings"
                )
            if not isinstance(value, Column):
                raise InvalidArgumentError(
                    f"with_columns value for {key!r} must be a Column, "
                    f"got {type(value).__name__}"
                )
            projected.append(value.alias(key))
        return self._derive(
            _plan.Project(self._plan, projected),
            new_schema=(
                _extend_schema(
                    self._schema, list(mapping.keys()), list(mapping.values())
                )
                if self._schema is not None
                else None
            ),
        )

    # -- filtering ----------------------------------------------------------

    def filter(self, predicate: Column) -> DataFrame:
        """Keep only rows matching a predicate (a ``WHERE`` clause).

        Args:
            predicate: A boolean-valued
                :class:`~hologres.dataframe.column.Column`, e.g.
                ``hg.col("amount") > 100``.

        Returns:
            A new ``DataFrame`` with the predicate applied.

        Raises:
            InvalidArgumentError: If ``predicate`` is not a Column.

        Example:
            >>> from hologres.table_name import TableName
            >>> from hologres.dataframe import _plan
            >>> import hologres.dataframe as hg
            >>> df = DataFrame(_plan.TableScan(TableName.valueOf("orders")))
            >>> df.filter(hg.col("amount") > 100)._to_sql().as_string()
            'SELECT * FROM "public"."orders" WHERE ("amount" > 100)'
        """
        if not isinstance(predicate, Column):
            raise InvalidArgumentError(
                f"filter predicate must be a Column, got {type(predicate).__name__}"
            )
        return self._derive(_plan.Filter(self._plan, predicate))

    def where(self, predicate: Column) -> DataFrame:
        """Alias for :meth:`filter` (PostgreSQL/SQL wording)."""
        return self.filter(predicate)

    def dropna(self, subset: list[str | Column] | None = None) -> DataFrame:
        """Drop rows where any listed column is missing (NULL or NaN).

        A row is kept only if every column being checked passes a keep
        condition: for floating-point columns that is
        ``col IS NOT NULL AND col <> 'NaN'::float8``; for other columns it is
        just ``col IS NOT NULL`` (checking NaN there would be a type error).
        With ``subset=None`` (or omitted) all columns are checked, which
        requires the DataFrame to have a pinned schema.

        Args:
            subset: Column names or Columns to check. Pass ``None`` (default)
                to check every column, which requires a pinned schema.

        Returns:
            A new ``DataFrame`` filtered to rows with no missing values in
            ``subset``.

        Raises:
            InvalidArgumentError: If ``subset`` is an empty list, if it names a
                column that isn't in the pinned schema, or if ``subset`` is
                ``None`` on a DataFrame without a pinned schema.

        Note:
            When no pinned schema is available (e.g. this DataFrame came from
            ``session.sql(...)`` without a describe), the NaN check is applied
            unconditionally and will raise at execution time on non-float
            columns. Give an explicit ``subset`` of float columns to avoid this,
            or rely on schema-aware handling by starting from ``session.table``.

        Example:
            >>> from hologres.table_name import TableName
            >>> from hologres.dataframe import _plan
            >>> df = DataFrame(_plan.TableScan(TableName.valueOf("t")))
            >>> out = df.dropna(["amount"])._to_sql().as_string()
            >>> "IS NOT NULL" in out
            True
        """
        if subset is None:
            if self._schema is None:
                raise InvalidArgumentError(
                    "dropna() without a subset requires a pinned schema; "
                    "either pass subset=[...] or start this DataFrame from "
                    "session.table(...)"
                )
            names = list(self._schema.names)
            if not names:  # pragma: no cover - defensive: no public API reaches this
                raise InvalidArgumentError(
                    "dropna() on a schema with no columns is a no-op; "
                    "check your projection"
                )
            columns = [(col(n), n) for n in names]
        else:
            if not subset:
                raise InvalidArgumentError("dropna requires a non-empty subset")
            columns = []
            for entry in subset:
                column = _to_column(entry)
                # Type-aware handling looks up the *source* column name in the
                # pinned schema (an alias only renames the output; the source
                # column must still exist).
                if isinstance(column._expr, _expr.ColumnRef):
                    source_name: str | None = column._expr.name
                    if (
                        self._schema is not None
                        and source_name not in self._schema.names
                    ):
                        raise InvalidArgumentError(
                            f"column {source_name!r} is not in schema "
                            f"{tuple(self._schema.names)}"
                        )
                else:
                    source_name = None
                columns.append((column, source_name))

        conditions = [self._keep_condition(column, name) for column, name in columns]
        predicate = functools.reduce(lambda a, b: a & b, conditions)
        return self._derive(_plan.Filter(self._plan, predicate))

    def _keep_condition(self, column: Column, name: str | None) -> Column:
        """Build the per-column keep condition used by :meth:`dropna`.

        With a pinned schema and a known float column, both NULL and NaN are
        rejected; for a non-float column, only NULL is rejected (the NaN
        comparison would be a runtime type error). With no pinned schema (or an
        unresolvable name) we fall back to the conservative combined check.
        """
        if self._schema is not None and name in self._schema.names:
            datatype = self._schema[name].datatype
            if isinstance(datatype, DecimalType):
                # Compare NaN in the numeric type space to avoid implicit
                # promotion to float8 (which would lose precision).
                template = sql.SQL("({} IS NOT NULL AND {} <> 'NaN'::numeric)")
                return Column(
                    _expr.PredicateExpr(template, [column._expr, column._expr])
                )
            if _is_float_type(datatype):
                return column.is_not_null() & (column != float("nan"))
            return column.is_not_null()
        # No schema info -> conservative behavior; the NaN literal renders as
        # 'NaN'::float8 which will error at execution on non-float columns
        # (documented on dropna).
        return column.is_not_null() & (column != float("nan"))

    # -- schema-driven column rewrites -------------------------------------

    def drop(self, *cols: str) -> DataFrame:
        """Drop one or more columns by name.

        PostgreSQL has no ``SELECT * EXCEPT``, so ``drop`` expands the pinned
        schema and projects only the remaining columns. It therefore requires a
        DataFrame with a pinned schema (start from :meth:`Session.table`).

        Args:
            *cols: Column names to drop. Each must exist in the current schema.
                At least one name is required.

        Returns:
            A new ``DataFrame`` without the dropped columns.

        Raises:
            InvalidArgumentError: If no columns are given, an argument is not a
                string, this DataFrame has no pinned schema, or a name is not in
                the schema.

        Example:
            >>> from hologres.table_name import TableName
            >>> from hologres.dataframe import _plan
            >>> from hologres.dataframe.types import (
            ...     LongType, StringType, StructField, StructType,
            ... )
            >>> schema = StructType([
            ...     StructField("id", LongType()),
            ...     StructField("region", StringType()),
            ... ])
            >>> df = DataFrame(
            ...     _plan.TableScan(TableName.valueOf("t")), schema=schema
            ... )
            >>> df.drop("region")._to_sql().as_string()
            'SELECT "id" FROM "public"."t"'
        """
        if not cols:
            raise InvalidArgumentError("drop() requires at least one column name")
        for c in cols:
            if not isinstance(c, str) or not c:
                raise InvalidArgumentError(
                    f"drop() arguments must be non-empty strings, got {c!r}"
                )
        schema = self._require_pinned_schema("drop")
        names = list(schema.names)
        seen: set[str] = set()
        duplicates: list[str] = []
        for c in cols:
            if c in seen and c not in duplicates:
                duplicates.append(c)
            seen.add(c)
        if duplicates:
            raise InvalidArgumentError(
                f"drop() got duplicate column name(s): {duplicates}"
            )
        to_drop = set(cols)
        unknown = to_drop - set(names)
        if unknown:
            raise InvalidArgumentError(
                f"column {next(iter(unknown))!r} is not in schema {tuple(names)}"
            )
        remaining = [n for n in names if n not in to_drop]
        if not remaining:
            raise InvalidArgumentError("drop() would remove every column")
        projected = [col(n) for n in remaining]
        new_schema = StructType([schema[n] for n in remaining])
        return self._derive(_plan.Project(self._plan, projected), new_schema=new_schema)

    def rename(self, mapping: dict[str, str]) -> DataFrame:
        """Rename one or more columns.

        Preserves order and unchanged columns. Requires a pinned schema.

        Args:
            mapping: ``{"old_name": "new_name", ...}``. Every key must exist in
                the current schema; every value must be a non-empty string.

        Returns:
            A new ``DataFrame`` with the renamed columns.

        Raises:
            InvalidArgumentError: If ``mapping`` is empty, if any key/value is
                not a non-empty string, if a key is not in the schema, or if
                the result would contain duplicate names.

        Example:
            >>> from hologres.table_name import TableName
            >>> from hologres.dataframe import _plan
            >>> from hologres.dataframe.types import (
            ...     LongType, StringType, StructField, StructType,
            ... )
            >>> schema = StructType([
            ...     StructField("id", LongType()),
            ...     StructField("region", StringType()),
            ... ])
            >>> df = DataFrame(
            ...     _plan.TableScan(TableName.valueOf("t")), schema=schema
            ... )
            >>> df.rename({"region": "area"})._to_sql().as_string()
            'SELECT "id", "region" AS "area" FROM "public"."t"'
        """
        if not isinstance(mapping, dict) or not mapping:
            raise InvalidArgumentError("rename() requires a non-empty dict")
        for old, new in mapping.items():
            if not isinstance(old, str) or not old:
                raise InvalidArgumentError("rename() keys must be non-empty strings")
            if not isinstance(new, str) or not new:
                raise InvalidArgumentError(
                    f"rename() value for {old!r} must be a non-empty string"
                )
        schema = self._require_pinned_schema("rename")
        names = list(schema.names)
        unknown = set(mapping) - set(names)
        if unknown:
            raise InvalidArgumentError(
                f"column {next(iter(unknown))!r} is not in schema {tuple(names)}"
            )
        new_names = [mapping.get(n, n) for n in names]
        duplicates = sorted({n for n in new_names if new_names.count(n) > 1})
        if duplicates:
            raise InvalidArgumentError(
                f"rename() would produce duplicate column name(s): {duplicates}"
            )
        projected = [
            col(n).alias(mapping[n]) if n in mapping else col(n) for n in names
        ]
        new_schema = StructType(
            [
                StructField(
                    mapping.get(n, n),
                    schema[n].datatype,
                    schema[n].nullable,
                )
                for n in names
            ]
        )
        return self._derive(_plan.Project(self._plan, projected), new_schema=new_schema)

    def fillna(self, mapping: dict[str, Any]) -> DataFrame:
        """Replace NULL (and NaN, for float columns) values in named columns.

        For each key ``name`` in ``mapping`` with a pinned schema type:

        * If the column is a float type (``FloatType`` / ``DoubleType`` /
          ``DecimalType``), rewrite as
          ``CASE WHEN name IS NULL OR name = 'NaN'::float8 THEN v ELSE name END``.
        * Otherwise (non-float), rewrite as ``COALESCE(name, v)``, so no NaN
          comparison is attempted on a non-numeric column.

        Columns not in ``mapping`` pass through unchanged. Requires a pinned
        schema.

        Args:
            mapping: ``{"col_name": replacement_value, ...}``. Values are
                emitted as SQL literals via :func:`~hologres.dataframe.lit`.

        Returns:
            A new ``DataFrame`` with the replacements applied. Column types,
            names, and order are preserved.

        Raises:
            InvalidArgumentError: If ``mapping`` is empty, a key is not a
                non-empty string, or a key is not in the schema.

        Example:
            >>> from hologres.table_name import TableName
            >>> from hologres.dataframe import _plan
            >>> from hologres.dataframe.types import (
            ...     DoubleType, StringType, StructField, StructType,
            ... )
            >>> schema = StructType([
            ...     StructField("region", StringType()),
            ...     StructField("amount", DoubleType()),
            ... ])
            >>> df = DataFrame(
            ...     _plan.TableScan(TableName.valueOf("t")), schema=schema
            ... )
            >>> sql_text = df.fillna({"amount": 0.0}).\
_to_sql().as_string()
            >>> "CASE WHEN" in sql_text and "'NaN'::float8" in sql_text
            True
        """
        if not isinstance(mapping, dict) or not mapping:
            raise InvalidArgumentError("fillna() requires a non-empty dict")
        for name in mapping:
            if not isinstance(name, str) or not name:
                raise InvalidArgumentError("fillna() keys must be non-empty strings")
        for name, value in mapping.items():
            if isinstance(value, Column):
                raise InvalidArgumentError(
                    f"fillna() value for {name!r} must be a plain scalar "
                    "(str/int/float/bool/None/...), not a Column; "
                    "use with_column(...) for expression-based rewrites"
                )
        schema = self._require_pinned_schema("fillna")
        unknown = set(mapping) - set(schema.names)
        if unknown:
            raise InvalidArgumentError(
                f"column {next(iter(unknown))!r} is not in schema {tuple(schema.names)}"
            )
        projected = []
        for name in schema.names:
            if name not in mapping:
                projected.append(col(name))
                continue
            value = mapping[name]
            literal_expr = _expr.Literal(value)
            column_expr = _expr.ColumnRef(name)
            datatype = schema[name].datatype
            if _is_float_type(datatype):
                # For NUMERIC (DecimalType) compare against 'NaN'::numeric to
                # avoid an implicit promotion to float8 (which loses precision).
                nan_cast = (
                    "'NaN'::numeric"
                    if isinstance(datatype, DecimalType)
                    else "'NaN'::float8"
                )
                template = sql.SQL(
                    "CASE WHEN {} IS NULL OR {} = " + nan_cast + " THEN {} ELSE {} END"
                )
                pred = _expr.PredicateExpr(
                    template, [column_expr, column_expr, literal_expr, column_expr]
                )
            else:
                template = sql.SQL("COALESCE({}, {})")
                pred = _expr.PredicateExpr(template, [column_expr, literal_expr])
            projected.append(Column(pred).alias(name))
        # A fillna with a non-null replacement makes that column NOT NULL going
        # forward; a fillna(None) is odd but leaves nullability alone.
        replaced_names = {n for n, v in mapping.items() if v is not None}
        new_schema = StructType(
            [
                StructField(
                    n,
                    schema[n].datatype,
                    False if n in replaced_names else schema[n].nullable,
                )
                for n in schema.names
            ]
        )
        return self._derive(_plan.Project(self._plan, projected), new_schema=new_schema)

    def _require_pinned_schema(self, op: str):
        """Assert this DataFrame has a pinned schema; return it if so."""
        if self._schema is None:
            raise InvalidArgumentError(
                f"{op}() needs a pinned schema; start this DataFrame from "
                "session.table(name) so the column list is known"
            )
        return self._schema

    # -- ordering / paging --------------------------------------------------

    def sort(self, *cols: str | Column, ascending: bool = True) -> DataFrame:
        """Order rows by one or more columns (an ``ORDER BY`` clause).

        A column that already carries a direction (from ``.asc()``/``.desc()``)
        keeps it; any bare column takes the ``ascending`` default.

        Args:
            *cols: Column names or Columns to sort by, in priority order.
            ascending: Default direction for columns without their own; ``True``
                for ``ASC`` (default), ``False`` for ``DESC``.

        Returns:
            A new sorted ``DataFrame``.

        Raises:
            InvalidArgumentError: If no columns are given.

        Example:
            >>> from hologres.table_name import TableName
            >>> from hologres.dataframe import _plan
            >>> import hologres.dataframe as hg
            >>> df = DataFrame(_plan.TableScan(TableName.valueOf("orders")))
            >>> df.sort("region", hg.col("amount").desc())._to_sql().as_string()
            'SELECT * FROM "public"."orders" ORDER BY "region" ASC, "amount" DESC'
        """
        if not cols:
            raise InvalidArgumentError("sort() requires at least one column")
        orders = []
        for c in cols:
            column = _to_column(c)
            if isinstance(column._expr, _expr.SortExpr):
                orders.append(column)
            else:
                orders.append(column.asc() if ascending else column.desc())
        return self._derive(_plan.Sort(self._plan, orders))

    def order_by(self, *cols: str | Column, ascending: bool = True) -> DataFrame:
        """Alias for :meth:`sort` (PostgreSQL/SQL wording)."""
        return self.sort(*cols, ascending=ascending)

    def limit(self, n: int) -> DataFrame:
        """Cap the number of rows (a ``LIMIT`` clause).

        Args:
            n: Maximum number of rows; a non-negative integer.

        Returns:
            A new ``DataFrame`` with the limit applied.

        Raises:
            ValueError: If ``n`` is not a non-negative integer.

        Example:
            >>> from hologres.table_name import TableName
            >>> from hologres.dataframe import _plan
            >>> df = DataFrame(_plan.TableScan(TableName.valueOf("t")))
            >>> df.limit(10)._to_sql().as_string()
            'SELECT * FROM "public"."t" LIMIT 10'
        """
        return self._derive(_plan.Limit(self._plan, n))

    def offset(self, n: int) -> DataFrame:
        """Skip a number of leading rows (an ``OFFSET`` clause).

        Args:
            n: Number of rows to skip; a non-negative integer.

        Returns:
            A new ``DataFrame`` with the offset applied.

        Raises:
            ValueError: If ``n`` is not a non-negative integer.

        Example:
            >>> from hologres.table_name import TableName
            >>> from hologres.dataframe import _plan
            >>> df = DataFrame(_plan.TableScan(TableName.valueOf("t")))
            >>> df.offset(20)._to_sql().as_string()
            'SELECT * FROM "public"."t" OFFSET 20'
        """
        return self._derive(_plan.Offset(self._plan, n))

    # -- deduplication ------------------------------------------------------

    def distinct(self) -> DataFrame:
        """Remove duplicate rows (``SELECT DISTINCT``).

        Returns:
            A new ``DataFrame`` with duplicates removed across all columns.

        Example:
            >>> from hologres.table_name import TableName
            >>> from hologres.dataframe import _plan
            >>> df = DataFrame(_plan.TableScan(TableName.valueOf("t")))
            >>> df.distinct()._to_sql().as_string()
            'SELECT DISTINCT * FROM "public"."t"'
        """
        return self._derive(_plan.Distinct(self._plan, on=None))

    def drop_duplicates(self, subset: list[str | Column] | None = None) -> DataFrame:
        """Keep one row per distinct combination of ``subset`` columns.

        With no ``subset`` this is the same as :meth:`distinct`. With a subset it
        numbers the rows of each group with ``row_number() OVER (PARTITION BY
        ...)`` and keeps the first, then projects the original columns back.

        Note:
            PostgreSQL would express this as ``SELECT DISTINCT ON (...)``, but
            Hologres only accepts ``DISTINCT ON`` when the selected columns are
            exactly the ``ON`` columns -- selecting anything else fails in the
            planner. The window function is the portable spelling, at the cost of
            needing the column list, so a subset requires a pinned schema.

        Warning:
            Which row survives each group is unspecified (no ordering is
            imposed). Sort first and materialize if you need a particular one.

        Args:
            subset: Columns that define a duplicate, or ``None`` for all columns.

        Returns:
            A new deduplicated ``DataFrame``.

        Raises:
            InvalidArgumentError: If ``subset`` is an empty list, an entry is
                neither a str nor a Column, or this DataFrame has no pinned
                schema.

        Example:
            >>> from tests._fake_conn import make_session, FakeColumnInfo
            >>> info = [FakeColumnInfo(n, 25) for n in ("c", "d", "u")]
            >>> rows = [("a", "bigint", "int8"), ("b", "text", "text")]
            >>> session, _ = make_session(script=[(rows, info)])
            >>> out = session.table("public.t").drop_duplicates(["a"])._to_sql()
            >>> 'row_number() OVER (PARTITION BY "a")' in out.as_string()
            True
        """
        if subset is None:
            return self.distinct()
        if not subset:
            raise InvalidArgumentError(
                "drop_duplicates subset must be non-empty (or None for all columns)"
            )
        schema = self._require_pinned_schema("drop_duplicates")
        keys = []
        for c in subset:
            column = _to_column(c)
            # A subset entry only identifies a column; any asc/desc metadata is
            # meaningless for partitioning.
            if isinstance(column._expr, _expr.SortExpr):
                column = Column(column._expr.child)
            keys.append(column)

        row_number = Column(
            _expr.WindowExpr("row_number", [], [k._expr for k in keys])
        ).alias(_DEDUP_RANK)
        original = [col(f.name) for f in schema.fields]
        numbered = _plan.Project(self._plan, [*original, row_number])
        first_of_group = _plan.Filter(numbered, col(_DEDUP_RANK) == 1)
        return self._derive(_plan.Project(first_of_group, original))

    # -- aggregation --------------------------------------------------------

    def group_by(self, *cols: str | Column) -> GroupedDataFrame:
        """Group rows by zero or more keys, awaiting aggregation.

        Calling ``group_by()`` with no arguments requests a **global**
        aggregation -- the whole input collapses to one row and the produced
        SQL has no ``GROUP BY`` clause.

        With a pinned schema, bare column-name keys are validated at build time
        (Ibis-style "analysis before action"); derived expressions skip the
        check.

        Args:
            *cols: Column names (str) or ``Column`` expressions.

        Returns:
            A :class:`~hologres.dataframe.grouped.GroupedDataFrame`. Call
            :meth:`~hologres.dataframe.grouped.GroupedDataFrame.agg` or a
            shortcut (``sum``/``avg``/``count``/``max``/``min``) on it to get a
            DataFrame back.

        Raises:
            InvalidArgumentError: If an argument is neither a str nor a Column,
                or a bare column name is not in the pinned schema.

        Example:
            >>> from hologres.table_name import TableName
            >>> from hologres.dataframe import _plan
            >>> import hologres.dataframe as hg
            >>> df = DataFrame(_plan.TableScan(TableName.valueOf("t")))
            >>> out = df.group_by("region").agg(
            ...     hg.sum("amount").alias("total")
            ... )._to_sql().as_string()
            >>> 'AS "total"' in out and 'GROUP BY "region"' in out
            True
        """
        keys = []
        for c in cols:
            key = _to_column(c)
            if (
                self._schema is not None
                and isinstance(key._expr, _expr.ColumnRef)
                and key._alias is None
                and key._expr.name not in self._schema.names
            ):
                raise InvalidArgumentError(
                    f"column {key._expr.name!r} is not in schema "
                    f"{tuple(self._schema.names)}"
                )
            keys.append(key)
        return GroupedDataFrame(self, keys)

    # -- joins --------------------------------------------------------------

    def join(
        self,
        other: DataFrame,
        on: str | list[str] | Column | None = None,
        how: str = "inner",
        *,
        lsuffix: str | None = None,
        rsuffix: str | None = None,
    ) -> DataFrame:
        """Join this DataFrame with another, staying inside the engine.

        The condition takes either shape:

        * **USING** -- ``on="user_id"`` or ``on=["a", "b"]``: column names shared
          by both sides. The join columns are merged, so each appears **once** in
          the output. Prefer this form when the names line up; it cannot be
          ambiguous.
        * **ON** -- ``on=left["user_id"] == right["id"]``: any boolean predicate.
          Reference each side with ``df[...]`` (not ``hg.col``) so the compiler
          can qualify the columns with each input's alias.

        Args:
            other: The right-hand DataFrame. Must come from the same session
                (or from no session).
            on: The join condition, as described above. Only ``how="cross"``
                accepts ``None``.
            how: ``"inner"`` (default), ``"left"``, ``"right"``, ``"full"``, or
                ``"cross"``. Case-insensitive; ``"outer"``/``"full_outer"`` mean
                ``"full"`` and ``"left_outer"``/``"right_outer"`` are accepted
                too.
            lsuffix: Suffix appended to *this* side's columns whose names also
                occur on the other side.
            rsuffix: Suffix appended to ``other``'s conflicting columns.

        Returns:
            A new lazy ``DataFrame``.

        Raises:
            InvalidArgumentError: If ``other`` is not a DataFrame; if ``how`` is
                unknown; if the ``on``/``how`` combination is contradictory; if
                the two sides come from different sessions; if a ``USING`` column
                is missing on either side; or if the output would contain
                duplicate column names and no suffix was given.

        Note:
            Column names must be unique in the result, since ``df.columns`` and
            ``Row`` lookups key on them -- hence the immediate error on a
            collision. Outer joins can introduce NULLs on the padded side, but
            the derived schema does not widen ``nullable`` to reflect that.

        Example:
            >>> from hologres.table_name import TableName
            >>> from hologres.dataframe import _plan
            >>> orders = DataFrame(_plan.TableScan(TableName.valueOf("orders")))
            >>> users = DataFrame(_plan.TableScan(TableName.valueOf("users")))
            >>> out = orders.join(users, on="user_id")._to_sql().as_string()
            >>> 'INNER JOIN "public"."users" "t1" USING ("user_id")' in out
            True
            >>> pred = orders["user_id"] == users["id"]
            >>> out = orders.join(users, on=pred, how="left")._to_sql().as_string()
            >>> 'LEFT JOIN' in out and 'ON ("t0"."user_id" = "t1"."id")' in out
            True
        """
        if not isinstance(other, DataFrame):
            raise InvalidArgumentError(
                f"join expects a DataFrame, got {type(other).__name__}"
            )

        how_norm = _normalize_join_type(how)
        using, predicate = _parse_join_condition(on, how_norm)

        session = self._merged_session(other, "join")
        execution_options = self._merged_execution_options(other, "join")

        left_schema = self._resolvable_schema()
        right_schema = other._resolvable_schema()
        plan: _plan.LogicalPlan = _plan.Join(
            self._plan, other._plan, how_norm, on=predicate, using=using
        )
        schema, projection = _join_output(
            self._plan,
            left_schema,
            other._plan,
            right_schema,
            using,
            lsuffix,
            rsuffix,
        )
        if projection is not None:
            plan = _plan.Project(plan, projection)
        return DataFrame(
            plan,
            session,
            schema=schema,
            execution_options=execution_options,
        )

    def _resolvable_schema(self):
        """Return only schema information already known locally.

        Construction-time transformations must not issue describe queries,
        deploy remote functions, or resolve pending sources. An unknown schema
        is therefore left as ``None`` until an explicit schema access or action.
        """
        if self._schema is not None:
            return self._schema
        if self._cached_schema is not None:
            return self._cached_schema
        return None

    def _merged_session(self, other: DataFrame, op: str):
        """Return the single session shared by ``self`` and ``other``.

        A combined query runs on one connection, so the two inputs must not come
        from different sessions. A DataFrame with no session inherits the
        other's.
        """
        if (
            self._session is not None
            and other._session is not None
            and self._session is not other._session
        ):
            raise InvalidArgumentError(
                f"cannot {op} DataFrames from different sessions; the combined "
                "query is pushed down as one statement and needs one connection"
            )
        return self._session if self._session is not None else other._session

    def _merged_execution_options(self, other: DataFrame, op: str):
        """Merge execution settings for one combined SQL statement."""
        left = self._execution_options
        right = other._execution_options
        if left is not None and right is not None and left != right:
            raise InvalidArgumentError(
                f"cannot {op} DataFrames with different serverless settings; "
                "apply serverless(...) after combining them"
            )
        return left if left is not None else right

    # -- set operations -----------------------------------------------------

    def union(self, other: DataFrame) -> DataFrame:
        """Stack ``other``'s rows below this DataFrame's, removing duplicates.

        Compiles to SQL ``UNION``. For the variant that keeps duplicate rows,
        use :meth:`union_all`.

        Warning:
            Columns are matched **by position**, not by name -- exactly like SQL
            ``UNION``. The output takes *this* DataFrame's column names, and a
            name mismatch is silently accepted. Reorder with :meth:`select`
            first if the two schemas do not already line up.

        Args:
            other: The DataFrame to append. Must have the same number of columns
                and come from the same session (or from no session).

        Returns:
            A new lazy ``DataFrame``.

        Raises:
            InvalidArgumentError: If ``other`` is not a DataFrame, the two sides
                have different column counts, or they come from different
                sessions.

        Example:
            >>> from hologres.table_name import TableName
            >>> from hologres.dataframe import _plan
            >>> a = DataFrame(_plan.TableScan(TableName.valueOf("a")))
            >>> b = DataFrame(_plan.TableScan(TableName.valueOf("b")))
            >>> out = a.union(b)._to_sql().as_string()
            >>> 'UNION' in out and 'UNION ALL' not in out
            True
        """
        return self._union(other, distinct=True)

    def union_all(self, other: DataFrame) -> DataFrame:
        """Stack ``other``'s rows below this DataFrame's, keeping duplicates.

        Compiles to SQL ``UNION ALL``. See :meth:`union` for the column-matching
        rules; the only difference is that duplicate rows are preserved.

        Args:
            other: The DataFrame to append. Must have the same number of columns
                and come from the same session (or from no session).

        Returns:
            A new lazy ``DataFrame``.

        Raises:
            InvalidArgumentError: If ``other`` is not a DataFrame, the two sides
                have different column counts, or they come from different
                sessions.
        """
        return self._union(other, distinct=False)

    def _union(self, other: DataFrame, distinct: bool) -> DataFrame:
        if not isinstance(other, DataFrame):
            raise InvalidArgumentError(
                f"union expects a DataFrame, got {type(other).__name__}"
            )
        session = self._merged_session(other, "union")
        execution_options = self._merged_execution_options(other, "union")
        left_schema = self._resolvable_schema()
        right_schema = other._resolvable_schema()
        if (
            left_schema is not None
            and right_schema is not None
            and len(left_schema.fields) != len(right_schema.fields)
        ):
            raise InvalidArgumentError(
                f"union needs matching column counts, but the left side has "
                f"{len(left_schema.fields)} column(s) {tuple(left_schema.names)} "
                f"and the right has {len(right_schema.fields)} "
                f"{tuple(right_schema.names)}"
            )
        plan = _plan.Union(self._plan, other._plan, distinct)
        return DataFrame(
            plan,
            session,
            schema=left_schema,
            execution_options=execution_options,
        )

    # -- array expansion ----------------------------------------------------

    def explode(self, column: str, *, outer: bool = False) -> DataFrame:
        """Expand an array column into one row per element.

        Replaces the array column in place (same name, same position); the other
        columns are repeated for each element. Compiles to ``unnest`` in the
        SELECT list, e.g. ``SELECT "id", unnest("tags") AS "tags" FROM ...``.

        Note:
            Hologres does not support ``LATERAL``, so the ``CROSS JOIN LATERAL
            unnest(...)`` form is unavailable
            here -- including the implicit-lateral ``FROM t, unnest(t.arr)``
            shorthand. A set-returning function in the SELECT list is the one
            spelling the engine accepts.

        Warning:
            With the default ``outer=False``, a row whose array is empty or NULL
            disappears entirely. Pass ``outer=True`` to keep such rows with a
            NULL in the expanded column (Spark's ``explode`` vs
            ``explode_outer``).

        Args:
            column: Name of the array column to expand.
            outer: Keep rows with an empty/NULL array (as one NULL row) instead
                of dropping them.

        Returns:
            A new lazy ``DataFrame`` where ``column`` holds element values.

        Raises:
            InvalidArgumentError: If this DataFrame has no pinned schema, the
                column is unknown, or the column is not an array type.

        Example:
            >>> from tests._fake_conn import make_session, FakeColumnInfo
            >>> info = [FakeColumnInfo(n, 25) for n in
            ...         ("column_name", "data_type", "udt_name")]
            >>> rows = [("id", "bigint", "int8"), ("tags", "ARRAY", "_text")]
            >>> session, _ = make_session(script=[(rows, info)])
            >>> t = session.table("public.docs")
            >>> t.explode("tags")._to_sql().as_string()
            'SELECT "id", unnest("tags") AS "tags" FROM "public"."docs"'
        """
        schema = self._require_pinned_schema("explode")
        if column not in schema.names:
            raise InvalidArgumentError(
                f"column {column!r} is not in schema {tuple(schema.names)}"
            )
        datatype = schema[column].datatype
        if not isinstance(datatype, ArrayType):
            raise InvalidArgumentError(
                f"explode needs an array column, but {column!r} is "
                f"{datatype!r}; for a jsonb array use "
                'hg.expr("jsonb_array_elements(...)")'
            )

        source = col(column)
        if outer:
            # An empty or NULL array yields no rows, which would drop the whole
            # row; substituting a one-element NULL array keeps it.
            empty = lit("{}").cast(datatype)
            one_null = lit("{NULL}").cast(datatype)
            source = function("coalesce", function("nullif", source, empty), one_null)
        exploded = function("unnest", source).alias(column)

        projection = [
            exploded if field.name == column else col(field.name)
            for field in schema.fields
        ]
        element_type = datatype.element_type
        nullable = True if outer else schema[column].nullable
        new_fields = [
            StructField(f.name, element_type, nullable) if f.name == column else f
            for f in schema.fields
        ]
        return DataFrame(
            _plan.Project(self._plan, projection),
            self._session,
            schema=StructType(new_fields),
            execution_options=self._execution_options,
        )

    def serverless(
        self,
        *,
        priority: int | None = None,
        required_cores: int | None = None,
        max_cores: int | None = None,
    ) -> DataFrame:
        """Request Serverless Computing for this DataFrame's actions.

        The marker is lazy and immutable: it does not contact Hologres until an
        action runs, and the original DataFrame remains unchanged. Hologres
        still decides whether a particular SQL statement is eligible for
        Serverless execution.

        Args:
            priority: Query priority from 1 (lowest) to 5 (highest). ``None``
                keeps the server-side default.
            required_cores: Explicit requested cores from 0 to 102400. ``0``
                asks Hologres to estimate resources automatically.
            max_cores: Per-query core limit from 0 to 102400. ``0`` disables
                this query-level cap; the instance quota still applies.

        Returns:
            A new DataFrame carrying the Serverless execution settings.

        Example:
            >>> from hologres.table_name import TableName
            >>> from hologres.dataframe import _plan
            >>> df = DataFrame(_plan.TableScan(TableName.valueOf("orders")))
            >>> out = df.serverless(priority=4, required_cores=64, max_cores=128)
            >>> out is df
            False
            >>> out._to_sql().as_string() == df._to_sql().as_string()
            True
        """
        from ._execution import ServerlessOptions

        options = ServerlessOptions.create(
            priority=priority,
            required_cores=required_cores,
            max_cores=max_cores,
        )
        result = DataFrame(
            self._plan,
            self._session,
            schema=self._schema,
            execution_options=options,
        )
        result._cached_schema = self._cached_schema
        return result

    # -- actions (execute against Hologres) --------------------------------

    def as_dynamic_table(
        self,
        name: str,
        *,
        freshness: str,
        refresh_mode: str = "auto",
        auto_refresh: bool = True,
        mode: str = "ignore",
        options: dict[str, Any] | None = None,
    ) -> None:
        """Materialize this query as a Hologres Dynamic Table.

        This is an immediate DDL action. ``mode="ignore"`` (the default) uses
        ``IF NOT EXISTS``; ``mode="errorifexists"`` lets Hologres reject an
        existing target; ``mode="replace"`` drops and recreates the target in
        one transaction, so a failed create rolls back the drop.

        Args:
            name: Target table name, optionally schema-qualified.
            freshness: Target data freshness, for example ``"10 minutes"``.
            refresh_mode: ``"auto"``, ``"full"``, or ``"incremental"``.
            auto_refresh: Whether Hologres refreshes the table automatically.
            mode: Existing-target behavior: ``"ignore"``,
                ``"errorifexists"``, or ``"replace"``.
            options: Additional Dynamic Table ``WITH`` options. ``cdc_format``
                is accepted as an alias for ``base_table_cdc_format``.

        Returns:
            ``None``. Query the created object with ``session.table(name)``.

        Example:
            >>> result = session.table("public.orders").select(  # doctest: +SKIP
            ...     "order_id", "region"
            ... )
            >>> result.as_dynamic_table(  # doctest: +SKIP
            ...     "public.dt_orders",
            ...     freshness="10 minutes",
            ...     refresh_mode="incremental",
            ...     options={"cdc_format": "binlog"},
            ... )
        """
        from ._dynamic_table import as_dynamic_table

        return as_dynamic_table(
            self,
            name,
            freshness=freshness,
            refresh_mode=refresh_mode,
            auto_refresh=auto_refresh,
            mode=mode,
            options=options,
        )

    def write(
        self,
        table: str,
        *,
        overwrite: bool = False,
        partition: dict[str, Any] | None = None,
        on_conflict: tuple[str, list[str]] | None = None,
        allow_binlog_gap: bool = False,
    ) -> None:
        """Write this DataFrame into an existing Hologres table.

        Appends use ``INSERT INTO``. Overwrites use native ``INSERT OVERWRITE``
        on Hologres 3.1+ and a transactional delete/truncate plus insert fallback
        on older servers. The operation executes immediately and returns ``None``.

        ``allow_binlog_gap=True`` is required when an overwrite targets a table
        whose downstream CDC consumers cannot receive a complete row-level change
        stream from the overwrite operation.
        """
        from ._write import write_dataframe

        return write_dataframe(
            self,
            table,
            overwrite=overwrite,
            partition=partition,
            on_conflict=on_conflict,
            allow_binlog_gap=allow_binlog_gap,
        )

    def write_files(
        self,
        path: str,
        *,
        format: str = "csv",
        oss_endpoint: str | None = None,
        role_arn: str | None = None,
        single_file: bool = False,
        target_file_size_mb: int = 10,
    ) -> None:
        """Write this DataFrame to an OSS path through ``EXTERNAL_FILES``.

        Compiles to ``INSERT INTO external_files(...) SELECT ...``. Only CSV
        output is supported by Hologres. The query is executed immediately and
        the method returns ``None``.

        Args:
            path: Destination ``oss://`` URI. A directory-like path lets
                Hologres create one or more output files; with
                ``single_file=True`` this may name the output file directly.
            format: Output format. Hologres currently supports only ``"csv"``.
            oss_endpoint: OSS endpoint visible to Hologres, for example
                ``"oss-cn-hangzhou-internal.aliyuncs.com"``.
            role_arn: RAM role that Hologres assumes to access the OSS bucket.
                Omit it when the bucket is accessible through the server's
                existing credentials or does not require authentication.
            single_file: Ask Hologres to produce one file. When true,
                ``target_file_size_mb`` is not sent to the server.
            target_file_size_mb: Approximate split size for multi-file output.
                Must be a positive integer.

        Returns:
            ``None``.

        Raises:
            InvalidArgumentError: If this DataFrame has no session, the path is
                not an OSS URI, or an option has an invalid type or value.
            UnsupportedOperationError: If ``format`` is not ``"csv"``.
            ExecutionError: If Hologres rejects or fails the export.

        Example:
            >>> orders = session.table("public.orders")  # doctest: +SKIP
            >>> orders.write_files(  # doctest: +SKIP
            ...     "oss://bucket/export/",
            ...     oss_endpoint="oss-cn-hangzhou-internal.aliyuncs.com",
            ...     role_arn="acs:ram::123:role/holo-role",
            ...     target_file_size_mb=100,
            ... )
        """
        if self._session is None:
            raise InvalidArgumentError(
                "this DataFrame has no session; obtain it from "
                "session.table() or session.sql() first"
            )
        from ._external_files import build_write_files_sql, validate_oss_write

        normalized_path, normalized_format, options = validate_oss_write(
            path,
            format,
            oss_endpoint=oss_endpoint,
            role_arn=role_arn,
            single_file=single_file,
            target_file_size_mb=target_file_size_mb,
        )
        require_capability = getattr(self._session, "_require_capability", None)
        if require_capability is not None:
            require_capability("external_files", operation="write_files")
        query = build_write_files_sql(
            self._prepare_sql(), normalized_path, normalized_format, options
        )
        self._run_sql(query, fetch=False)
        return None

    def _run_sql(self, query, *, fetch=True):
        """Execute SQL, passing private options only when they are present."""
        if self._execution_options is None:
            return self._session.execute(query, fetch=fetch)
        return self._session.execute(
            query,
            fetch=fetch,
            _execution_options=self._execution_options,
        )

    def _run_sql_stream(self, query, *, batch_size):
        """Stream SQL, preserving compatibility with Session-shaped objects."""
        if self._execution_options is None:
            return self._session.execute_stream(query, batch_size=batch_size)
        return self._session.execute_stream(
            query,
            batch_size=batch_size,
            _execution_options=self._execution_options,
        )

    def _execute(self, fetch: bool = True):
        """Internal: dispatch this DataFrame's compiled SQL to the session."""
        if self._session is None:
            raise InvalidArgumentError(
                "this DataFrame has no session; obtain it from "
                "session.table() or session.sql() first"
            )
        return self._run_sql(self._prepare_sql(), fetch=fetch)

    def collect(self) -> list[Row]:
        """Execute the query and return all rows as a list.

        Materializes the entire result in memory. For large results prefer
        :meth:`iter_rows` (streaming via a server-side cursor).

        Returns:
            A list of :class:`~hologres.dataframe.row.Row`, in query order.

        Raises:
            InvalidArgumentError: If this DataFrame has no session.
            ExecutionError: If Hologres rejects or fails the query.

        Example:
            >>> session.table("public.orders").limit(3).collect()  # doctest: +SKIP
            [Row(...), Row(...), Row(...)]
        """
        rows, _ = self._execute()
        return rows

    def show(self, n: int = 10, truncate: int = 20) -> None:
        """Print the first ``n`` rows to stdout in a table format.

        This is a debugging helper; it does not return the data. See
        :meth:`collect` or :meth:`to_pandas` for programmatic access.

        Args:
            n: Number of rows to show. Defaults to 10.
            truncate: Maximum characters per cell value; longer strings are
                cut off with ``...``. Pass ``0`` to disable truncation.

        Raises:
            InvalidArgumentError: If this DataFrame has no session, or
                ``n``/``truncate`` are negative.
            ExecutionError: If Hologres rejects the query.

        Example:
            >>> session.table("public.orders").show(3)  # doctest: +SKIP
        """
        if not isinstance(n, int) or isinstance(n, bool) or n < 0:
            raise InvalidArgumentError(f"n must be a non-negative int, got {n!r}")
        if not isinstance(truncate, int) or isinstance(truncate, bool) or truncate < 0:
            raise InvalidArgumentError(
                f"truncate must be a non-negative int, got {truncate!r}"
            )

        rows, schema = self.limit(n)._execute()
        if not rows:
            print("(0 rows)")
            return
        names = list(schema.names) if schema.fields else list(rows[0].fields)

        def fmt(v):
            s = "NULL" if v is None else str(v)
            if truncate and len(s) > truncate:
                s = s[: max(truncate - 3, 0)] + "..."
            return s

        widths = [len(name) for name in names]
        rendered_rows = []
        for r in rows:
            values = [fmt(v) for v in r.values]
            for i, cell in enumerate(values):
                if i < len(widths):
                    widths[i] = max(widths[i], len(cell))
            rendered_rows.append(values)

        sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
        header = "|" + "|".join(f" {n:<{w}} " for n, w in zip(names, widths)) + "|"
        print(sep)
        print(header)
        print(sep)
        for values in rendered_rows:
            padded = values + [""] * (len(widths) - len(values))
            print("|" + "|".join(f" {v:<{w}} " for v, w in zip(padded, widths)) + "|")
        print(sep)
        print(f"({len(rendered_rows)} row{'' if len(rendered_rows) == 1 else 's'})")

    def count(self) -> int:
        """Return the number of rows this query would produce.

        Compiles to ``SELECT COUNT(*) FROM (<self>) __count__`` -- a single
        integer round-trip that does not materialize any data rows client-side.

        Returns:
            The row count as an ``int``.

        Raises:
            InvalidArgumentError: If this DataFrame has no session.
            ExecutionError: If Hologres rejects the query.

        Example:
            >>> df = session.table("public.orders")  # doctest: +SKIP
            >>> df.filter(hg.col("amount") > 100).count()  # doctest: +SKIP
            42
        """
        if self._session is None:
            raise InvalidArgumentError(
                "this DataFrame has no session; obtain it from "
                "session.table() or session.sql() first"
            )
        query = sql.SQL("SELECT COUNT(*) FROM ({}) __count__").format(
            self._prepare_sql()
        )
        rows, _ = self._run_sql(query)
        return int(rows[0][0])

    def first(self) -> Row | None:
        """Return the first row of the result, or ``None`` if empty.

        Executes ``LIMIT 1``. Result order is defined only if a ``sort`` /
        ``order_by`` is in effect.

        Returns:
            The first :class:`~hologres.dataframe.row.Row`, or ``None``.

        Example:
            >>> session.table("public.orders").sort("amount").first()  # doctest: +SKIP
        """
        rows = self.limit(1).collect()
        return rows[0] if rows else None

    def take(self, n: int) -> list[Row]:
        """Return the first ``n`` rows.

        Equivalent to ``limit(n).collect()``. ``n`` is a count, not a positional
        index (the argument is a count, not a positional index).

        Args:
            n: Number of rows to fetch; must be a non-negative int.

        Returns:
            A list of up to ``n`` rows.

        Raises:
            ValueError: If ``n`` is not a non-negative int.

        Example:
            >>> session.table("public.orders").take(5)  # doctest: +SKIP
        """
        return self.limit(n).collect()

    def to_pandas(self):
        """Return the result as a ``pandas.DataFrame``.

        Materializes everything into memory (via :meth:`collect`) and constructs
        a pandas DataFrame using this DataFrame's :attr:`schema` for column
        names.

        Returns:
            A ``pandas.DataFrame``.

        Raises:
            ImportError: If the required ``pandas`` dependency is missing from
                the environment.
            InvalidArgumentError: If this DataFrame has no session.
            ExecutionError: If Hologres rejects the query.

        Example:
            >>> session.table("public.orders").to_pandas()  # doctest: +SKIP
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for to_pandas(); reinstall or upgrade "
                "hologres-dataframe to restore its required dependencies"
            ) from None
        rows, schema = self._execute()
        names = (
            list(schema.names)
            if schema.fields
            else (list(rows[0].fields) if rows else [])
        )
        return pd.DataFrame([r.values for r in rows], columns=names)

    def iter_rows(self, *, batch_size: int = 1000):
        """Iterate rows in batches instead of materializing the whole result.

        Prefer this over :meth:`collect` for large result sets: only one batch is
        held in Python at a time. The connection is returned to the pool when the
        iterator is exhausted or garbage-collected.

        Args:
            batch_size: Rows to fetch per round trip.

        Yields:
            :class:`~hologres.dataframe.row.Row` instances, one per result row.

        Raises:
            InvalidArgumentError: If this DataFrame has no session, or
                ``batch_size`` is not a positive int.
            ExecutionError: If Hologres rejects the query.

        Example:
            >>> for row in session.table("public.orders").iter_rows():  # doctest: +SKIP
            ...     process(row)
        """
        if self._session is None:
            raise InvalidArgumentError(
                "this DataFrame has no session; obtain it from "
                "session.table() or session.sql() first"
            )
        return self._run_sql_stream(self._prepare_sql(), batch_size=batch_size)


def _derive_project_schema(parent, columns):
    """Compute the derived schema after a projection.

    Enforces the Ibis-style "analysis before action" contract: unknown column
    names and unnamed derived expressions are rejected at build time so
    downstream schema-based operators (drop/rename/fillna, ``columns``) never
    disagree with the SQL that will actually run.
    """
    from .types import StringType, StructField, StructType

    fields = []
    for column in columns:
        expr = column._expr
        alias = column._alias
        if isinstance(expr, _expr.ColumnRef):
            source = expr.name
            if source not in parent.names:
                raise InvalidArgumentError(
                    f"column {source!r} is not in schema {tuple(parent.names)}"
                )
            source_field = parent[source]
            out_name = alias if alias is not None else source
            fields.append(
                StructField(out_name, source_field.datatype, source_field.nullable)
            )
        elif isinstance(expr, _expr.RemoteFunctionCall):
            out_name = alias if alias is not None else expr.output_name
            fields.append(
                StructField(out_name, expr.definition.return_type, nullable=True)
            )
        else:
            # Derived expression: an alias is required so that df.columns and
            # the runtime column list match. Type inference for arbitrary
            # expressions is out of scope; use a placeholder type.
            if alias is None:
                raise InvalidArgumentError(
                    "derived expressions in select() must be given a name via "
                    ".alias(...)"
                )
            fields.append(StructField(alias, StringType()))
    return StructType(fields)


def _extend_schema(parent, new_names, columns=None):
    """Return ``parent`` extended with derived expression types when known."""
    from .types import StringType, StructField, StructType

    columns = columns or [None] * len(new_names)
    fields = list(parent.fields)
    for name, column in zip(new_names, columns):
        datatype = StringType()
        if column is not None and isinstance(column._expr, _expr.RemoteFunctionCall):
            datatype = column._expr.definition.return_type
        fields.append(StructField(name, datatype))
    return StructType(fields)


def _is_float_type(datatype) -> bool:
    """Whether ``datatype`` is a floating-point kind that can hold NaN.

    Used by ``dropna`` and ``fillna`` to decide whether a NaN comparison is
    legal or should be skipped in favor of a plain NULL check.
    """
    return isinstance(datatype, (FloatType, DoubleType, DecimalType))


# Accepted spellings of the join types, mapped to the canonical name. The
# ``*_outer`` spellings are accepted for compatibility with common DataFrame APIs.
_JOIN_TYPE_ALIASES = {
    "inner": "inner",
    "left": "left",
    "left_outer": "left",
    "leftouter": "left",
    "right": "right",
    "right_outer": "right",
    "rightouter": "right",
    "full": "full",
    "outer": "full",
    "full_outer": "full",
    "fullouter": "full",
    "cross": "cross",
}


def _normalize_join_type(how) -> str:
    """Resolve a user-supplied ``how`` to a canonical join type."""
    if not isinstance(how, str):
        raise InvalidArgumentError(f"how must be a string, got {type(how).__name__}")
    key = how.strip().lower().replace(" ", "_")
    if key not in _JOIN_TYPE_ALIASES:
        raise InvalidArgumentError(
            f"unknown join type {how!r}; expected one of "
            f"{tuple(sorted(set(_JOIN_TYPE_ALIASES.values())))}"
        )
    return _JOIN_TYPE_ALIASES[key]


def _parse_join_condition(on, how: str) -> tuple[list[str] | None, Column | None]:
    """Split the ``on`` argument into ``(using_columns, predicate)``."""
    if how == "cross":
        if on is not None:
            raise InvalidArgumentError("a cross join takes no on= condition")
        return None, None
    if on is None:
        raise InvalidArgumentError(
            f"a {how} join requires on=<column name(s)> or on=<predicate>; "
            "use how='cross' for an unconditional join"
        )
    if isinstance(on, Column):
        return None, on
    if isinstance(on, str):
        return [on], None
    if isinstance(on, (list, tuple)):
        if not on:
            raise InvalidArgumentError("on= is an empty list, so there is no condition")
        for name in on:
            if not isinstance(name, str):
                raise InvalidArgumentError(
                    "on= as a list must hold column names (str), got "
                    f"{type(name).__name__}"
                )
        return list(on), None
    raise InvalidArgumentError(
        "on must be a column name, a list of column names, or a Column "
        f"predicate, got {type(on).__name__}"
    )


def _join_output(
    left_plan,
    left_schema,
    right_plan,
    right_schema,
    using: list[str] | None,
    lsuffix: str | None,
    rsuffix: str | None,
):
    """Derive a join's output schema and, if suffixes apply, its projection.

    Returns ``(schema, projection)``. ``projection`` is ``None`` whenever
    ``SELECT *`` already produces unique column names; otherwise it is the
    explicit, suffixed column list to place above the join. ``schema`` is
    ``None`` when a side's columns are unknown, leaving resolution to a later
    describe.
    """
    for label, suffix in (("lsuffix", lsuffix), ("rsuffix", rsuffix)):
        if suffix is not None and not (isinstance(suffix, str) and suffix):
            raise InvalidArgumentError(f"{label} must be a non-empty string")

    if left_schema is None or right_schema is None:
        if lsuffix is not None or rsuffix is not None:
            raise InvalidArgumentError(
                "lsuffix/rsuffix need the column list of both sides, and one "
                "side's schema is unknown; select the columns explicitly instead"
            )
        return None, None

    using_names = list(using or ())
    for name in using_names:
        if name not in left_schema.names or name not in right_schema.names:
            raise InvalidArgumentError(
                f"join column {name!r} must exist on both sides; left has "
                f"{tuple(left_schema.names)}, right has {tuple(right_schema.names)}"
            )

    using_set = set(using_names)
    conflicts = (set(left_schema.names) & set(right_schema.names)) - using_set
    if conflicts and lsuffix is None and rsuffix is None:
        raise InvalidArgumentError(
            f"the join output would repeat the column name(s) "
            f"{tuple(sorted(conflicts))}; pass lsuffix=/rsuffix= to disambiguate, "
            "join on those columns so they merge, or select() explicitly"
        )

    fields = []
    # A projection is only needed to apply suffixes; SELECT * is fine otherwise.
    projection: list[Column] | None = [] if conflicts else None

    # PostgreSQL puts the merged USING columns first, then the remaining left
    # columns, then the right ones -- mirror that order.
    for name in using_names:
        fields.append(left_schema[name])
        if projection is not None:
            # A merged USING column must stay unqualified: with an outer join
            # either side can be the NULL-padded one.
            projection.append(col(name))

    for schema, plan, suffix in (
        (left_schema, left_plan, lsuffix),
        (right_schema, right_plan, rsuffix),
    ):
        for field in schema.fields:
            if field.name in using_set:
                continue
            renamed = field.name in conflicts and suffix is not None
            out_name = field.name + suffix if renamed else field.name
            fields.append(StructField(out_name, field.datatype, field.nullable))
            if projection is not None:
                ref = Column(_expr.ColumnRef(field.name, relation=plan))
                projection.append(ref.alias(out_name) if renamed else ref)

    return StructType(fields), projection
