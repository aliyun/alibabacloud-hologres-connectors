"""The :class:`Column` -- a symbolic column expression.

A ``Column`` does not hold data. It is a symbol that Python operators build into
an expression tree, which is later compiled to SQL and pushed down to Hologres.
Get one from :func:`hologres.dataframe.functions.col` or
:func:`~hologres.dataframe.functions.lit`, or by operating on existing columns.

    import hologres.dataframe as hg

    price = hg.col("amount")
    with_tax = price * 1.06            # amount * 1.06
    big_east = (price > 100) & (hg.col("region") == "east")

Rules worth remembering (they match common Python DataFrame APIs):

* Combine predicates with ``& | ~``, never ``and``/``or``/``not`` -- the latter
  are Python keywords that cannot be overloaded. Parenthesize each side:
  ``(col("a") > 1) & (col("b") < 2)``.
* ``==`` builds a *predicate*, not a bool. ``col("a") == col("b")`` is a
  ``Column`` you pass to ``filter``; it never tells you whether two columns are
  the same object.
* A bare value on either side of an operator is auto-promoted to a literal:
  ``col("x") + 1`` and ``1 + col("x")`` both work, no ``lit`` needed.
* Division follows PostgreSQL: integer / integer truncates
  to an integer (``col("a") / col("b")`` on two int columns floors the result).
  For a fractional result, cast a side first, e.g.
  ``col("a").cast(hg.DoubleType()) / col("b")``.
* ``col("x") == None`` renders ``("x" = NULL)``, which never matches (same as
  regular Python booleans). Use ``col("x").is_null()`` / ``is_not_null()`` to test for
  NULL.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from psycopg import sql

from . import _expr
from .exceptions import InvalidArgumentError

if TYPE_CHECKING:
    from .types import DataType

__all__ = ["Column"]


def _to_expression(value: Any) -> _expr.Expression:
    """Coerce a value into an Expression, promoting bare values to literals."""
    if isinstance(value, Column):
        return value._expr
    return _expr.Literal(value)


class Column:
    """A lazy column expression that compiles to SQL.

    You rarely construct this directly; use :func:`hologres.dataframe.col` and
    :func:`hologres.dataframe.lit`, then combine with operators and the methods
    below. Every operation returns a **new** ``Column`` (they are immutable and
    chainable).

    Because ``==`` and the other comparison operators are overloaded to build
    predicates, ``Column`` is intentionally **not hashable** and cannot be used
    in a set or as a dict key.

    Example:
        >>> import hologres.dataframe as hg
        >>> (hg.col("amount") * 0.06).alias("tax")._to_projection_sql().as_string()
        '("amount" * 0.06) AS "tax"'
        >>> pred = (hg.col("amount") > 100) & (hg.col("region") == "east")
        >>> pred._to_sql().as_string()
        '(("amount" > 100) AND ("region" = \\'east\\'))'
    """

    __slots__ = ("_expr", "_alias")

    def __init__(self, expression: _expr.Expression, alias: str | None = None) -> None:
        self._expr = expression
        self._alias = alias

    # -- rendering contexts (used by the compiler in later commits) --------

    def _to_sql(self) -> sql.Composable:
        """Render the bare expression (for WHERE/ON/derived-column contexts)."""
        return self._expr.to_sql()

    def _to_projection_sql(self) -> sql.Composable:
        """Render for a SELECT list: ``expr AS alias`` when an alias is set.

        With no alias the bare expression is emitted (a plain column keeps its
        own name; a derived expression stays unnamed).
        """
        if self._alias is not None:
            return sql.SQL("{expr} AS {alias}").format(
                expr=self._expr.to_sql(),
                alias=sql.Identifier(self._alias),
            )
        return self._expr.to_sql()

    def _to_order_sql(self, default_ascending: bool = True) -> sql.Composable:
        """Render as an ``ORDER BY`` item.

        If this column already carries sort metadata (from ``asc``/``desc``),
        that wins; otherwise ``default_ascending`` decides the direction.
        """
        if isinstance(self._expr, _expr.SortExpr):
            return self._expr.to_sql()
        return _expr.SortExpr(self._expr, ascending=default_ascending).to_sql()

    def _bind_relations(
        self,
        mapping: dict[int, str],
        ambiguous: set[int],
        *,
        outputs: dict[tuple[int, str], tuple[str, str] | None] | None = None,
        passthrough: dict[int, str] | None = None,
        sealed: set[int] | None = None,
    ) -> Column:
        """Resolve relation-bound references to join aliases, keeping the alias.

        Called by the compiler right before rendering, once a join has assigned
        an alias to each of its inputs. Returns ``self`` unchanged when the tree
        holds no reference that ``mapping`` covers.
        """
        if not any((mapping, ambiguous, outputs, passthrough, sealed)):
            return self
        bound = _expr.rewrite_relations(
            self._expr,
            mapping,
            ambiguous,
            outputs=outputs,
            passthrough=passthrough,
            sealed=sealed or set(),
        )
        if bound is self._expr:
            return self
        return Column(bound, self._alias)

    @property
    def output_name(self) -> str | None:
        """The result column name: the alias if set, else the underlying name.

        Returns ``None`` for an unnamed derived expression with no alias.

        Example:
            >>> import hologres.dataframe as hg
            >>> hg.col("amount").output_name
            'amount'
            >>> (hg.col("a") + 1).alias("s").output_name
            's'
            >>> (hg.col("a") + 1).output_name is None
            True
        """
        if self._alias is not None:
            return self._alias
        return self._expr.output_name

    # -- naming -------------------------------------------------------------

    def alias(self, name: str) -> Column:
        """Give the column a name in the result set (``expr AS name``).

        Args:
            name: The output column name.

        Returns:
            A new ``Column`` carrying the alias.

        Raises:
            InvalidArgumentError: If ``name`` is not a non-empty string.

        Example:
            >>> import hologres.dataframe as hg
            >>> hg.col("amount").alias("amt")._to_projection_sql().as_string()
            '"amount" AS "amt"'
        """
        if not isinstance(name, str) or not name:
            raise InvalidArgumentError("alias name must be a non-empty string")
        self._reject_sort_context("alias()")
        return Column(self._expr, alias=name)

    # -- sorting ------------------------------------------------------------

    def asc(self) -> Column:
        """Sort by this column ascending. See :meth:`asc_nulls_last` for nulls.

        Example:
            >>> import hologres.dataframe as hg
            >>> hg.col("a").asc()._to_order_sql().as_string()
            '"a" ASC'
        """
        return self._with_sort(ascending=True, nulls=None)

    def desc(self) -> Column:
        """Sort by this column descending.

        Example:
            >>> import hologres.dataframe as hg
            >>> hg.col("amount").desc()._to_order_sql().as_string()
            '"amount" DESC'
        """
        return self._with_sort(ascending=False, nulls=None)

    def asc_nulls_first(self) -> Column:
        """Ascending, with NULLs sorted before non-NULLs."""
        return self._with_sort(ascending=True, nulls="first")

    def asc_nulls_last(self) -> Column:
        """Ascending, with NULLs sorted after non-NULLs."""
        return self._with_sort(ascending=True, nulls="last")

    def desc_nulls_first(self) -> Column:
        """Descending, with NULLs sorted before non-NULLs."""
        return self._with_sort(ascending=False, nulls="first")

    def desc_nulls_last(self) -> Column:
        """Descending, with NULLs sorted after non-NULLs."""
        return self._with_sort(ascending=False, nulls="last")

    def _with_sort(self, ascending: bool, nulls: str | None) -> Column:
        if isinstance(self._expr, _expr.SortExpr):
            base = self._expr.child
        else:
            base = self._expr
        return Column(_expr.SortExpr(base, ascending=ascending, nulls=nulls))

    # -- null tests ---------------------------------------------------------

    def is_null(self) -> Column:
        """Build the predicate ``expr IS NULL``.

        Example:
            >>> import hologres.dataframe as hg
            >>> hg.col("a").is_null()._to_sql().as_string()
            '("a" IS NULL)'
        """
        self._reject_sort_context("is_null()")
        return Column(_expr.UnaryExpr("IS NULL", self._expr, postfix=True))

    def is_not_null(self) -> Column:
        """Build the predicate ``expr IS NOT NULL``.

        Example:
            >>> import hologres.dataframe as hg
            >>> hg.col("a").is_not_null()._to_sql().as_string()
            '("a" IS NOT NULL)'
        """
        self._reject_sort_context("is_not_null()")
        return Column(_expr.UnaryExpr("IS NOT NULL", self._expr, postfix=True))

    # -- cast ---------------------------------------------------------------

    def cast(self, datatype: DataType) -> Column:
        """Cast the expression to another type (``(expr)::type``).

        Useful, for instance, to do arithmetic on a :class:`ByteType` column,
        which stores as PostgreSQL ``"char"``: ``col("b").cast(hg.IntegerType())``.

        Args:
            datatype: The target :class:`~hologres.dataframe.types.DataType`.

        Returns:
            A new ``Column`` wrapping the cast.

        Raises:
            InvalidArgumentError: If ``datatype`` is not a ``DataType``.

        Example:
            >>> import hologres.dataframe as hg
            >>> hg.col("b").cast(hg.IntegerType())._to_sql().as_string()
            '("b")::integer'
            >>> hg.col("x").cast(hg.DecimalType(10, 2))._to_sql().as_string()
            '("x")::numeric(10,2)'
        """
        from .types import DataType

        if not isinstance(datatype, DataType):
            raise InvalidArgumentError(
                f"cast() requires a DataType, got {type(datatype).__name__}"
            )
        self._reject_sort_context("cast()")
        return Column(_expr.CastExpr(self._expr, datatype.sql_name))

    # -- predicates ---------------------------------------------------------

    def isin(self, *values: Any) -> Column:
        """Build ``expr IN (...)`` from the given values.

        Args:
            *values: The candidate values. Pass them individually
                (``isin(1, 2, 3)``), splat a collection (``isin(*ids)``), or
                pass a single collection (``isin([1, 2, 3])``) -- a lone list,
                tuple, or set is unwrapped for you. Each value is emitted as a
                literal.

        Returns:
            A new predicate ``Column``. An empty value set compiles to the
            always-false ``FALSE``, matching SQL ``IN ()`` intent.

        Example:
            >>> import hologres.dataframe as hg
            >>> hg.col("region").isin("east", "west")._to_sql().as_string()
            '("region" IN (\\'east\\', \\'west\\'))'
            >>> hg.col("id").isin()._to_sql().as_string()
            'FALSE'
        """
        # Accept both isin(1, 2, 3) and isin([1, 2, 3]); unwrap a lone
        # collection so a single list arg is not mistaken for one array value.
        self._reject_sort_context("isin()")
        if len(values) == 1 and isinstance(values[0], (list, tuple, set, frozenset)):
            values = tuple(values[0])
        if not values:
            return Column(_expr.PredicateExpr(sql.SQL("FALSE"), []))
        # The template contains only library-controlled tokens ("{}", commas);
        # every user value goes through Literal, so this stays injection-safe.
        placeholders = ", ".join("{}" for _ in values)
        template = sql.SQL(f"({{}} IN ({placeholders}))")
        operands = [self._expr, *[_expr.Literal(v) for v in values]]
        return Column(_expr.PredicateExpr(template, operands))

    def like(self, pattern: str) -> Column:
        """Build a case-sensitive ``expr LIKE pattern`` predicate.

        Args:
            pattern: A SQL ``LIKE`` pattern, e.g. ``"a%"``. Emitted as a literal.

        Example:
            >>> import hologres.dataframe as hg
            >>> hg.col("name").like("a%")._to_sql().as_string()
            '("name" LIKE \\'a%\\')'
        """
        self._reject_sort_context("like()")
        return Column(
            _expr.PredicateExpr(
                sql.SQL("({} LIKE {})"), [self._expr, _expr.Literal(pattern)]
            )
        )

    def ilike(self, pattern: str) -> Column:
        """Build a case-insensitive ``expr ILIKE pattern`` predicate (PG extension).

        Args:
            pattern: A SQL ``ILIKE`` pattern. Emitted as a literal.

        Example:
            >>> import hologres.dataframe as hg
            >>> hg.col("name").ilike("A%")._to_sql().as_string()
            '("name" ILIKE \\'A%\\')'
        """
        self._reject_sort_context("ilike()")
        return Column(
            _expr.PredicateExpr(
                sql.SQL("({} ILIKE {})"), [self._expr, _expr.Literal(pattern)]
            )
        )

    def between(self, low: Any, high: Any) -> Column:
        """Build the inclusive predicate ``expr BETWEEN low AND high``.

        Args:
            low: Lower bound (inclusive). A ``Column`` or a bare value.
            high: Upper bound (inclusive). A ``Column`` or a bare value.

        Example:
            >>> import hologres.dataframe as hg
            >>> hg.col("amount").between(10, 100)._to_sql().as_string()
            '("amount" BETWEEN 10 AND 100)'
        """
        self._reject_sort_context("between()")
        return Column(
            _expr.PredicateExpr(
                sql.SQL("({} BETWEEN {} AND {})"),
                [self._expr, _to_expression(low), _to_expression(high)],
            )
        )

    # -- arithmetic operators ----------------------------------------------

    def _reject_sort_context(self, operation: str) -> None:
        """Guard against reusing an asc()/desc() column outside sort/order_by.

        A sort column carries direction metadata (``ASC``/``DESC``) that is only
        valid inside ``ORDER BY``; feeding it into another operation would emit
        invalid SQL like ``("a" ASC + 1)``.
        """
        if isinstance(self._expr, _expr.SortExpr):
            raise InvalidArgumentError(
                f"cannot use a sort column (from asc()/desc()) in {operation}; "
                "sort columns are only valid inside sort()/order_by()"
            )

    def _binary(self, operator: str, other: Any, reflected: bool = False) -> Column:
        self._reject_sort_context("an arithmetic/comparison/logical operation")
        left = _to_expression(other) if reflected else self._expr
        right = self._expr if reflected else _to_expression(other)
        return Column(_expr.BinaryExpr(left, operator, right))

    def __add__(self, other: Any) -> Column:
        return self._binary("+", other)

    def __radd__(self, other: Any) -> Column:
        return self._binary("+", other, reflected=True)

    def __sub__(self, other: Any) -> Column:
        return self._binary("-", other)

    def __rsub__(self, other: Any) -> Column:
        return self._binary("-", other, reflected=True)

    def __mul__(self, other: Any) -> Column:
        return self._binary("*", other)

    def __rmul__(self, other: Any) -> Column:
        return self._binary("*", other, reflected=True)

    def __truediv__(self, other: Any) -> Column:
        return self._binary("/", other)

    def __rtruediv__(self, other: Any) -> Column:
        return self._binary("/", other, reflected=True)

    def __mod__(self, other: Any) -> Column:
        return self._binary("%", other)

    def __rmod__(self, other: Any) -> Column:
        return self._binary("%", other, reflected=True)

    def __neg__(self) -> Column:
        self._reject_sort_context("negation")
        return Column(_expr.UnaryExpr("-", self._expr))

    # -- comparison operators ----------------------------------------------

    def __eq__(self, other: Any) -> Column:  # type: ignore[override]
        return self._binary("=", other)

    def __ne__(self, other: Any) -> Column:  # type: ignore[override]
        return self._binary("<>", other)

    def __gt__(self, other: Any) -> Column:
        return self._binary(">", other)

    def __ge__(self, other: Any) -> Column:
        return self._binary(">=", other)

    def __lt__(self, other: Any) -> Column:
        return self._binary("<", other)

    def __le__(self, other: Any) -> Column:
        return self._binary("<=", other)

    # Overloading __eq__ makes instances unhashable unless __hash__ is defined;
    # Leaving it None is intentional: expressions are not hashable.
    __hash__ = None  # type: ignore[assignment]

    # -- logical operators --------------------------------------------------

    def __and__(self, other: Any) -> Column:
        return self._binary("AND", other)

    def __rand__(self, other: Any) -> Column:
        return self._binary("AND", other, reflected=True)

    def __or__(self, other: Any) -> Column:
        return self._binary("OR", other)

    def __ror__(self, other: Any) -> Column:
        return self._binary("OR", other, reflected=True)

    def __invert__(self) -> Column:
        self._reject_sort_context("negation (~)")
        return Column(_expr.UnaryExpr("NOT", self._expr))

    def __bool__(self) -> bool:
        raise InvalidArgumentError(
            "Cannot convert a Column to bool. Combine predicates with the "
            "bitwise operators & | ~ (not the keywords and/or/not), and "
            "parenthesize each side, e.g. (col('a') > 1) & (col('b') < 2)."
        )

    def __repr__(self) -> str:
        rendered = self._to_projection_sql().as_string()
        return f"Column<{rendered}>"
