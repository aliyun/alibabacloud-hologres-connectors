"""Internal expression tree behind :class:`~hologres.dataframe.column.Column`.

A ``Column`` wraps one of these nodes. Nothing here is public API; users build
expressions through ``hg.col`` / ``hg.lit`` and Python operators, and the tree
is compiled to SQL only when a DataFrame action runs.

The tree is kept (rather than eagerly concatenating SQL strings) so that column
references can be qualified late -- e.g. a join can render ``a.user_id`` for a
``ColumnRef`` that was originally written as ``col("user_id")``.

Every node renders through :mod:`psycopg.sql`, so identifiers and literals are
always escaped and the output is injection-safe.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from psycopg import sql

from .exceptions import InvalidArgumentError

__all__ = [
    "BinaryExpr",
    "CastExpr",
    "ColumnRef",
    "Expression",
    "FunctionCall",
    "Literal",
    "PredicateExpr",
    "RawExpr",
    "RemoteFunctionCall",
    "SortExpr",
    "Star",
    "UnaryExpr",
    "WindowExpr",
    "rewrite_relations",
]


class Expression(ABC):
    """Base class for all expression-tree nodes.

    Subclasses render themselves to a :class:`psycopg.sql.Composable` and know
    the name their result would carry in a projection (``output_name``).
    """

    @abstractmethod
    def to_sql(self) -> sql.Composable:
        """Render this node as a ``psycopg.sql`` composable (no DB access)."""
        raise NotImplementedError  # pragma: no cover

    @property
    def output_name(self) -> str | None:
        """The result column name this node implies, or ``None`` if unnamed.

        Only a bare :class:`ColumnRef` has a natural name; derived expressions
        return ``None`` and must be given one via ``Column.alias``.
        """
        return None


class ColumnRef(Expression):
    """A reference to a column, optionally qualified by a relation alias.

    Args:
        name: The column name.
        qualifier: Optional relation/table alias, e.g. ``"a"`` so the reference
            renders as ``a.name``. Left unset, only ``name`` is emitted.
        relation: Optional identity token for the relation this column came from
            (``df["col"]`` passes the source DataFrame's plan node). It carries
            identity only and never renders: a join assigns aliases at compile
            time and :func:`rewrite_relations` turns the token into a
            ``qualifier``.

    Example:
        >>> ColumnRef("amount").to_sql().as_string()
        '"amount"'
        >>> ColumnRef("user_id", "a").to_sql().as_string()
        '"a"."user_id"'
    """

    __slots__ = ("name", "qualifier", "relation")

    def __init__(
        self,
        name: str,
        qualifier: str | None = None,
        relation: object | None = None,
    ) -> None:
        self.name = name
        self.qualifier = qualifier
        self.relation = relation

    def to_sql(self) -> sql.Composable:
        if self.qualifier is not None:
            return sql.Identifier(self.qualifier, self.name)
        return sql.Identifier(self.name)

    @property
    def output_name(self) -> str | None:
        return self.name


class Literal(Expression):
    """A constant value, rendered as a SQL literal.

    ``None`` becomes ``NULL``, ``bool`` becomes ``true``/``false``, strings are
    quoted and escaped, and lists become PostgreSQL array literals -- all via
    ``psycopg.sql.Literal``.

    Args:
        value: The Python value to embed.

    Example:
        >>> Literal(100).to_sql().as_string()
        '100'
        >>> Literal("a'b").to_sql().as_string()
        "'a''b'"
        >>> Literal(None).to_sql().as_string()
        'NULL'
    """

    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value

    def to_sql(self) -> sql.Composable:
        return sql.Literal(self.value)


class Star(Expression):
    """The ``*`` wildcard, used to pass through all columns in a projection.

    Emitted by ``with_column``/``with_columns`` so a derived column can be added
    without dropping the existing ones (``SELECT *, expr AS name``).

    Example:
        >>> Star().to_sql().as_string()
        '*'
    """

    __slots__ = ()

    def to_sql(self) -> sql.Composable:
        return sql.SQL("*")


class BinaryExpr(Expression):
    """An infix binary operation such as ``a + b`` or ``a = b``.

    The result is always wrapped in parentheses so precedence never surprises.

    Args:
        left: Left operand.
        operator: The raw SQL operator token, e.g. ``"+"``, ``"="``, ``"AND"``.
        right: Right operand.

    Example:
        >>> BinaryExpr(ColumnRef("a"), "+", Literal(1)).to_sql().as_string()
        '("a" + 1)'
    """

    __slots__ = ("left", "operator", "right")

    def __init__(self, left: Expression, operator: str, right: Expression) -> None:
        self.left = left
        self.operator = operator
        self.right = right

    def to_sql(self) -> sql.Composable:
        return sql.SQL("({left} {op} {right})").format(
            left=self.left.to_sql(),
            op=sql.SQL(self.operator),
            right=self.right.to_sql(),
        )


class UnaryExpr(Expression):
    """A unary operation, either prefix (``NOT x``, ``-x``) or postfix.

    Args:
        operator: The raw SQL operator token, e.g. ``"NOT"``, ``"-"``,
            ``"IS NULL"``.
        operand: The single operand.
        postfix: If ``True``, the operator follows the operand (used for
            ``IS NULL`` / ``IS NOT NULL``); otherwise it precedes it.

    Example:
        >>> UnaryExpr("NOT", ColumnRef("flag")).to_sql().as_string()
        '(NOT "flag")'
        >>> UnaryExpr("IS NULL", ColumnRef("a"), postfix=True).to_sql().as_string()
        '("a" IS NULL)'
    """

    __slots__ = ("operator", "operand", "postfix")

    def __init__(
        self, operator: str, operand: Expression, postfix: bool = False
    ) -> None:
        self.operator = operator
        self.operand = operand
        self.postfix = postfix

    def to_sql(self) -> sql.Composable:
        if self.postfix:
            template = "({operand} {op})"
        else:
            template = "({op} {operand})"
        return sql.SQL(template).format(
            op=sql.SQL(self.operator),
            operand=self.operand.to_sql(),
        )


class PredicateExpr(Expression):
    """A predicate whose SQL is built from a template plus operands.

    Covers shapes that are neither plain binary nor unary: ``IN (...)``,
    ``LIKE``, ``BETWEEN ... AND ...``. The template is a trusted, library-owned
    ``psycopg.sql.SQL`` string; only the operands come from user input and they
    are always escaped.

    Args:
        template: A ``psycopg.sql.SQL`` template with ``{}`` placeholders.
        operands: Expressions filling the placeholders, in order.

    Example:
        >>> from psycopg import sql
        >>> tmpl = sql.SQL("({} BETWEEN {} AND {})")
        >>> PredicateExpr(
        ...     tmpl, [ColumnRef("a"), Literal(1), Literal(9)]
        ... ).to_sql().as_string()
        '("a" BETWEEN 1 AND 9)'
    """

    __slots__ = ("template", "operands")

    def __init__(self, template: sql.SQL, operands: list[Expression]) -> None:
        self.template = template
        self.operands = operands

    def to_sql(self) -> sql.Composable:
        return self.template.format(*[o.to_sql() for o in self.operands])


class CastExpr(Expression):
    """A type cast, ``(expr)::type``.

    Args:
        child: The expression being cast.
        type_name: The target SQL type name. This comes from a library-owned
            :class:`~hologres.dataframe.types.DataType`, never a raw user
            string, so embedding it as SQL is safe.

    Example:
        >>> CastExpr(ColumnRef("b"), "integer").to_sql().as_string()
        '("b")::integer'
    """

    __slots__ = ("child", "type_name")

    def __init__(self, child: Expression, type_name: str) -> None:
        self.child = child
        self.type_name = type_name

    def to_sql(self) -> sql.Composable:
        return sql.SQL("({child})::{type}").format(
            child=self.child.to_sql(),
            type=sql.SQL(self.type_name),
        )


class SortExpr(Expression):
    """An ``ORDER BY`` item: an expression plus direction and null placement.

    Only meaningful inside ``ORDER BY``; :meth:`to_sql` renders the full item.

    Args:
        child: The expression to sort by.
        ascending: ``True`` for ``ASC`` (default), ``False`` for ``DESC``.
        nulls: ``"first"``, ``"last"``, or ``None`` to omit ``NULLS ...`` and
            use the engine default.

    Example:
        >>> SortExpr(ColumnRef("amount"), ascending=False).to_sql().as_string()
        '"amount" DESC'
        >>> SortExpr(ColumnRef("a"), nulls="last").to_sql().as_string()
        '"a" ASC NULLS LAST'
    """

    __slots__ = ("child", "ascending", "nulls")

    def __init__(
        self, child: Expression, ascending: bool = True, nulls: str | None = None
    ) -> None:
        self.child = child
        self.ascending = ascending
        self.nulls = nulls

    def to_sql(self) -> sql.Composable:
        direction = sql.SQL("ASC" if self.ascending else "DESC")
        parts = [self.child.to_sql(), sql.SQL(" "), direction]
        if self.nulls is not None:
            nulls_kw = sql.SQL("NULLS FIRST" if self.nulls == "first" else "NULLS LAST")
            parts.extend([sql.SQL(" "), nulls_kw])
        return sql.Composed(parts)

    @property
    def output_name(self) -> str | None:
        return self.child.output_name


class FunctionCall(Expression):
    """A named function call, ``fn(arg1, arg2, ...)`` or ``fn(*)``.

    Covers both scalar and aggregate function calls -- the SQL syntax is the
    same at the compile layer. ``name`` is embedded via ``sql.SQL`` without
    escaping, so **public entry points must validate it as a bare SQL
    identifier** (:func:`~hologres.dataframe.function` enforces this with a
    regex; the internal ``hg.sum``/``hg.count``/... helpers pass compile-time
    constants). User-provided arguments always render through their
    :class:`Expression` ``.to_sql()`` and are therefore escaped.

    Args:
        name: The SQL function name (e.g. ``"sum"``, ``"count"``,
            ``"string_agg"``).
        args: Positional operand expressions, in order.
        is_star: When ``True``, render as ``name(*)``. In this case ``args``
            must be empty. Used by :func:`hologres.dataframe.count` for
            ``COUNT(*)``.

    Example:
        >>> FunctionCall("count", [], is_star=True).to_sql().as_string()
        'count(*)'
        >>> FunctionCall("sum", [ColumnRef("amount")]).to_sql().as_string()
        'sum("amount")'
        >>> FunctionCall("count", [], is_star=True).output_name
        'count'
        >>> FunctionCall("sum", [ColumnRef("amount")]).output_name
        'sum(amount)'
        >>> FunctionCall("stddev", [ColumnRef("a"), ColumnRef("b")]).output_name is None
        True
    """

    __slots__ = ("name", "args", "is_star", "required_capability")

    def __init__(
        self,
        name: str,
        args: list[Expression],
        is_star: bool = False,
        required_capability: str | None = None,
    ) -> None:
        self.name = name
        self.args = list(args)
        self.is_star = is_star
        self.required_capability = required_capability

    def to_sql(self) -> sql.Composable:
        if self.is_star:
            return sql.SQL("{}(*)").format(sql.SQL(self.name))
        rendered_args = sql.SQL(", ").join(a.to_sql() for a in self.args)
        return sql.SQL("{}({})").format(sql.SQL(self.name), rendered_args)

    @property
    def output_name(self) -> str | None:
        if self.is_star:
            return self.name
        if len(self.args) == 1 and isinstance(self.args[0], ColumnRef):
            return f"{self.name}({self.args[0].name})"
        return None


class RemoteFunctionCall(FunctionCall):
    """A call carrying the remote UDF definition needed before execution.

    SQL rendering is identical to :class:`FunctionCall`. The extra
    ``definition`` reference lets a Session discover and deploy every remote
    function used by a logical plan before it submits the compiled query.
    """

    __slots__ = ("definition",)

    def __init__(self, definition, args: list[Expression]) -> None:
        super().__init__(definition.sql_name, args)
        self.definition = definition

    def to_sql(self) -> sql.Composable:
        rendered_args = sql.SQL(", ").join(arg.to_sql() for arg in self.args)
        return sql.SQL("{}({})").format(
            sql.Identifier(*self.definition.sql_name_parts), rendered_args
        )

    @property
    def output_name(self) -> str | None:
        return self.definition.output_name


class WindowExpr(Expression):
    """A window function call, ``fn(args) OVER (PARTITION BY p1, p2)``.

    Deliberately minimal: partitioning only, no ``ORDER BY`` and no frame
    clause, because the one internal caller (``drop_duplicates``) needs exactly
    ``row_number() OVER (PARTITION BY ...)`` to pick one row per group. Hologres
    rejects ``DISTINCT ON`` when columns outside the ``ON`` list are selected,
    which is why deduplication goes through a window function instead.

    ``name`` is embedded via ``sql.SQL`` without escaping, so it must be a
    library-owned constant, never user input.

    Args:
        name: The window function name, e.g. ``"row_number"``.
        args: Positional argument expressions; may be empty.
        partition_by: Partition expressions. Must be non-empty.

    Example:
        >>> WindowExpr("row_number", [], [ColumnRef("region")]).to_sql().as_string()
        'row_number() OVER (PARTITION BY "region")'
    """

    __slots__ = ("name", "args", "partition_by")

    def __init__(
        self, name: str, args: list[Expression], partition_by: list[Expression]
    ) -> None:
        self.name = name
        self.args = list(args)
        self.partition_by = list(partition_by)

    def to_sql(self) -> sql.Composable:
        return sql.SQL("{name}({args}) OVER (PARTITION BY {parts})").format(
            name=sql.SQL(self.name),
            args=sql.SQL(", ").join(a.to_sql() for a in self.args),
            parts=sql.SQL(", ").join(p.to_sql() for p in self.partition_by),
        )


class RawExpr(Expression):
    """A raw SQL fragment, inserted verbatim into the compiled statement.

    This is the load-bearing carrier of :func:`hologres.dataframe.expr`, the
    library's single documented escape hatch for SQL syntax we do not model
    (e.g. ``count(*) filter (where paid)`` or ``percentile_cont(0.5) within
    group (order by amount)``).

    Warning:
        The fragment is emitted **verbatim**, without any escaping or
        parameterization. Callers are fully responsible for its correctness
        and safety; do not build the fragment by concatenating user input.

    Args:
        sql_text: The raw SQL fragment.

    Example:
        >>> RawExpr("count(*) filter (where paid)").to_sql().as_string()
        'count(*) filter (where paid)'
    """

    __slots__ = ("sql_text",)

    def __init__(self, sql_text: str) -> None:
        self.sql_text = sql_text

    def to_sql(self) -> sql.Composable:
        return sql.SQL(self.sql_text)

    # output_name intentionally inherits None from Expression -- a raw SQL
    # fragment cannot be auto-named; callers must use .alias(...) in agg().


def rewrite_relations(
    expression: Expression,
    mapping: dict[int, str],
    ambiguous: frozenset[int] | set[int] = frozenset(),
    *,
    outputs: dict[tuple[int, str], tuple[str, str] | None] | None = None,
    passthrough: dict[int, str] | None = None,
    sealed: frozenset[int] | set[int] = frozenset(),
) -> Expression:
    """Resolve relation tokens into qualifiers, returning a rewritten tree.

    A ``df["col"]`` reference records *which relation* it came from but not the
    alias that relation will get -- aliases are assigned when a join compiles.
    This walks the tree and replaces every :class:`ColumnRef` whose ``relation``
    is in ``mapping`` with an equivalent ref carrying the corresponding
    qualifier. References to relations outside ``mapping`` are left alone and
    render as bare column names.

    Args:
        mapping: Relation token ``id()`` to alias, e.g. ``{id(plan): "t0"}``.
        ambiguous: Relation token ``id()``\\ s that map to more than one alias
            (the same DataFrame used on both sides of a join). Referencing one
            is an error rather than a silent wrong qualifier.

    Returns:
        The rewritten expression. The input is never mutated.

    Raises:
        InvalidArgumentError: If a reference resolves to an ambiguous relation.
        TypeError: If the tree contains an unknown node type.

    Example:
        >>> plan = object()
        >>> ref = ColumnRef("amount", relation=plan)
        >>> ref.to_sql().as_string()
        '"amount"'
        >>> rewrite_relations(ref, {id(plan): "t0"}).to_sql().as_string()
        '"t0"."amount"'
    """
    outputs = outputs or {}
    passthrough = passthrough or {}

    def rewrite(child: Expression) -> Expression:
        return rewrite_relations(
            child,
            mapping,
            ambiguous,
            outputs=outputs,
            passthrough=passthrough,
            sealed=sealed,
        )

    if isinstance(expression, ColumnRef):
        if expression.relation is None:
            return expression
        key = id(expression.relation)
        if key in ambiguous:
            raise InvalidArgumentError(
                f"column {expression.name!r} is ambiguous: the same DataFrame is "
                "used on both sides of the join. Get each side separately (call "
                "session.table(...) once per side) or use the USING form, e.g. "
                f"on={expression.name!r}"
            )
        if key in mapping:
            return ColumnRef(
                expression.name,
                qualifier=mapping[key],
                relation=expression.relation,
            )
        output_key = (key, expression.name)
        if output_key in outputs:
            binding = outputs[output_key]
            if binding is None:
                raise InvalidArgumentError(
                    f"column {expression.name!r} is ambiguous after a projection"
                )
            qualifier, name = binding
            return ColumnRef(
                name,
                qualifier=qualifier,
                relation=expression.relation,
            )
        if key in passthrough:
            return ColumnRef(
                expression.name,
                qualifier=passthrough[key],
                relation=expression.relation,
            )
        if key in sealed:
            raise InvalidArgumentError(
                f"column {expression.name!r} is no longer available after projection"
            )
        return expression

    if isinstance(expression, (Literal, Star, RawExpr)):
        # No children, and a raw fragment is opaque -- the caller owns any
        # qualifiers inside it.
        return expression

    if isinstance(expression, BinaryExpr):
        return BinaryExpr(
            rewrite(expression.left),
            expression.operator,
            rewrite(expression.right),
        )

    if isinstance(expression, UnaryExpr):
        return UnaryExpr(
            expression.operator,
            rewrite(expression.operand),
            postfix=expression.postfix,
        )

    if isinstance(expression, PredicateExpr):
        return PredicateExpr(
            expression.template,
            [rewrite(o) for o in expression.operands],
        )

    if isinstance(expression, CastExpr):
        return CastExpr(
            rewrite(expression.child),
            expression.type_name,
        )

    if isinstance(expression, SortExpr):
        return SortExpr(
            rewrite(expression.child),
            ascending=expression.ascending,
            nulls=expression.nulls,
        )

    if isinstance(expression, RemoteFunctionCall):
        return RemoteFunctionCall(
            expression.definition,
            [rewrite(a) for a in expression.args],
        )

    if isinstance(expression, FunctionCall):
        return FunctionCall(
            expression.name,
            [rewrite(a) for a in expression.args],
            is_star=expression.is_star,
            required_capability=expression.required_capability,
        )

    if isinstance(expression, WindowExpr):
        return WindowExpr(
            expression.name,
            [rewrite(a) for a in expression.args],
            [rewrite(p) for p in expression.partition_by],
        )

    raise TypeError(f"Unknown expression node: {type(expression).__name__}")
