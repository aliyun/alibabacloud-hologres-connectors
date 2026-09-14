"""Compile a logical plan into a single PostgreSQL statement.

The strategy is *conservative fusion*: stack as many operators as is provably
safe into one ``SELECT``, and drop down into a subquery only when merging would
change semantics. Correctness first, fewer subqueries second -- when in doubt,
nest (PostgreSQL handles subquery nesting without any correctness penalty).

Everything here is a pure function of the plan; no database is touched. The
result is a :class:`psycopg.sql.Composed`, so it renders with ``.as_string()``
and every identifier/literal is escaped.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from psycopg import sql

from . import _expr, _plan
from ._external_files import build_external_files_relation
from .functions import col

if TYPE_CHECKING:
    from .column import Column

__all__ = ["compile_plan"]

# Library-owned constants, safe to embed as SQL keywords.
_JOIN_KEYWORDS = {
    "inner": "INNER JOIN",
    "left": "LEFT JOIN",
    "right": "RIGHT JOIN",
    "full": "FULL JOIN",
    "cross": "CROSS JOIN",
}


def _needs_json_cast(datatype) -> bool:
    """Whether a staged column arrives as text and must be cast back."""
    from .types import JsonbType

    return isinstance(datatype, JsonbType)


def _stage_type_name(datatype) -> str:
    """The type to declare for a staged column in the ``AS (...)`` list.

    Hologres cannot read json/jsonb directly out of a staged Arrow file, so
    those columns are declared as ``text`` and cast in the projection instead.
    """
    return "text" if _needs_json_cast(datatype) else datatype.sql_name


@dataclass
class _SelectState:
    """The slots of one ``SELECT`` being assembled.

    ``from_sql`` is the ``FROM`` content (a table identifier or a parenthesized,
    aliased subquery). ``projection`` of ``None`` means ``SELECT *``.
    """

    from_sql: sql.Composable
    projection: list[Column] | None = None
    distinct: bool = False
    distinct_on: list[Column] | None = None
    where: list[Column] = field(default_factory=list)
    group_by: list[Column] | None = None
    order_by: list[Column] = field(default_factory=list)
    limit: int | None = None
    offset: int | None = None


class _Compiler:
    """Stateful compiler; holds the subquery-alias counter for one compile."""

    def __init__(self) -> None:
        self._alias_counter = 0
        # Relation identity (id() of a plan node) -> the join alias it renders
        # under. Populated when a Join compiles and dropped again as soon as the
        # surrounding SELECT is wrapped in a subquery, since the aliases stop
        # being visible at that point.
        self._relation_alias: dict[int, str] = {}
        self._ambiguous: set[int] = set()
        self._relation_outputs: dict[tuple[int, str], tuple[str, str] | None] = {}
        self._relation_passthrough: dict[int, str] = {}
        self._sealed_relations: set[int] = set()

    def _next_alias(self) -> str:
        alias = f"t{self._alias_counter}"
        self._alias_counter += 1
        return alias

    def _bind(self, column: Column) -> Column:
        """Resolve a column's relation references against the current aliases."""
        return column._bind_relations(
            self._relation_alias,
            self._ambiguous,
            outputs=self._relation_outputs,
            passthrough=self._relation_passthrough,
            sealed=self._sealed_relations,
        )

    def _clear_relation_bindings(self) -> None:
        self._relation_alias = {}
        self._ambiguous = set()
        self._relation_outputs = {}
        self._relation_passthrough = {}
        self._sealed_relations = set()

    # -- rendering ----------------------------------------------------------

    def render(self, state: _SelectState) -> sql.Composed:
        """Render one ``_SelectState`` to a ``SELECT`` statement."""
        parts: list[sql.Composable] = [sql.SQL("SELECT ")]

        if state.distinct_on is not None:
            on_list = sql.SQL(", ").join(
                self._bind(c)._to_sql() for c in state.distinct_on
            )
            parts.append(sql.SQL("DISTINCT ON ({}) ").format(on_list))
        elif state.distinct:
            parts.append(sql.SQL("DISTINCT "))

        if state.projection is None:
            parts.append(sql.SQL("*"))
        else:
            parts.append(
                sql.SQL(", ").join(
                    self._bind(c)._to_projection_sql() for c in state.projection
                )
            )

        parts.append(sql.SQL(" FROM "))
        parts.append(state.from_sql)

        if state.where:
            predicate = sql.SQL(" AND ").join(
                self._bind(w)._to_sql() for w in state.where
            )
            parts.append(sql.SQL(" WHERE "))
            parts.append(predicate)

        if state.group_by:
            keys = sql.SQL(", ").join(self._bind(k)._to_sql() for k in state.group_by)
            parts.append(sql.SQL(" GROUP BY "))
            parts.append(keys)

        if state.order_by:
            orders = sql.SQL(", ").join(
                self._bind(o)._to_order_sql() for o in state.order_by
            )
            parts.append(sql.SQL(" ORDER BY "))
            parts.append(orders)

        if state.limit is not None:
            parts.append(sql.SQL(" LIMIT {}").format(sql.Literal(state.limit)))

        if state.offset is not None:
            parts.append(sql.SQL(" OFFSET {}").format(sql.Literal(state.offset)))

        return sql.Composed(parts)

    def _nest(self, state: _SelectState) -> _SelectState:
        """Wrap ``state`` as an aliased subquery and start a fresh state on it."""
        alias = self._next_alias()
        subquery = sql.SQL("({}) {}").format(self.render(state), sql.Identifier(alias))
        self._bind_nested_outputs(state, alias)
        return _SelectState(from_sql=subquery)

    def _bind_nested_outputs(self, state: _SelectState, alias: str) -> None:
        """Carry relation-bound columns through a flattened subquery output."""
        direct = self._relation_alias
        previous_outputs = self._relation_outputs
        previous_passthrough = self._relation_passthrough
        known = (
            set(direct)
            | set(previous_passthrough)
            | {key[0] for key in previous_outputs}
            | set(self._sealed_relations)
        )
        projection = state.projection
        has_star = projection is None or any(
            isinstance(column._expr, _expr.Star) for column in projection
        )

        outputs: dict[tuple[int, str], tuple[str, str] | None] = {}
        passthrough: dict[int, str] = {}
        sealed = set(self._sealed_relations)

        if has_star:
            passthrough.update({key: alias for key in direct})
            passthrough.update({key: alias for key in previous_passthrough})
            for key, binding in previous_outputs.items():
                outputs[key] = None if binding is None else (alias, binding[1])
        else:
            sealed.update(known)
            for column in projection:
                expression = column._expr
                if not isinstance(expression, _expr.ColumnRef):
                    continue
                relation = expression.relation
                if relation is None or column.output_name is None:
                    continue
                relation_id = id(relation)
                origins = self._lineage_origins(
                    relation_id,
                    expression.name,
                    direct,
                    previous_outputs,
                    previous_passthrough,
                )
                for origin in origins:
                    binding = (alias, column.output_name)
                    if origin in outputs and outputs[origin] != binding:
                        outputs[origin] = None
                    else:
                        outputs[origin] = binding

        self._relation_alias = {}
        self._ambiguous = set()
        self._relation_outputs = outputs
        self._relation_passthrough = passthrough
        self._sealed_relations = sealed

    @staticmethod
    def _lineage_origins(
        relation_id,
        column_name,
        direct,
        previous_outputs,
        previous_passthrough,
    ):
        if relation_id in direct:
            source_alias = direct[relation_id]
            return {
                (candidate, column_name)
                for candidate, candidate_alias in direct.items()
                if candidate_alias == source_alias
            }
        key = (relation_id, column_name)
        if key in previous_outputs:
            binding = previous_outputs[key]
            if binding is None:
                return {key}
            source_alias, output_name = binding
            return {
                origin
                for origin, candidate in previous_outputs.items()
                if candidate == (source_alias, output_name)
            }
        if relation_id in previous_passthrough:
            source_alias = previous_passthrough[relation_id]
            return {
                (candidate, column_name)
                for candidate, candidate_alias in previous_passthrough.items()
                if candidate_alias == source_alias
            }
        return set()

    # -- merge predicates ---------------------------------------------------

    @staticmethod
    def _has_tail(state: _SelectState) -> bool:
        """Whether ORDER BY / LIMIT / OFFSET is already set (the "tail")."""
        return any((state.order_by, state.limit is not None, state.offset is not None))

    def _project_mergeable(self, state: _SelectState) -> bool:
        # A new projection is safe only over a bare relation: no existing
        # projection, no DISTINCT, no GROUP BY, and nothing in the tail.
        return (
            state.projection is None
            and not state.distinct
            and state.distinct_on is None
            and state.group_by is None
            and not self._has_tail(state)
        )

    def _filter_mergeable(self, state: _SelectState) -> bool:
        # WHERE runs before projection/DISTINCT/GROUP BY/tail, so all must be empty.
        return (
            state.projection is None
            and not state.distinct
            and state.distinct_on is None
            and state.group_by is None
            and not self._has_tail(state)
        )

    # -- plan dispatch ------------------------------------------------------

    def compile(self, plan: _plan.LogicalPlan) -> _SelectState:
        """Compile a plan node (recursively) into a ``_SelectState``."""
        if isinstance(plan, _plan.TableScan):
            return _SelectState(from_sql=plan.table_name.full_name)

        if isinstance(plan, _plan.RawSql):
            alias = self._next_alias()
            from_sql = sql.SQL("({}) {}").format(
                sql.SQL(plan.sql_text), sql.Identifier(alias)
            )
            return _SelectState(from_sql=from_sql)

        if isinstance(plan, _plan.ExternalFilesScan):
            return _SelectState(
                from_sql=build_external_files_relation(
                    plan.path, plan.format, plan.options, plan.schema
                )
            )

        if isinstance(plan, _plan.InlineValues):
            rendered_rows = []
            for index, row in enumerate(plan.rows):
                if index == 0:
                    # Cast the first row so the relation gets real column types;
                    # bare literals would leave PostgreSQL with `unknown`.
                    values = [
                        _expr.CastExpr(_expr.Literal(v), t.sql_name).to_sql()
                        for v, t in zip(row, plan.types)
                    ]
                else:
                    values = [_expr.Literal(v).to_sql() for v in row]
                rendered_rows.append(sql.SQL("({})").format(sql.SQL(", ").join(values)))
            alias = self._next_alias()
            from_sql = sql.SQL("(VALUES {}) {} ({})").format(
                sql.SQL(", ").join(rendered_rows),
                sql.Identifier(alias),
                sql.SQL(", ").join(sql.Identifier(c) for c in plan.column_names),
            )
            return _SelectState(from_sql=from_sql)

        if isinstance(plan, _plan.PendingSource):
            # First compile triggers the upload; later ones reuse the result.
            return self.compile(plan.resolve())

        if isinstance(plan, _plan.StageScan):
            fields = plan.schema.fields
            # Hologres reads json/jsonb out of Arrow files as text, so the
            # column list declares text and the projection casts back.
            declarations = sql.SQL(", ").join(
                sql.SQL("{} {}").format(
                    sql.Identifier(f.name), sql.SQL(_stage_type_name(f.datatype))
                )
                for f in fields
            )
            from_sql = sql.SQL("external_files(path={}) AS ({})").format(
                sql.Literal(f"internal_stage://{plan.stage}"), declarations
            )
            state = _SelectState(from_sql=from_sql)
            if any(_needs_json_cast(f.datatype) for f in fields):
                state.projection = [
                    col(f.name).cast(f.datatype).alias(f.name)
                    if _needs_json_cast(f.datatype)
                    else col(f.name)
                    for f in fields
                ]
            return state

        if isinstance(plan, _plan.Project):
            state = self.compile(plan.child)
            if not self._project_mergeable(state):
                state = self._nest(state)
            state.projection = list(plan.columns)
            return state

        if isinstance(plan, _plan.Filter):
            state = self.compile(plan.child)
            if not self._filter_mergeable(state):
                state = self._nest(state)
            state.where.append(plan.predicate)
            return state

        if isinstance(plan, _plan.Sort):
            state = self.compile(plan.child)
            # Cannot re-sort after LIMIT/OFFSET; and a sort on top of DISTINCT
            # must apply to the de-duplicated result (and must not clobber the
            # ORDER BY that a DISTINCT ON relies on). All force an outer SELECT.
            if (
                state.limit is not None
                or state.offset is not None
                or state.distinct
                or state.distinct_on is not None
            ):
                state = self._nest(state)
            state.order_by = list(plan.orders)
            return state

        if isinstance(plan, _plan.Limit):
            state = self.compile(plan.child)
            if state.limit is not None:
                state = self._nest(state)
            state.limit = plan.n
            return state

        if isinstance(plan, _plan.Offset):
            state = self.compile(plan.child)
            # OFFSET after LIMIT needs nesting; a second OFFSET also nests.
            if state.limit is not None or state.offset is not None:
                state = self._nest(state)
            state.offset = plan.n
            return state

        if isinstance(plan, _plan.Distinct):
            state = self.compile(plan.child)
            if plan.on is None:
                # Plain DISTINCT runs before ORDER BY/LIMIT. An existing tail,
                # DISTINCT, or GROUP BY must be nested, so we never emit
                # `DISTINCT ... ORDER BY <col>` where <col> was dropped by the
                # projection, and DISTINCT after aggregate stays a proper outer.
                if (
                    state.distinct
                    or state.distinct_on is not None
                    or state.group_by is not None
                    or self._has_tail(state)
                ):
                    state = self._nest(state)
                state.distinct = True
            else:
                # DISTINCT ON (cols) must share its SELECT with a leading ORDER
                # BY on the same cols (the caller pairs it with a matching Sort;
                # see _plan.Distinct). Only an existing DISTINCT/GROUP BY/LIMIT/
                # OFFSET forces nesting -- a matching ORDER BY must stay in place.
                if (
                    state.distinct
                    or state.distinct_on is not None
                    or state.group_by is not None
                    or state.limit is not None
                    or state.offset is not None
                ):
                    state = self._nest(state)
                state.distinct_on = list(plan.on)
            return state

        if isinstance(plan, _plan.Aggregate):
            state = self.compile(plan.child)
            # Aggregate takes over the SELECT list and adds GROUP BY. Any
            # existing projection/distinct/group_by/tail must be nested first.
            if (
                state.projection is not None
                or state.distinct
                or state.distinct_on is not None
                or state.group_by is not None
                or self._has_tail(state)
            ):
                state = self._nest(state)
            state.projection = list(plan.group_keys) + list(plan.aggregations)
            state.group_by = list(plan.group_keys)
            return state

        if isinstance(plan, _plan.Join):
            # Render both inputs first: each may nest internally, which clears
            # the alias map, so the map is only installed once both are done.
            left_from, left_alias = self._join_operand(plan.left)
            right_from, right_alias = self._join_operand(plan.right)
            self._relation_alias, self._ambiguous = self._relation_map(
                plan.left, left_alias, plan.right, right_alias
            )
            self._relation_outputs = {}
            self._relation_passthrough = {}
            self._sealed_relations = set()

            parts: list[sql.Composable] = [
                left_from,
                sql.SQL(" {} ").format(sql.SQL(_JOIN_KEYWORDS[plan.how])),
                right_from,
            ]
            if plan.using is not None:
                using = sql.SQL(", ").join(sql.Identifier(c) for c in plan.using)
                parts.append(sql.SQL(" USING ({})").format(using))
            elif plan.on is not None:
                parts.append(sql.SQL(" ON {}").format(self._bind(plan.on)._to_sql()))
            return _SelectState(from_sql=sql.Composed(parts))

        if isinstance(plan, _plan.Union):
            left_sql = self.render(self.compile(plan.left))
            self._clear_relation_bindings()
            right_sql = self.render(self.compile(plan.right))
            self._clear_relation_bindings()
            keyword = sql.SQL(" UNION " if plan.distinct else " UNION ALL ")
            # Each arm is parenthesized so a per-arm LIMIT/ORDER BY binds to that
            # arm, not to the whole set operation.
            combined = sql.SQL("({}){}({})").format(left_sql, keyword, right_sql)
            alias = self._next_alias()
            wrapped = sql.SQL("({}) {}").format(combined, sql.Identifier(alias))
            return _SelectState(from_sql=wrapped)

        raise TypeError(f"Unknown plan node: {type(plan).__name__}")

    # -- join helpers -------------------------------------------------------

    def _join_operand(self, plan: _plan.LogicalPlan) -> tuple[sql.Composable, str]:
        """Render one join input as an aliased ``FROM`` item.

        Both inputs always get an alias so that ``ON`` predicates can qualify
        their columns unambiguously. A base table becomes ``"schema"."t" "t0"``;
        anything else becomes ``(SELECT ...) "t0"``.
        """
        if isinstance(plan, _plan.TableScan):
            alias = self._next_alias()
            operand = sql.SQL("{} {}").format(
                plan.table_name.full_name, sql.Identifier(alias)
            )
            return operand, alias

        if isinstance(plan, _plan.RawSql):
            alias = self._next_alias()
            operand = sql.SQL("({}) {}").format(
                sql.SQL(plan.sql_text), sql.Identifier(alias)
            )
            return operand, alias

        state = self.compile(plan)
        rendered = self.render(state)
        alias = self._next_alias()
        # The input's own aliases are sealed inside its subquery.
        self._clear_relation_bindings()
        return sql.SQL("({}) {}").format(rendered, sql.Identifier(alias)), alias

    @staticmethod
    def _relation_map(
        left: _plan.LogicalPlan,
        left_alias: str,
        right: _plan.LogicalPlan,
        right_alias: str,
    ) -> tuple[dict[int, str], set[int]]:
        """Map every relation in each input to that input's alias.

        Mapping the whole subtree (not just its root) is what lets an ``ON``
        predicate reference the original table -- ``orders["id"]`` still resolves
        after ``orders.filter(...)`` became the join input.

        A relation reachable from both sides (the same DataFrame joined to
        itself) cannot be resolved and lands in the ambiguous set instead.
        """
        mapping = {id(node): left_alias for node in _plan.iter_nodes(left)}
        ambiguous: set[int] = set()
        for node in _plan.iter_nodes(right):
            key = id(node)
            if key in mapping:
                del mapping[key]
                ambiguous.add(key)
            else:
                mapping[key] = right_alias
        return mapping, ambiguous


def compile_plan(plan: _plan.LogicalPlan) -> sql.Composed:
    """Compile a logical plan into a single PostgreSQL ``SELECT`` statement.

    Args:
        plan: The root logical-plan node.

    Returns:
        A :class:`psycopg.sql.Composed` ready to render with ``.as_string()`` or
        execute with a psycopg cursor.

    Raises:
        TypeError: If the plan contains an unknown node type.

    Example:
        >>> from hologres.table_name import TableName
        >>> from hologres.dataframe import _plan
        >>> from hologres.dataframe.functions import col
        >>> scan = _plan.TableScan(TableName.valueOf("public.orders"))
        >>> flt = _plan.Filter(scan, col("amount") > 100)
        >>> compile_plan(flt).as_string()
        'SELECT * FROM "public"."orders" WHERE ("amount" > 100)'
    """
    # A bare raw-SQL source needs no wrapping; return it verbatim.
    if isinstance(plan, _plan.RawSql):
        return sql.Composed([sql.SQL(plan.sql_text)])

    compiler = _Compiler()
    return compiler.render(compiler.compile(plan))
