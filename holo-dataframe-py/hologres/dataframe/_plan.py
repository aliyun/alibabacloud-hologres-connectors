"""Internal logical-plan nodes.

A DataFrame is, underneath, an immutable chain of these nodes: a source
(:class:`TableScan`, :class:`RawSql`, :class:`InlineValues`,
:class:`ExternalFilesScan`, :class:`StageScan`, or a not-yet-materialized
:class:`PendingSource`) with single-input operators stacked on top
(:class:`Project`, :class:`Filter`, :class:`Sort`, ...). The DataFrame user API
builds these; :mod:`hologres.dataframe._compiler` turns a plan into a single
``psycopg.sql`` statement.

Nothing here renders SQL or touches a database -- nodes only describe structure,
so rendering and the subquery-nesting rules live entirely in the compiler.

:class:`Join` and :class:`Union` are the two-input nodes. There is no lateral
node: Hologres does not support ``LATERAL``, so ``explode`` compiles to
``unnest`` in the SELECT list, i.e. a plain :class:`Project`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from hologres.table_name import TableName

if TYPE_CHECKING:
    from .column import Column
    from .types import DataType

__all__ = [
    "Aggregate",
    "Distinct",
    "ExternalFilesScan",
    "Filter",
    "InlineValues",
    "Join",
    "Limit",
    "LogicalPlan",
    "Offset",
    "PendingSource",
    "Project",
    "RawSql",
    "Sort",
    "StageScan",
    "TableScan",
    "Union",
    "iter_nodes",
]


class LogicalPlan:
    """Base class for logical-plan nodes.

    Subclasses are immutable structural descriptions; they carry no rendering
    logic. Every non-source node references its upstream via ``child``.
    """

    __slots__ = ()


class TableScan(LogicalPlan):
    """Read all rows and columns from a base table.

    Args:
        table_name: The source table, as a
            :class:`hologres.table_name.TableName` (schema-qualified, quoted,
            injection-safe).

    Example:
        >>> from hologres.table_name import TableName
        >>> TableScan(TableName.valueOf("public.orders")).table_name.table_name
        'orders'
    """

    __slots__ = ("table_name",)

    def __init__(self, table_name: TableName) -> None:
        self.table_name = table_name


class RawSql(LogicalPlan):
    """A source built from a raw SQL string (as with ``session.sql``).

    The text is treated as an opaque subquery source: the compiler wraps it in
    parentheses and never parses or rewrites it. Callers are responsible for the
    SQL's correctness and safety.

    Args:
        sql_text: The raw SQL statement, e.g. ``"SELECT * FROM orders"``.
    """

    __slots__ = ("sql_text",)

    def __init__(self, sql_text: str) -> None:
        self.sql_text = sql_text


class ExternalFilesScan(LogicalPlan):
    """Read server-visible files through Hologres ``EXTERNAL_FILES``.

    Args:
        path: An ``oss://`` path.
        format: ``csv``, ``parquet``, or ``orc``.
        schema: Optional explicit ``StructType``. ``None`` asks Hologres to
            infer the schema.
        options: Validated EXTERNAL_FILES options as string values.
    """

    __slots__ = ("path", "format", "schema", "options")

    def __init__(self, path: str, format: str, schema, options: dict[str, str]) -> None:
        self.path = path
        self.format = format
        self.schema = schema
        self.options = dict(options)


class InlineValues(LogicalPlan):
    """Client-side literal data, embedded in the query as a ``VALUES`` relation.

    This is how small in-memory sources (``hg.from_dict`` / ``hg.from_records``)
    reach Hologres: the rows travel inside the statement itself, so no table is
    created and no data is written -- the source stays fully lazy and needs no
    extra round trip.

    Every value renders through ``psycopg.sql.Literal``, so the result is
    injection-safe regardless of what the caller put in the rows.

    Args:
        column_names: Output column names, in order.
        rows: The data, one tuple per row, each aligned with ``column_names``.
            Must be non-empty -- SQL has no way to spell an empty ``VALUES``.
        types: One :class:`~hologres.dataframe.types.DataType` per column. The
            compiler casts the first row to these so PostgreSQL does not have to
            guess (bare literals would otherwise come out as ``unknown``).

    Example:
        >>> from hologres.dataframe.types import LongType, StringType
        >>> node = InlineValues(["id", "name"], [(1, "a")], [LongType(), StringType()])
        >>> node.column_names
        ['id', 'name']
    """

    __slots__ = ("column_names", "rows", "types")

    def __init__(
        self,
        column_names: list[str],
        rows: list[tuple],
        types: list[DataType],
    ) -> None:
        self.column_names = list(column_names)
        self.rows = list(rows)
        self.types = list(types)


class StageScan(LogicalPlan):
    """Read Arrow files that were uploaded to a Hologres internal stage.

    Compiles to ``external_files(path='internal_stage://<stage>') AS (<cols>)``,
    which Hologres 4.1+ accepts as an ordinary ``FROM`` item. This lets a
    client-side source be queried straight out of the stage: no target table is
    created and the data is never copied a second time.

    Args:
        stage: The internal stage name (already created and populated).
        schema: Column names and types of the staged files, rendered into the
            required ``AS (...)`` column definition list.
    """

    __slots__ = ("stage", "schema")

    def __init__(self, stage: str, schema) -> None:
        self.stage = stage
        self.schema = schema


class PendingSource(LogicalPlan):
    """A client-side source that has not been uploaded to Hologres yet.

    ``hg.from_pandas`` (and local ``read_files``) must not write anything when
    the DataFrame is built -- that would break the lazy contract. This node
    stands in for the data until the first action compiles the plan, at which
    point the compiler calls ``loader()`` once and caches the real source node
    it returns (a :class:`StageScan` or :class:`TableScan`).

    Args:
        loader: Zero-argument callable performing the upload and returning the
            resolved source plan. Called at most once.
        schema: The source's schema, known upfront so ``df.schema`` and the
            build-time column checks work without touching the server.
        requirements: Static or zero-argument callable returning capability
            requirements that must be checked before ``loader`` runs.
    """

    __slots__ = ("loader", "schema", "resolved", "requirements")

    def __init__(self, loader, schema, *, requirements=()) -> None:
        self.loader = loader
        self.schema = schema
        self.resolved: LogicalPlan | None = None
        self.requirements = requirements

    def resolve(self) -> LogicalPlan:
        """Materialize the data if needed and return the real source node."""
        if self.resolved is None:
            self.resolved = self.loader()
        return self.resolved


class Project(LogicalPlan):
    """Choose/derive output columns (a ``SELECT`` list).

    Args:
        child: Upstream plan.
        columns: The projected columns, in order. Each is a
            :class:`~hologres.dataframe.column.Column` and may be a bare
            reference, an aliased expression, or a derived expression.
    """

    __slots__ = ("child", "columns")

    def __init__(self, child: LogicalPlan, columns: list[Column]) -> None:
        self.child = child
        self.columns = list(columns)


class Filter(LogicalPlan):
    """Keep only rows matching a predicate (a ``WHERE`` clause).

    Args:
        child: Upstream plan.
        predicate: A boolean-valued
            :class:`~hologres.dataframe.column.Column`.
    """

    __slots__ = ("child", "predicate")

    def __init__(self, child: LogicalPlan, predicate: Column) -> None:
        self.child = child
        self.predicate = predicate


class Sort(LogicalPlan):
    """Order rows (an ``ORDER BY`` clause).

    Args:
        child: Upstream plan.
        orders: Sort keys, in priority order. Each is a
            :class:`~hologres.dataframe.column.Column`; direction comes from the
            column's ``asc``/``desc`` metadata (default ascending).

    Note:
        Ordering is only guaranteed when the sort is the final operation before
        an action. If a later operator (e.g. a projection or filter) pushes the
        sort into a subquery, SQL does not guarantee the outer query preserves
        the subquery's order, so the planner may drop it. Put ``sort``/
        ``order_by`` last in the chain when a stable order matters.
    """

    __slots__ = ("child", "orders")

    def __init__(self, child: LogicalPlan, orders: list[Column]) -> None:
        self.child = child
        self.orders = list(orders)


class Limit(LogicalPlan):
    """Cap the number of rows (a ``LIMIT`` clause).

    Args:
        child: Upstream plan.
        n: Maximum number of rows. Must be a non-negative integer.

    Raises:
        ValueError: If ``n`` is not a non-negative integer.
    """

    __slots__ = ("child", "n")

    def __init__(self, child: LogicalPlan, n: int) -> None:
        if not isinstance(n, int) or isinstance(n, bool) or n < 0:
            raise ValueError(f"limit must be a non-negative integer, got {n!r}")
        self.child = child
        self.n = n


class Offset(LogicalPlan):
    """Skip a number of leading rows (an ``OFFSET`` clause).

    Args:
        child: Upstream plan.
        n: Number of rows to skip. Must be a non-negative integer.

    Raises:
        ValueError: If ``n`` is not a non-negative integer.
    """

    __slots__ = ("child", "n")

    def __init__(self, child: LogicalPlan, n: int) -> None:
        if not isinstance(n, int) or isinstance(n, bool) or n < 0:
            raise ValueError(f"offset must be a non-negative integer, got {n!r}")
        self.child = child
        self.n = n


class Distinct(LogicalPlan):
    """Remove duplicate rows.

    With ``on=None`` this is ``SELECT DISTINCT`` over all output columns. With
    ``on`` set it is PostgreSQL's ``SELECT DISTINCT ON (...)``, keeping one row
    per distinct combination of the given columns.

    Args:
        child: Upstream plan.
        on: Columns for ``DISTINCT ON (...)``, or ``None`` for plain
            ``DISTINCT``.

    Note:
        When ``on`` is set, the caller must place this node directly above a
        :class:`Sort` whose leading keys match ``on`` (e.g. ``drop_duplicates``
        builds ``Distinct(Sort(child, keys), on=keys)``). PostgreSQL requires the
        ``DISTINCT ON`` expressions to be the initial ``ORDER BY`` expressions,
        and without a matching sort the kept row per group is non-deterministic.
        The compiler trusts this invariant and does not validate the prefix.
    """

    __slots__ = ("child", "on")

    def __init__(self, child: LogicalPlan, on: list[Column] | None = None) -> None:
        self.child = child
        self.on = list(on) if on is not None else None


class Aggregate(LogicalPlan):
    """Group rows by keys and produce aggregations.

    Compiles to ``SELECT k1, ..., agg1, ... FROM (...) GROUP BY k1, ...``.
    An empty ``group_keys`` is a global aggregation (no ``GROUP BY``): the
    whole input collapses to a single row.

    Args:
        child: Upstream plan.
        group_keys: Group-by keys, in order. May be empty.
        aggregations: Aggregate output columns, in order. Must be non-empty.

    Note:
        PostgreSQL requires every non-aggregate column in the SELECT list to
        appear in ``GROUP BY``. This node trusts the caller to comply and
        leaves the enforcement to the server; the client-side ``group_by``/
        ``agg`` builders shape the columns so this constraint holds.
    """

    __slots__ = ("child", "group_keys", "aggregations")

    def __init__(
        self,
        child: LogicalPlan,
        group_keys: list[Column],
        aggregations: list[Column],
    ) -> None:
        self.child = child
        self.group_keys = list(group_keys)
        self.aggregations = list(aggregations)


JOIN_TYPES = ("inner", "left", "right", "full", "cross")


class Join(LogicalPlan):
    """Combine two inputs row-wise (a ``JOIN`` clause).

    The join condition takes one of two shapes, mirroring SQL:

    * ``using``: a list of column names present in both inputs, compiling to
      ``USING ("c1", "c2")``. The join columns appear once in the output.
    * ``on``: a boolean predicate, compiling to ``ON (...)``. Column references
      are qualified with the aliases the compiler assigns to each input.

    Args:
        left: Left input plan.
        right: Right input plan.
        how: One of :data:`JOIN_TYPES`, already normalized to lowercase.
        on: The join predicate, for the ``ON`` form.
        using: Shared column names, for the ``USING`` form.

    Raises:
        ValueError: If ``how`` is unknown, if a cross join is given a condition,
            or if a non-cross join is not given exactly one of ``on``/``using``.
    """

    __slots__ = ("left", "right", "how", "on", "using")

    def __init__(
        self,
        left: LogicalPlan,
        right: LogicalPlan,
        how: str,
        on: Column | None = None,
        using: list[str] | None = None,
    ) -> None:
        if how not in JOIN_TYPES:
            raise ValueError(f"unknown join type {how!r}, expected one of {JOIN_TYPES}")
        if how == "cross":
            if on is not None or using is not None:
                raise ValueError("a cross join takes no on/using condition")
        elif (on is None) == (using is None):
            raise ValueError(
                f"a {how} join needs exactly one of on/using, got "
                f"on={on!r}, using={using!r}"
            )
        self.left = left
        self.right = right
        self.how = how
        self.on = on
        self.using = list(using) if using is not None else None


class Union(LogicalPlan):
    """Stack one input's rows on top of another's (a ``UNION`` clause).

    Columns are matched **by position**, and the output takes the left input's
    column names -- the same rule PostgreSQL applies. ``distinct`` selects
    between the two SQL forms.

    Args:
        left: Top input plan; its column names become the output names.
        right: Bottom input plan.
        distinct: ``True`` for ``UNION`` (duplicate rows removed), ``False`` for
            ``UNION ALL`` (duplicates kept).
    """

    __slots__ = ("left", "right", "distinct")

    def __init__(self, left: LogicalPlan, right: LogicalPlan, distinct: bool) -> None:
        self.left = left
        self.right = right
        self.distinct = distinct


def iter_nodes(plan: LogicalPlan) -> Iterator[LogicalPlan]:
    """Yield ``plan`` and every node beneath it, depth-first.

    Walks both the single-input ``child`` link and the two-input ``left``/
    ``right`` links, so it covers any plan shape. Used to map every relation in
    a join's input to that input's alias.

    Example:
        >>> from hologres.table_name import TableName
        >>> scan = TableScan(TableName.valueOf("t"))
        >>> [type(n).__name__ for n in iter_nodes(Limit(scan, 5))]
        ['Limit', 'TableScan']
    """
    yield plan
    child = getattr(plan, "child", None)
    if child is not None:
        yield from iter_nodes(child)
    for side in ("left", "right"):
        node = getattr(plan, side, None)
        if node is not None:
            yield from iter_nodes(node)
