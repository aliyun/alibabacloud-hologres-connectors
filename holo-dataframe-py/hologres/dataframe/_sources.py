"""Client-side data sources: build a DataFrame from in-memory Python data.

``hg.from_dict`` and ``hg.from_records`` keep the data **in the statement**: the
rows are compiled into an inline ``VALUES`` relation, so nothing is written to
Hologres, no table is created, and the source stays as lazy as any other. That
makes them ideal for small lookup tables, test fixtures, and literals you want
to join against a real table.

    import hologres.dataframe as hg

    tiers = hg.from_dict(
        {"tier": ["gold", "silver"], "discount": [0.2, 0.1]}, session=session
    )
    orders.join(tiers, on="tier").show()

Large data does not belong in a statement; past ``inline_limit`` rows these
functions hand off to the materializing path (``hg.from_pandas``).
"""

from __future__ import annotations

from typing import Any

from hologres import HoloConfig

from . import _plan
from .exceptions import InvalidArgumentError, UnsupportedOperationError
from .types import DataType, DoubleType, LongType, StringType, StructField, StructType

__all__ = ["from_dict", "from_pandas", "from_records"]

# Past this many rows, embedding the data in the SQL text stops being sane.
_DEFAULT_INLINE_LIMIT = 10000

# Arrow/stage ingestion landed in Hologres 4.1; older servers get a table.
_STAGE_MIN_VERSION = (4, 1, 0)

# Stage entries expire on their own, so an abandoned upload cannot leak forever.
_STAGE_TTL_SECONDS = 7200

# Prefix for auto-generated server-side objects, so leftovers are identifiable.
_SCRATCH_PREFIX = "_hg_df_tmp_"


def from_dict(
    data: dict[str, list],
    *,
    session: Any = None,
    schema: StructType | None = None,
    inline_limit: int = _DEFAULT_INLINE_LIMIT,
):
    """Build a DataFrame from a column-oriented dict.

    Args:
        data: Maps column name to that column's values. Every column must have
            the same length, and there must be at least one column and one row.
        session: The session the result should execute on. Optional -- without
            one the DataFrame still compiles to SQL, but actions will refuse to
            run for lack of a connection.
        schema: Overrides the inferred column names and types. Must have the
            same number of fields as ``data`` has columns.
        inline_limit: Maximum number of rows to embed in the statement.

    Returns:
        A lazy :class:`~hologres.dataframe.dataframe.DataFrame` over an inline
        ``VALUES`` relation.

    Raises:
        InvalidArgumentError: If ``data`` is not a non-empty dict of equal-length
            value sequences, or ``schema`` does not match the column count.
        UnsupportedOperationError: If the data has more than ``inline_limit``
            rows; use :func:`from_pandas` for data that large.

    Example:
        >>> import hologres.dataframe as hg
        >>> df = hg.from_dict({"id": [1, 2], "name": ["a", "b"]})
        >>> out = df._to_sql().as_string()
        >>> 'VALUES ((1)::bigint, (\\'a\\')::text), (2, \\'b\\')' in out
        True
        >>> df.columns
        ['id', 'name']
    """
    if not isinstance(data, dict) or not data:
        raise InvalidArgumentError(
            "from_dict expects a non-empty dict of column name -> values, got "
            f"{type(data).__name__}"
        )

    names = []
    columns = []
    for name, values in data.items():
        if not isinstance(name, str) or not name:
            raise InvalidArgumentError(
                f"column names must be non-empty strings, got {name!r}"
            )
        if not isinstance(values, (list, tuple)):
            raise InvalidArgumentError(
                f"column {name!r} must hold a list or tuple of values, got "
                f"{type(values).__name__}"
            )
        names.append(name)
        columns.append(list(values))

    lengths = {len(c) for c in columns}
    if len(lengths) > 1:
        sizes = {n: len(c) for n, c in zip(names, columns)}
        raise InvalidArgumentError(f"columns must be the same length, got {sizes}")

    rows = [tuple(row) for row in zip(*columns)]
    return _inline_dataframe(names, columns, rows, session, schema, inline_limit)


def from_records(
    rows: list[dict] | list[tuple],
    *,
    columns: list[str] | None = None,
    session: Any = None,
    schema: StructType | None = None,
    inline_limit: int = _DEFAULT_INLINE_LIMIT,
):
    """Build a DataFrame from row-oriented data.

    Args:
        rows: Either a list of dicts (column names come from the first row, and
            every row must carry exactly those keys) or a list of tuples/lists
            (in which case ``columns`` is required). Must be non-empty.
        columns: Column names, required when ``rows`` holds tuples.
        session: The session the result should execute on; see :func:`from_dict`.
        schema: Overrides the inferred column names and types.
        inline_limit: Maximum number of rows to embed in the statement.

    Returns:
        A lazy :class:`~hologres.dataframe.dataframe.DataFrame` over an inline
        ``VALUES`` relation.

    Raises:
        InvalidArgumentError: If ``rows`` is empty or mixes shapes, if dict rows
            disagree on their keys, if tuple rows arrive without ``columns`` or
            with the wrong width, or if ``schema`` does not match.
        UnsupportedOperationError: If there are more than ``inline_limit`` rows.

    Example:
        >>> import hologres.dataframe as hg
        >>> df = hg.from_records([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
        >>> df.columns
        ['id', 'name']
        >>> hg.from_records([(1, "a")], columns=["id", "name"]).columns
        ['id', 'name']
    """
    if not isinstance(rows, (list, tuple)) or not rows:
        raise InvalidArgumentError(
            "from_records expects a non-empty list of dicts or tuples, got "
            f"{type(rows).__name__}"
        )

    first = rows[0]
    if isinstance(first, dict):
        if columns is not None:
            raise InvalidArgumentError(
                "columns= applies to tuple rows only; dict rows already name "
                "their columns"
            )
        names = list(first.keys())
        expected = set(names)
        tuples = []
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                raise InvalidArgumentError(
                    f"row {index} is a {type(row).__name__}, but row 0 is a dict; "
                    "all rows must have the same shape"
                )
            if set(row.keys()) != expected:
                raise InvalidArgumentError(
                    f"row {index} has keys {tuple(row.keys())}, expected "
                    f"{tuple(names)}; all rows must share the same keys"
                )
            tuples.append(tuple(row[n] for n in names))
    elif isinstance(first, (list, tuple)):
        if not columns:
            raise InvalidArgumentError(
                "columns= is required when rows are tuples, since tuples carry "
                "no column names"
            )
        names = list(columns)
        tuples = []
        for index, row in enumerate(rows):
            if not isinstance(row, (list, tuple)):
                raise InvalidArgumentError(
                    f"row {index} is a {type(row).__name__}, but row 0 is a "
                    f"{type(first).__name__}; all rows must have the same shape"
                )
            if len(row) != len(names):
                raise InvalidArgumentError(
                    f"row {index} has {len(row)} value(s) but {len(names)} "
                    f"column name(s) were given: {tuple(names)}"
                )
            tuples.append(tuple(row))
    else:
        raise InvalidArgumentError(
            f"rows must hold dicts or tuples, got {type(first).__name__}"
        )

    columns_data = [list(col) for col in zip(*tuples)]
    return _inline_dataframe(names, columns_data, tuples, session, schema, inline_limit)


def _inline_dataframe(names, columns, rows, session, schema, inline_limit):
    """Assemble the InlineValues plan shared by from_dict/from_records."""
    if not rows:
        raise InvalidArgumentError(
            "the data has no rows; SQL cannot express an empty VALUES relation"
        )
    if not isinstance(inline_limit, int) or isinstance(inline_limit, bool):
        raise InvalidArgumentError(
            f"inline_limit must be an int, got {type(inline_limit).__name__}"
        )
    if len(rows) > inline_limit:
        if session is None:
            raise UnsupportedOperationError(
                f"{len(rows)} rows exceeds inline_limit={inline_limit}, so the data "
                "must be uploaded instead of embedded in the statement -- which "
                "needs session=..."
            )
        # Too big for the statement: hand off to the materializing path.
        return _inline_overflow_to_arrow(names, columns, session, schema)

    if schema is not None:
        if not isinstance(schema, StructType):
            raise InvalidArgumentError(
                f"schema must be a StructType, got {type(schema).__name__}"
            )
        if len(schema.fields) != len(names):
            raise InvalidArgumentError(
                f"schema has {len(schema.fields)} field(s) but the data has "
                f"{len(names)} column(s) {tuple(names)}"
            )
        out_names = list(schema.names)
        types = [f.datatype for f in schema.fields]
        out_schema = schema
    else:
        out_names = names
        types = [_infer_column_type(n, c) for n, c in zip(names, columns)]
        out_schema = StructType(
            [StructField(n, t) for n, t in zip(out_names, types)],
        )

    from .dataframe import DataFrame

    plan = _plan.InlineValues(out_names, rows, types)
    return DataFrame(plan, session, schema=out_schema)


def _infer_column_type(name: str, values: list) -> DataType:
    """Infer one column's type from its Python values.

    An all-NULL column has nothing to go on and becomes ``text``; mixed integers
    and floats widen to ``double precision``; any other mixture is ambiguous and
    is rejected rather than guessed at.
    """
    from .types import from_python_type

    present = {type(v) for v in values if v is not None}
    if not present:
        return StringType()
    if len(present) > 1:
        if present <= {int, float}:
            return DoubleType() if float in present else LongType()
        kinds = tuple(sorted(t.__name__ for t in present))
        raise InvalidArgumentError(
            f"column {name!r} mixes value types {kinds}; pass schema= to state "
            "the column type explicitly"
        )
    (kind,) = present
    try:
        return from_python_type(kind)
    except InvalidArgumentError as exc:
        raise InvalidArgumentError(
            f"column {name!r} holds unsupported values of type {kind.__name__}; "
            "pass schema= to state the column type explicitly"
        ) from exc


# ---------------------------------------------------------------------------
# Materialized sources: data too large for the statement goes to the server.
# ---------------------------------------------------------------------------


def _inline_overflow_to_arrow(names, columns, session, schema):
    """Route an oversized inline source through the materializing path."""
    pa = _import_pyarrow("uploading data past inline_limit")
    table = pa.table(dict(zip(names, columns)))
    return from_arrow(table, session=session, schema=schema, origin="from_dict")


def from_pandas(
    df,
    *,
    session: Any,
    schema: StructType | None = None,
    table_name: str | None = None,
):
    """Build a DataFrame from a pandas ``DataFrame``, uploading the data.

    Unlike :func:`from_dict`, a pandas frame is assumed to be large enough that
    embedding it in the statement is the wrong move, so the data is always
    uploaded to Hologres. Nothing happens until the first action, though: the
    upload is deferred and performed once, so building and chaining stays free.

    Which channel is used depends on the server:

    * **Hologres 4.1+**: the frame is converted to Arrow and uploaded to an
      internal stage, which is then queried in place via ``external_files``. No
      table is created and the data is not copied a second time.
    * **older**: a plain table is created and filled with ``COPY ... FROM
      STDIN``. It cannot be a PostgreSQL ``TEMPORARY`` table, since those are
      visible only to the connection that made them while queries here run on
      whichever pooled connection is free.

    Both are cleaned up by :meth:`Session.close` (or
    :meth:`Session.drop_scratch`); stages additionally carry a server-side TTL.

    Args:
        df: A ``pandas.DataFrame``.
        session: The session to upload through. Required.
        schema: Overrides the schema inferred from the frame's dtypes.
        table_name: Materialize into this table instead of an auto-generated
            one. Naming it makes it **yours**: it is not registered for
            automatic cleanup.

    Returns:
        A lazy :class:`~hologres.dataframe.dataframe.DataFrame` over the
        uploaded data.

    Raises:
        InvalidArgumentError: If ``session`` is missing, ``df`` is not a pandas
            DataFrame, it has no rows, or ``schema`` does not match its columns.
        ImportError: If pandas or pyarrow is not installed.

    Example:
        >>> import pandas as pd  # doctest: +SKIP
        >>> frame = pd.DataFrame({"id": [1, 2]})  # doctest: +SKIP
        >>> df = session.from_pandas(frame)  # doctest: +SKIP
        >>> df.filter(hg.col("id") > 1).collect()  # uploads here  # doctest: +SKIP
    """
    if session is None:
        raise InvalidArgumentError(
            "from_pandas requires session=...; the data is uploaded to Hologres"
        )
    pa = _import_pyarrow("from_pandas")
    if not hasattr(df, "columns") or not hasattr(df, "dtypes"):
        raise InvalidArgumentError(
            f"from_pandas expects a pandas DataFrame, got {type(df).__name__}"
        )
    table = pa.Table.from_pandas(df, preserve_index=False)
    return from_arrow(
        table,
        session=session,
        schema=schema,
        table_name=table_name,
        origin="from_pandas",
    )


def from_arrow(
    table,
    *,
    session: Any,
    schema: StructType | None = None,
    table_name: str | None = None,
    origin: str = "from_arrow",
):
    """Internal: build a lazily-materialized DataFrame from a ``pyarrow.Table``.

    Shared by :func:`from_pandas` and (from C9c) local file reading, both of
    which already hold columnar data.
    """
    from .types import from_arrow_type

    if table.num_rows == 0:
        raise InvalidArgumentError(f"{origin} got no rows; there is nothing to upload")

    if schema is not None:
        if not isinstance(schema, StructType):
            raise InvalidArgumentError(
                f"schema must be a StructType, got {type(schema).__name__}"
            )
        if len(schema.fields) != table.num_columns:
            raise InvalidArgumentError(
                f"schema has {len(schema.fields)} field(s) but the data has "
                f"{table.num_columns} column(s) {tuple(table.schema.names)}"
            )
    else:
        schema = StructType(
            [
                StructField(field.name, from_arrow_type(field.type))
                for field in table.schema
            ]
        )

    source = _ArrowSource(table)
    use_stage = None

    def choose_stage():
        nonlocal use_stage
        if use_stage is None:
            use_stage = (
                table_name is None and session.server_version() >= _STAGE_MIN_VERSION
            )
        return use_stage

    def loader():
        return _materialize(
            session,
            source,
            schema,
            table_name,
            use_stage=choose_stage(),
        )

    def requirements():
        if choose_stage():
            return (("capability", "external_files"),)
        return ()

    from .dataframe import DataFrame

    return DataFrame(
        _plan.PendingSource(loader, schema, requirements=requirements),
        session,
        schema=schema,
    )


class _ArrowSource:
    """A columnar source that can hand over either Arrow or plain rows.

    The two materialization channels want different shapes, and converting for
    the channel that is not used would be wasted work: the stage path wants the
    Arrow table as-is, the COPY path wants rows. Each accessor is only called by
    the channel that needs it.
    """

    __slots__ = ("_table",)

    def __init__(self, table) -> None:
        self._table = table

    def arrow_table(self):
        """The data as a ``pyarrow.Table`` (already columnar: no conversion)."""
        return self._table

    def row_iter(self):
        """The data as tuples, one per row."""
        columns = [column.to_pylist() for column in self._table.columns]
        return zip(*columns)


def _materialize(
    session,
    source,
    schema: StructType,
    table_name: str | None,
    *,
    use_stage: bool | None = None,
):
    """Upload ``source`` and return the plan node that reads it back.

    Picks the channel by server version, pulling only the representation that
    channel needs.
    """
    if use_stage is None:
        use_stage = (
            table_name is None and session.server_version() >= _STAGE_MIN_VERSION
        )
    if use_stage:
        return _materialize_via_stage(session, source, schema)
    return _materialize_via_table(session, source, schema, table_name)


def _materialize_via_stage(session, source, schema: StructType):
    """Upload Arrow to a fresh internal stage and read it via external_files."""
    from psycopg import sql

    from hologres import CopyStageWriter

    stage = _scratch_name()
    session.execute(
        sql.SQL("call hologres.hg_create_internal_stage({}, {}, {})").format(
            sql.Literal(stage), sql.Literal(stage), sql.Literal(_STAGE_TTL_SECONDS)
        ),
        fetch=False,
    )
    session._register_stage(stage)

    try:
        client_schema = _client_table_schema(schema, stage)
        client_config = _client_stage_config(session)
        table = source.arrow_table().rename_columns(schema.names)
        with CopyStageWriter(
            config=client_config,
            schema=client_schema,
            stage_name=stage,
            auto_commit=False,
        ) as writer:
            writer.write_table(table)
    except Exception as exc:
        raise session._wrap(exc)
    return _plan.StageScan(stage, schema)


def _materialize_via_table(session, source, schema: StructType, table_name):
    """Create a table and fill it with COPY ... FROM STDIN."""
    from hologres.table_name import TableName
    from psycopg import sql

    generated = table_name is None
    name = (
        TableName.valueOf(table_name)
        if table_name
        else TableName.valueOf(_scratch_name())
    )
    columns = sql.SQL(", ").join(
        sql.SQL("{} {}").format(sql.Identifier(f.name), sql.SQL(f.datatype.sql_name))
        for f in schema.fields
    )
    session.execute(
        sql.SQL("CREATE TABLE {} ({})").format(name.full_name, columns), fetch=False
    )
    if generated:
        session._register_scratch_table(name)

    copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(
        name.full_name,
        sql.SQL(", ").join(sql.Identifier(f.name) for f in schema.fields),
    )
    try:
        with session._pool.connection() as conn:
            with conn.cursor() as cur:
                session._apply_schema(cur)
                try:
                    with cur.copy(copy_sql) as copy:
                        for row in source.row_iter():
                            copy.write_row(row)
                finally:
                    session._reset_schema(cur)
    except Exception as exc:
        raise session._wrap(exc)
    return _plan.TableScan(name)


def _client_table_schema(schema: StructType, table_name: str):
    """Adapt a DataFrame schema to the schema consumed by CopyStageWriter."""
    from hologres import Column, TableName, TableSchema

    columns = []
    for field in schema.fields:
        datatype = field.datatype
        type_name = datatype.sql_name
        kwargs = {"allow_null": field.nullable}

        # Column.from_pg_type_name accepts the base name while precision and
        # scale are carried separately for the client's Arrow schema builder.
        if datatype.type_name == "string":
            type_name = "varchar" if getattr(datatype, "length", None) else "text"
        elif datatype.type_name == "decimal":
            type_name = "numeric"
            kwargs["precision"] = datatype.precision or 38
            kwargs["scale"] = datatype.scale or 0

        columns.append(Column.from_pg_type_name(field.name, type_name, **kwargs))

    return TableSchema(TableName.valueOf(f"public.{table_name}"), columns)


def _client_stage_config(session):
    """Build the client writer config from a DataFrame session conninfo."""
    from psycopg.conninfo import conninfo_to_dict

    if session._conninfo is None:
        raise InvalidArgumentError(
            "a stage upload requires a session created via hg.connect(...)"
        )

    values = conninfo_to_dict(session._conninfo)
    return _DataFrameStageConfig(
        session._conninfo,
        host=values["host"],
        port=int(values["port"]),
        database=values["dbname"],
        username=values["user"],
        password=values["password"],
        app_name=values.get("application_name", "hologres-dataframe"),
    )


class _DataFrameStageConfig(HoloConfig):
    """HoloConfig that preserves libpq options from the DataFrame session."""

    def __init__(self, original_conninfo: str, **kwargs):
        super().__init__(**kwargs)
        self._original_conninfo = original_conninfo

    @property
    def conninfo(self) -> str:
        return self._original_conninfo


def _scratch_name() -> str:
    """A unique name for an auto-created server-side object."""
    import uuid

    return f"{_SCRATCH_PREFIX}{uuid.uuid4().hex[:12]}"


def _import_pyarrow(what: str):
    """Import pyarrow, with an actionable message when it is missing."""
    try:
        import pyarrow as pa
    except ImportError as exc:  # pragma: no cover - exercised via monkeypatch
        raise ImportError(
            f"pyarrow is required for {what}. Install it with: pip install pyarrow"
        ) from exc
    return pa
