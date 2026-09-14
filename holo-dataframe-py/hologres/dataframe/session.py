"""The :class:`Session` and the top-level :func:`connect` entry point.

A ``Session`` is the execution boundary of ``hologres.dataframe``: it holds a
``psycopg_pool`` connection pool to Hologres, a current schema (``search_path``),
and the methods that turn a plan into results. Everything upstream of a Session
call is lazy; a Session call is the first thing that touches the network.

Typical use::

    import hologres.dataframe as hg

    with hg.connect(
        host="xxx.hologres.aliyuncs.com",
        port=80,
        dbname="my_db",
        user="<access_id>",
        password="<access_key>",
    ) as session:
        session.table("public.orders").filter(hg.col("amount") > 100).show(10)

Naming and schema semantics follow Ibis: :meth:`Session.table` fetches the table
schema once and pins it on the resulting DataFrame, so ``df.schema`` /
``df.columns`` are then free, and column-name errors surface immediately at
build time -- consistent with Spark's "analysis before action" contract.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any, Iterator

from psycopg import Connection, conninfo, sql
from psycopg_pool import ConnectionPool

from .exceptions import (
    ExecutionError,
    InvalidArgumentError,
    UnsupportedOperationError,
)
from .row import Row
from .types import StructField, StructType, parse_type

if TYPE_CHECKING:
    from .dataframe import DataFrame

__all__ = ["Session", "connect"]

logger = logging.getLogger("hologres.dataframe")

# hg_version() returns e.g. "Hologres 5.1.0 (tag: ...), compatible with ...".
_VERSION_RE = re.compile(r"Hologres\s+(\d+)\.(\d+)(?:\.(\d+))?", re.IGNORECASE)


# ------------------------------------------------------------------------
# Type-oid → PG type name mapping for cursor.description.
#
# psycopg3 exposes column type_code as an OID (integer). We resolve it via
# psycopg.postgres.types, which is the built-in registry, so any user-defined
# type falls back to a plain best-effort name.
# ------------------------------------------------------------------------


def _oid_to_type_name(type_code: int) -> str:
    """Resolve a PostgreSQL OID (from cursor.description.type_code) to a name.

    Falls back to ``"text"`` for unknown OIDs so :func:`parse_type` still yields
    a usable type.
    """
    from psycopg.postgres import types as pg_types

    try:
        info = pg_types[type_code]
    except KeyError:
        return "text"
    # In psycopg's registry an array OID resolves to its element's TypeInfo,
    # with the array_oid attribute pointing back to itself. Detect that so we
    # can render as e.g. "int4[]".
    if getattr(info, "array_oid", None) == type_code:
        return f"{info.name}[]"
    return info.name


def _description_to_struct(description) -> StructType:
    """Turn a psycopg cursor.description into a :class:`StructType`."""
    fields = []
    for col in description or []:
        try:
            datatype = parse_type(_oid_to_type_name(col.type_code))
        except InvalidArgumentError:
            # Unknown type -> approximate as text so callers can still work with it.
            datatype = parse_type("text")
        fields.append(StructField(col.name, datatype))
    return StructType(fields)


def _parse_table_name(name: str, default_schema: str = "public"):
    """Parse a table identifier, reporting failures as InvalidArgumentError.

    ``TableName.valueOf`` raises a bare ``ValueError``, which would escape the
    library's exception tree; callers of the public API should only ever see a
    :class:`~hologres.dataframe.exceptions.HoloDataFrameError`.
    """
    from hologres.table_name import TableName

    try:
        parts = TableName._parse_multi_identifier(name)
        if len(parts) == 1:
            return TableName(default_schema, parts[0])
        return TableName.valueOf(name)
    except ValueError as exc:
        raise InvalidArgumentError(f"invalid table name {name!r}: {exc}") from exc


class Session:
    """A live connection to Hologres backed by a ``psycopg_pool`` connection pool.

    You do not construct this directly; call :func:`connect` (or ``hg.connect``).
    Every :class:`~hologres.dataframe.dataframe.DataFrame` obtained from this
    session executes through the same pool.

    A Session is a context manager: enter to guarantee closure of the pool.

    Example:
        >>> import hologres.dataframe as hg  # doctest: +SKIP
        >>> with hg.connect(host="h", port=80, dbname="d", user="u",
        ...                 password="p") as session:  # doctest: +SKIP
        ...     rows = session.sql("SELECT 1").collect()
    """

    __slots__ = (
        "_pool",
        "_closed",
        "_dbname",
        "_pool_factory",
        "_conninfo",
        "_schema_name",
        "_server_version",
        "_capability_cache",
        "_scratch_tables",
        "_stages",
        "_fc_config",
        "_fc_deployer",
        "_deployed_remote_functions",
    )

    def __init__(
        self,
        pool: ConnectionPool,
        dbname: str,
        pool_factory=None,
        conninfo: str | None = None,
        fc_config=None,
        fc_deployer=None,
    ) -> None:
        # Internal constructor. Users call connect() / hg.connect().
        self._pool = pool
        self._dbname = dbname
        self._closed = False
        # Builds a fresh pool bound to a given dbname; set by connect() so
        # use_database can reconnect. None for test-injected sessions, which do
        # not support switching databases.
        self._pool_factory = pool_factory
        # The libpq conninfo this session was opened with, needed to derive a
        # FixedFE connection for stage uploads. None for test-injected sessions.
        self._conninfo = conninfo
        self._schema_name: str | None = None
        self._server_version: tuple[int, int, int] | None = None
        self._capability_cache: dict[tuple[str, str], bool] = {}
        # Server-side objects created on this session's behalf, dropped on close.
        self._scratch_tables: list = []
        self._stages: list = []
        from ._fc import validate_fc_config

        self._fc_config = validate_fc_config(fc_config)
        self._fc_deployer = fc_deployer
        self._deployed_remote_functions: dict[tuple, tuple[str, Any]] = {}

    # -- lifecycle ----------------------------------------------------------

    def close(self) -> None:
        """Drop this session's scratch objects, then close the connection pool.

        Idempotent. Any tables or stages that client-side sources materialized
        are dropped first (best effort, see :meth:`drop_scratch`) while the pool
        is still usable.

        Warning:
            If any :meth:`~hologres.dataframe.dataframe.DataFrame.iter_rows`
            generator obtained from this session is still in progress, its
            connection has not yet been returned to the pool, and ``close`` will
            block waiting for it. Exhaust or ``del`` in-flight iterators before
            calling ``close`` (or leave it to the context-manager exit and let
            garbage collection drop live generators).
        """
        if not self._closed:
            self.drop_scratch()
            self._closed = True
            self._pool.close()

    def drop_scratch(self) -> None:
        """Drop the temporary tables and stages this session created.

        Client-side sources (``hg.from_pandas``, ``read_files``) may materialize
        data server-side; those objects are registered here and dropped
        automatically by :meth:`close`. Call this to release them earlier -- for
        example in a long-lived session that loads many frames.

        Failures are logged and skipped rather than raised: cleanup runs on a
        best-effort basis so one undroppable object cannot strand the rest.
        """
        force = (
            sql.SQL(" FORCE")
            if self._scratch_tables and self.server_version() >= (3, 1, 0)
            else sql.SQL("")
        )
        remaining_tables = []
        for table_name in self._scratch_tables:
            try:
                self.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}{}").format(
                        table_name.full_name, force
                    ),
                    fetch=False,
                )
            except Exception as exc:
                logger.warning("failed to drop scratch table %s: %s", table_name, exc)
                remaining_tables.append(table_name)
        self._scratch_tables = remaining_tables

        remaining_stages = []
        for stage in self._stages:
            try:
                self.execute(
                    sql.SQL("call hologres.hg_drop_internal_stage({})").format(
                        sql.Literal(stage)
                    ),
                    fetch=False,
                )
            except Exception as exc:
                logger.warning("failed to drop stage %s: %s", stage, exc)
                remaining_stages.append(stage)
        self._stages = remaining_stages

    def _register_scratch_table(self, table_name) -> None:
        """Record a table to drop when this session is cleaned up."""
        self._scratch_tables.append(table_name)

    def _register_stage(self, stage: str) -> None:
        """Record an internal stage to drop when this session is cleaned up."""
        self._stages.append(stage)

    def _fixed_fe_connection(self):
        """Open a FixedFE connection, needed to upload files to a stage.

        ``COPY external_files(...) FROM STDIN`` is only accepted on FixedFE, so
        it cannot go through this session's regular pool. FixedFE is selected by
        adding libpq's ``options=type=fixed`` to the same endpoint.

        Raises:
            InvalidArgumentError: If this session was not created via
                :func:`connect` and so has no conninfo to derive from.
        """
        import psycopg

        if self._conninfo is None:
            raise InvalidArgumentError(
                "a FixedFE connection requires a session created via hg.connect(...)"
            )
        return psycopg.connect(f"{self._conninfo} options=type=fixed", autocommit=True)

    def __enter__(self) -> Session:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @property
    def closed(self) -> bool:
        """Whether the session has been closed."""
        return self._closed

    def _check_state(self) -> None:
        if self._closed:
            raise InvalidArgumentError("Session is closed")

    # -- low-level execution -----------------------------------------------

    def execute(
        self,
        query: sql.Composable,
        *,
        fetch: bool = True,
        _execution_options=None,
    ) -> tuple[list[Row], StructType]:
        """Execute a query and return rows + schema.

        Args:
            query: A :class:`psycopg.sql.Composable` (a ``Composed``, ``SQL``,
                ``Identifier``, ``Literal``, or plain string).
            fetch: If ``True``, fetch all rows and return them; if ``False``,
                run the statement for side effects only (rows will be empty).

        Returns:
            ``(rows, schema)``. When ``fetch`` is ``False`` or the cursor has no
            description (e.g. DDL/DML), the schema is empty.

        Raises:
            InvalidArgumentError: If the session is closed.
            ExecutionError: If PostgreSQL rejects the statement.
        """
        self._check_state()
        try:
            with self._pool.connection() as conn:
                if _execution_options is None:
                    return self._execute_on_connection(conn, query, fetch=fetch)
                with conn.transaction():
                    with conn.cursor() as cur:
                        self._apply_schema(cur, local=True)
                        self._apply_execution_options(cur, _execution_options)
                        return self._execute_on_cursor(cur, query, fetch=fetch)
        except UnsupportedOperationError:
            raise
        except Exception as exc:
            raise self._wrap(exc)

    def _execute_on_connection(self, conn, query, *, fetch):
        with conn.cursor() as cur:
            self._apply_schema(cur)
            try:
                return Session._execute_on_cursor(cur, query, fetch=fetch)
            finally:
                self._reset_schema(cur)

    @staticmethod
    def _execute_on_cursor(cur, query, *, fetch):
        cur.execute(query)
        if fetch and cur.description is not None:
            schema = _description_to_struct(cur.description)
            raw_rows = cur.fetchall()
            rows = [Row(schema.names, tuple(r)) for r in raw_rows]
            return rows, schema
        return [], StructType()

    def _apply_execution_options(self, cur, options) -> None:
        for statement in options.set_local_statements():
            try:
                cur.execute(statement)
            except Exception as exc:
                raise self._unsupported(
                    "serverless computing",
                    "DataFrame.serverless()",
                    f"the server rejected {statement.as_string()!r}: {exc}",
                ) from exc

    def _apply_schema(self, cur, *, local: bool = False) -> None:
        if self._schema_name is None:
            return
        scope = sql.SQL(" LOCAL") if local else sql.SQL("")
        cur.execute(
            sql.SQL("SET{} search_path TO {}").format(
                scope, sql.Identifier(self._schema_name)
            )
        )

    def _reset_schema(self, cur) -> None:
        if self._schema_name is not None:
            cur.execute(sql.SQL("RESET search_path"))

    def _ensure_remote_functions(self, plan) -> None:
        """Deploy and register remote functions referenced by a logical plan."""
        from ._fc import (
            DefaultFCDeployer,
            build_drop_function_sql,
            build_registration_sql,
            iter_remote_definitions,
            requires_function_recreate,
        )

        definitions = list(iter_remote_definitions(plan))
        if not definitions:
            return
        if self._fc_config is None and self._fc_deployer is None:
            raise InvalidArgumentError(
                "a DataFrame using @hg.udf/@hg.udtf requires "
                "fc_config=... on hg.connect(...)"
            )
        self._require_capability(
            "function_compute", operation="remote UDF/UDTF execution"
        )
        if self._fc_deployer is None:
            self._fc_deployer = DefaultFCDeployer(self._fc_config)
        for definition in definitions:
            key = (
                definition.sql_name,
                tuple(datatype.sql_name for datatype in definition.input_types),
            )
            fingerprint = definition.fingerprint
            previous_entry = self._deployed_remote_functions.get(key)
            previous_definition = None
            if previous_entry is not None:
                previous_fingerprint, previous_definition = previous_entry
                if previous_fingerprint == fingerprint:
                    continue
                if not definition.replace:
                    raise InvalidArgumentError(
                        f"remote function {definition.sql_name!r} was defined with "
                        "different metadata in the same Session and replace=False"
                    )
            endpoint_path = self._fc_deployer.deploy(definition)
            self.execute(
                sql.SQL("CREATE EXTENSION IF NOT EXISTS function_compute"),
                fetch=False,
            )
            registration = build_registration_sql(definition, endpoint_path)
            if previous_definition is not None and requires_function_recreate(
                previous_definition, definition
            ):
                self._execute_write_transaction(
                    [build_drop_function_sql(definition), registration]
                )
            else:
                self.execute(registration, fetch=False)
            self._deployed_remote_functions[key] = (fingerprint, definition)

    def _ensure_plan_capabilities(self, plan) -> None:
        """Validate server capabilities referenced by a logical plan."""
        from ._capabilities import iter_plan_requirements

        for kind, name in iter_plan_requirements(plan):
            if kind == "capability":
                self._require_capability(name)
            else:
                self._require_sql_function(name)

    def _require_capability(self, name: str, *, operation: str | None = None) -> None:
        """Require an engine capability before submitting dependent SQL."""
        key = ("capability", name)
        if self._capability_cache.get(key):
            return
        from ._capabilities import (
            BINLOG_GAP_GUC,
            DYNAMIC_TABLE,
            EXTERNAL_FILES,
            FUNCTION_COMPUTE,
            MIN_VERSIONS,
            OBJECT_TABLE,
            VECTOR_TYPE,
        )

        minimum = MIN_VERSIONS.get(name)
        # The Dynamic Table GUC is an authoritative capability probe and may
        # exist on a backported build whose version predates the baseline.
        if name == DYNAMIC_TABLE:
            minimum = None
        if minimum is not None and self.server_version() < minimum:
            raise self._unsupported(
                name,
                operation,
                f"requires Hologres {minimum[0]}.{minimum[1]}.{minimum[2]} or newer",
            )

        if name in {EXTERNAL_FILES, OBJECT_TABLE}:
            # These have no stronger probe than their known version baseline.
            pass
        elif name == DYNAMIC_TABLE:
            value = self._probe_setting(
                "hg_experimental_use_driver_based_interface_for_non_partition_dynamic_table"
            )
            if str(value).lower() not in {"on", "true", "1"}:
                raise self._unsupported(
                    name,
                    operation,
                    "the driver-based Dynamic Table interface is unavailable or "
                    "disabled",
                )
        elif name == FUNCTION_COMPUTE:
            rows, _ = self.execute(
                sql.SQL(
                    "SELECT EXISTS (SELECT 1 FROM pg_available_extensions "
                    "WHERE name = 'function_compute')"
                )
            )
            if not rows or not rows[0][0]:
                raise self._unsupported(
                    name, operation, "the function_compute extension is unavailable"
                )
        elif name == VECTOR_TYPE:
            rows, _ = self.execute(sql.SQL("SELECT to_regtype('vector') IS NOT NULL"))
            if not rows or not rows[0][0]:
                raise self._unsupported(
                    name, operation, "the pgvector type is unavailable"
                )
        elif name == BINLOG_GAP_GUC:
            value = self._probe_setting("hg_experimental_generate_binlog")
            if value is None:
                raise self._unsupported(
                    name,
                    operation,
                    "the hg_experimental_generate_binlog setting is unavailable",
                )
        else:
            raise InvalidArgumentError(f"unknown internal capability {name!r}")
        self._capability_cache[key] = True

    def _require_sql_function(self, name: str, *, operation: str | None = None) -> None:
        """Require a named built-in SQL function before plan execution."""
        key = ("sql_function", name)
        if self._capability_cache.get(key):
            return
        rows, _ = self.execute(
            sql.SQL("SELECT EXISTS (SELECT 1 FROM pg_proc WHERE proname = {})").format(
                sql.Literal(name)
            )
        )
        if not rows or not rows[0][0]:
            raise self._unsupported(
                f"SQL function {name}",
                operation,
                "the function is not available on this Hologres server",
            )
        self._capability_cache[key] = True

    def _probe_setting(self, name: str):
        try:
            rows, _ = self.execute(
                sql.SQL("SELECT current_setting({}, true)").format(sql.Literal(name))
            )
        except ExecutionError as exc:
            raise self._unsupported(
                name, None, "the server capability probe failed"
            ) from exc
        return rows[0][0] if rows else None

    @staticmethod
    def _unsupported(name: str, operation: str | None, detail: str):
        subject = operation or name
        return UnsupportedOperationError(f"{subject} is unsupported: {detail}")

    def _execute_write_transaction(
        self,
        statements,
        *,
        prelude=(),
        cleanup=(),
        allow_binlog_gap: bool = False,
        execution_options=None,
    ) -> None:
        """Run write statements on one connection and transaction.

        This is internal support for DataFrame writes. It exists because a
        pooled ``Session.execute`` call can borrow a different connection for
        each statement, which would break low-version overwrite transactions
        and a connection-local Binlog GUC.
        """
        self._check_state()
        try:
            with self._pool.connection() as conn:
                try:
                    if execution_options is None:
                        with conn.cursor() as cur:
                            self._apply_schema(cur)
                            for statement in prelude:
                                cur.execute(statement)
                        with conn.transaction():
                            with conn.cursor() as cur:
                                self._apply_schema(cur, local=True)
                                if allow_binlog_gap:
                                    cur.execute(
                                        sql.SQL(
                                            "SET LOCAL "
                                            "hg_experimental_generate_binlog = off"
                                        )
                                    )
                                for statement in statements:
                                    cur.execute(statement)
                    else:
                        with conn.transaction():
                            with conn.cursor() as cur:
                                self._apply_execution_options(cur, execution_options)
                                self._apply_schema(cur, local=True)
                                for statement in prelude:
                                    cur.execute(statement)
                                if allow_binlog_gap:
                                    cur.execute(
                                        sql.SQL(
                                            "SET LOCAL "
                                            "hg_experimental_generate_binlog = off"
                                        )
                                    )
                                for statement in statements:
                                    cur.execute(statement)
                finally:
                    with conn.cursor() as cur:
                        self._apply_schema(cur)
                        try:
                            for statement in cleanup:
                                cur.execute(statement)
                        finally:
                            self._reset_schema(cur)
        except UnsupportedOperationError:
            raise
        except Exception as exc:
            raise self._wrap(exc)

    def execute_stream(
        self,
        query: sql.Composable,
        *,
        batch_size: int = 1000,
        _execution_options=None,
    ) -> Iterator[Row]:
        """Execute a query and stream rows back in batches.

        Rows are pulled with ``fetchmany(batch_size)`` so the caller never holds
        the whole result set, only one batch at a time. Callers iterate the
        returned generator; the connection returns to the pool when it is
        exhausted or garbage-collected.

        Note:
            A server-side named cursor would stream more aggressively, but
            Hologres only allows ``DECLARE CURSOR`` inside a transaction block
            and this session's connections are autocommit, so client-side
            batching is what actually works here.

        Args:
            query: A :class:`psycopg.sql.Composable`.
            batch_size: Rows to fetch per round trip. Must be positive.

        Yields:
            :class:`~hologres.dataframe.row.Row` instances, one per row.

        Raises:
            InvalidArgumentError: If the session is closed or ``batch_size`` is
                not a positive int.
            ExecutionError: If PostgreSQL rejects the statement.
        """
        # Check state eagerly. If we did it in the generator body a closed
        # session would only fail on first next(), which is confusing.
        self._check_state()
        if not isinstance(batch_size, int) or isinstance(batch_size, bool):
            raise InvalidArgumentError(
                f"batch_size must be an int, got {type(batch_size).__name__}"
            )
        if batch_size <= 0:
            raise InvalidArgumentError(f"batch_size must be positive, got {batch_size}")
        return self._execute_stream_impl(query, batch_size, _execution_options)

    def _execute_stream_impl(
        self, query: sql.Composable, batch_size: int, execution_options
    ) -> Iterator[Row]:
        try:
            with self._pool.connection() as conn:
                if execution_options is None:
                    yield from self._stream_on_connection(conn, query, batch_size)
                    return
                with conn.transaction():
                    with conn.cursor() as cur:
                        self._apply_schema(cur, local=True)
                        self._apply_execution_options(cur, execution_options)
                        yield from self._stream_on_cursor(cur, query, batch_size)
        except UnsupportedOperationError:
            raise
        except Exception as exc:
            raise self._wrap(exc)

    def _stream_on_connection(self, conn, query, batch_size):
        with conn.cursor() as cur:
            self._apply_schema(cur)
            try:
                yield from Session._stream_on_cursor(cur, query, batch_size)
            finally:
                self._reset_schema(cur)

    @staticmethod
    def _stream_on_cursor(cur, query, batch_size):
        cur.execute(query)
        schema = _description_to_struct(cur.description)
        while True:
            batch = cur.fetchmany(batch_size)
            if not batch:
                return
            for row in batch:
                yield Row(schema.names, tuple(row))

    def describe(self, query: sql.Composable) -> StructType:
        """Return the result schema of ``query`` without fetching rows.

        Implemented by running the query with a false ``WHERE`` (``LIMIT 0``
        wrapper) and reading ``cursor.description``. Used by :attr:`DataFrame.schema`
        for DataFrames built from ``session.sql(...)`` where no schema is pinned.

        Args:
            query: A ``Composable`` producing a ``SELECT``.

        Returns:
            The result :class:`~hologres.dataframe.types.StructType`.

        Raises:
            ExecutionError: If PostgreSQL rejects the wrapped statement.
        """
        wrapped = sql.SQL("SELECT * FROM ({}) __desc__ LIMIT 0").format(query)
        _, schema = self.execute(wrapped)
        return schema

    # -- catalog / metadata -------------------------------------------------

    def table(self, name: str) -> DataFrame:
        """Reference an existing table by name, pinning its schema.

        A single ``information_schema.columns`` lookup fetches the table's
        column list and types, which is stored on the returned DataFrame so
        subsequent ``df.schema``, ``df.columns``, ``drop``, ``rename``,
        ``fillna`` calls do not repeat the round-trip.

        Args:
            name: The table identifier, either ``"table"`` or
                ``"schema.table"`` (quoted parts allowed).

        Returns:
            A new :class:`~hologres.dataframe.dataframe.DataFrame` sourced from
            the table.

        Raises:
            InvalidArgumentError: If ``name`` is not a valid identifier.
            ExecutionError: If the table does not exist or the lookup fails.
        """
        from ._plan import TableScan
        from .dataframe import DataFrame

        table_name = _parse_table_name(
            name, default_schema=self._schema_name or "public"
        )
        schema = self._fetch_table_schema(table_name.schema_name, table_name.table_name)
        return DataFrame(TableScan(table_name), session=self, schema=schema)

    def sql(self, query: str) -> DataFrame:
        """Build a DataFrame from a raw SQL string.

        The text is treated as an opaque subquery source; the schema is fetched
        lazily on first access to ``df.schema`` / ``df.columns`` via a
        ``LIMIT 0`` describe query. Callers are responsible for the SQL's
        correctness and safety.

        Args:
            query: The SQL text.

        Returns:
            A new :class:`~hologres.dataframe.dataframe.DataFrame`.
        """
        from ._plan import RawSql
        from .dataframe import DataFrame

        return DataFrame(RawSql(query), session=self)

    def from_dict(
        self,
        data: dict[str, list],
        *,
        schema: StructType | None = None,
        inline_limit: int = 10000,
    ) -> DataFrame:
        """Build a Session-bound DataFrame from column-oriented data.

        This is the Session-bound counterpart of :func:`hg.from_dict`. Keeping
        the Session on the returned DataFrame lets actions and ``write()`` use
        the same connection without another argument.
        """
        from ._sources import from_dict

        return from_dict(
            data,
            session=self,
            schema=schema,
            inline_limit=inline_limit,
        )

    def from_records(
        self,
        rows: list[dict] | list[tuple],
        *,
        columns: list[str] | None = None,
        schema: StructType | None = None,
        inline_limit: int = 10000,
    ) -> DataFrame:
        """Build a Session-bound DataFrame from row-oriented data."""
        from ._sources import from_records

        return from_records(
            rows,
            columns=columns,
            session=self,
            schema=schema,
            inline_limit=inline_limit,
        )

    def from_pandas(
        self,
        df,
        *,
        schema: StructType | None = None,
        table_name: str | None = None,
    ) -> DataFrame:
        """Build a Session-bound DataFrame from a pandas DataFrame."""
        from ._sources import from_pandas

        return from_pandas(
            df,
            session=self,
            schema=schema,
            table_name=table_name,
        )

    def object_table(self, name: str, *, refresh: bool = True):
        """Reference a Hologres **object table** mapping unstructured OSS files.

        An object table exposes the *metadata* of files in OSS as an ordinary
        relation (``object_uri``, ``etag``, ``file``, ``metadata``), so it is
        queried with plain SQL like any other table. The file bytes stay in OSS
        and are reached through Hologres AI functions.

        Because the metadata is a snapshot, this issues ``REFRESH OBJECT TABLE``
        first by default so newly uploaded files are visible.

        Args:
            name: The object table, either ``"table"`` or ``"schema.table"``.
            refresh: Run ``REFRESH OBJECT TABLE`` before returning. Pass
                ``False`` to skip the round trip when the metadata is already
                current (e.g. querying the same table repeatedly).

        Returns:
            A :class:`~hologres.dataframe.dataframe.DataFrame` with the object
            table's schema pinned.

        Raises:
            InvalidArgumentError: If ``name`` is not a valid identifier.
            ExecutionError: If the refresh fails or the table does not exist.

        Example:
            Read file metadata, digging into the ``file`` struct with ``->>``:

            >>> docs = session.object_table("public.pdfs")  # doctest: +SKIP
            >>> docs.select(  # doctest: +SKIP
            ...     "object_uri", hg.expr("file->>'size'").alias("size")
            ... ).show()

            Parsing the file contents needs a Hologres AI function, reached via
            :func:`hologres.dataframe.function` until they get dedicated
            wrappers::

                docs.select(hg.function("ai_parse_document", hg.col("file")))
        """
        from ._plan import TableScan
        from .dataframe import DataFrame

        table_name = _parse_table_name(name)
        self._require_object_table(table_name)
        if refresh:
            self.execute(
                sql.SQL("REFRESH OBJECT TABLE {}").format(table_name.full_name),
                fetch=False,
            )
        schema = self._fetch_table_schema(table_name.schema_name, table_name.table_name)
        return DataFrame(TableScan(table_name), session=self, schema=schema)

    def refresh_table(self, name: str) -> None:
        """Refresh a Hologres Dynamic Table immediately.

        Args:
            name: Dynamic Table name, optionally schema-qualified.

        Returns:
            ``None``.

        Raises:
            InvalidArgumentError: If ``name`` is invalid or the session is closed.
            ExecutionError: If Hologres rejects the refresh.

        Example:
            >>> session.refresh_table("public.dt_orders")  # doctest: +SKIP
        """
        if not isinstance(name, str) or not name:
            raise InvalidArgumentError("table name must be a non-empty string")
        table_name = _parse_table_name(name)
        self._require_dynamic_table(table_name)
        self.execute(
            sql.SQL("REFRESH TABLE {}").format(table_name.full_name), fetch=False
        )
        return None

    def _require_object_table(self, table_name) -> None:
        self._require_capability("object_table", operation="object_table")
        rows, _ = self.execute(
            sql.SQL(
                "SELECT EXISTS ("
                "SELECT 1 FROM hologres.hg_table_properties "
                "WHERE table_namespace = {schema} AND table_name = {table} "
                "AND property_key = 'is_object_table' "
                "AND lower(property_value) = 'true')"
            ).format(
                schema=sql.Literal(table_name.schema_name),
                table=sql.Literal(table_name.table_name),
            )
        )
        if not rows or not rows[0][0]:
            raise UnsupportedOperationError(
                f"object_table requires an existing Hologres Object Table, got "
                f"{table_name.schema_name}.{table_name.table_name}"
            )

    def _require_dynamic_table(self, table_name) -> None:
        self._require_capability("dynamic_table", operation="refresh_table")
        rows, _ = self.execute(
            sql.SQL(
                "SELECT EXISTS ("
                "SELECT 1 FROM hologres.hg_dynamic_table_properties "
                "WHERE dynamic_table_namespace = {schema} "
                "AND dynamic_table_name = {table})"
            ).format(
                schema=sql.Literal(table_name.schema_name),
                table=sql.Literal(table_name.table_name),
            )
        )
        if not rows or not rows[0][0]:
            raise UnsupportedOperationError(
                f"refresh_table requires an existing Hologres Dynamic Table, got "
                f"{table_name.schema_name}.{table_name.table_name}"
            )

    def read_files(
        self,
        path,
        *,
        format: str | None = None,
        schema: StructType | None = None,
        table_name: str | None = None,
        oss_endpoint: str | None = None,
        role_arn: str | None = None,
        **options: Any,
    ):
        """Read local or OSS files into a DataFrame.

        Local files (csv / json / parquet) are parsed with pyarrow and uploaded,
        taking the same route as :func:`hologres.dataframe.from_pandas`: an
        internal stage on Hologres 4.1+, otherwise a table. An ``oss://`` path
        stays on the server and is read lazily through ``EXTERNAL_FILES``.

        Local files are read when this is called, which makes ``df.schema``
        free; their **upload** is deferred to the first action and happens once.
        OSS paths are not accessed until schema resolution or an action runs.

        Args:
            path: A local file path, a glob such as ``"data/*.csv"``, a list of
                local paths/globs, or one ``oss://`` URI. Local files must share
                one schema; OSS paths cannot be mixed into a list.
            format: For local files, ``"csv"``, ``"json"``, or ``"parquet"``
                and optional when the extension identifies it. For OSS,
                explicitly pass ``"csv"``, ``"parquet"``, or ``"orc"``.
            schema: Explicit column names and types. For local files this
                relabels the Arrow data; for OSS it emits the
                ``EXTERNAL_FILES AS (...)`` declaration. ``None`` asks Hologres
                to infer an OSS schema lazily.
            table_name: For local files only, materialize into this table rather
                than an auto-named one; naming it opts out of automatic cleanup.
            oss_endpoint: OSS endpoint for an ``oss://`` path.
            role_arn: RAM role Hologres assumes to access the OSS path.
            **options: Reader options forwarded to pyarrow, e.g. ``delimiter``,
                ``quote_char``, ``skip_rows``, ``null_values`` for csv, or
                ``columns`` for parquet. For OSS, options are
                ``csv_skip_header``, ``csv_delimiter``,
                ``schema_deduce_file_num`` and ``schema_deduce_file_order``.
                An unrecognized name is an error rather than being ignored.

        Returns:
            A lazy :class:`~hologres.dataframe.dataframe.DataFrame`.

        Raises:
            InvalidArgumentError: If no file matches, the format cannot be
                determined, an option is unknown, the files disagree on their
                schema, the data has no rows, or OSS and local-only arguments
                are mixed.
            ImportError: If pyarrow is needed for a local path but not installed.

        Example:
            >>> df = session.read_files("data/*.csv")  # doctest: +SKIP
            >>> df.group_by("region").count().show()  # uploads here  # doctest: +SKIP
            >>> oss = session.read_files(  # doctest: +SKIP
            ...     "oss://bucket/data/", format="parquet"
            ... )
            >>> oss.filter(hg.col("id") > 0).show()  # doctest: +SKIP
        """
        is_oss = isinstance(path, str) and path.startswith("oss://")
        if isinstance(path, (list, tuple)) and any(
            isinstance(item, str) and item.startswith("oss://") for item in path
        ):
            raise InvalidArgumentError(
                "OSS read_files path must be one oss:// string, not a list"
            )
        if is_oss:
            if table_name is not None:
                raise InvalidArgumentError(
                    "table_name is only supported for local file materialization"
                )
            from ._external_files import validate_oss_read
            from ._plan import ExternalFilesScan
            from .dataframe import DataFrame

            normalized_path, file_format, file_schema, external_options = (
                validate_oss_read(
                    path,
                    format,
                    schema,
                    oss_endpoint=oss_endpoint,
                    role_arn=role_arn,
                    options=options,
                )
            )
            return DataFrame(
                ExternalFilesScan(
                    normalized_path,
                    file_format,
                    file_schema,
                    external_options,
                ),
                session=self,
                schema=file_schema,
            )

        if oss_endpoint is not None or role_arn is not None:
            raise InvalidArgumentError(
                "oss_endpoint and role_arn require an oss:// path"
            )

        from ._files import read_local_files
        from ._sources import from_arrow

        table, _fmt = read_local_files(path, format, **options)
        return from_arrow(
            table,
            session=self,
            schema=schema,
            table_name=table_name,
            origin="read_files",
        )

    def _fetch_table_schema(self, schema_name: str, table_name: str) -> StructType:
        """Query ``information_schema.columns`` for one table's schema."""
        query = sql.SQL(
            "SELECT column_name, data_type, udt_name "
            "FROM information_schema.columns "
            "WHERE table_schema = {schema} AND table_name = {table} "
            "ORDER BY ordinal_position"
        ).format(schema=sql.Literal(schema_name), table=sql.Literal(table_name))
        rows, _ = self.execute(query)
        if not rows:
            raise ExecutionError(
                f"table {schema_name!r}.{table_name!r} does not exist or has no columns"
            )
        fields = []
        for r in rows:
            col_name, data_type, udt_name = r[0], r[1], r[2]
            # information_schema uses "ARRAY" for arrays and reports the element
            # type via udt_name (which begins with "_"). Otherwise data_type is
            # usable directly ("integer", "bigint", "character varying", ...).
            if data_type == "ARRAY" and udt_name.startswith("_"):
                type_name = f"{udt_name[1:]}[]"
            else:
                type_name = udt_name or data_type
            try:
                dt = parse_type(type_name)
            except InvalidArgumentError:
                # Unknown type -> approximate as text so the caller can still work.
                dt = parse_type("text")
            fields.append(StructField(col_name, dt))
        return StructType(fields)

    def use_schema(self, name: str) -> None:
        """Set the active schema for every connection in this Session.

        Args:
            name: A schema name; used as a SQL identifier.

        Raises:
            InvalidArgumentError: If the session is closed or ``name`` is empty.
            ExecutionError: If the statement fails.
        """
        if not isinstance(name, str) or not name:
            raise InvalidArgumentError("schema name must be a non-empty string")
        self._check_state()
        self._schema_name = name
        # Function/type visibility can depend on search_path.
        self._capability_cache.clear()
        self._deployed_remote_functions.clear()

    def use_database(self, name: str) -> None:
        """Switch this session to another database by reconnecting in place.

        PostgreSQL binds ``dbname`` when a connection opens, so every connection
        in the pool points at the current database. Switching therefore rebuilds
        the pool: a new pool bound to ``name`` is opened first, and only once it
        succeeds is the old pool swapped out and closed. On failure the old pool
        is left untouched.

        The swap is in place -- every
        :class:`~hologres.dataframe.dataframe.DataFrame` derived from this
        session now runs against ``name``. Schemas pinned from the previous
        database may no longer be valid, so re-fetch them via
        :meth:`table`. To switch **schemas** within the same database use the
        much cheaper :meth:`use_schema`.

        Only this session is affected: each :func:`connect` owns its own pool,
        never shared with another session.

        Args:
            name: The database to switch to.

        Raises:
            InvalidArgumentError: If the session is closed, ``name`` is not a
                non-empty string, or this session was not created via
                :func:`connect` (and so cannot reconnect).
            ExecutionError: If the new pool cannot be opened; the session keeps
                using the previous database.
        """
        self._check_state()
        if not isinstance(name, str) or not name:
            raise InvalidArgumentError("database name must be a non-empty string")
        if self._pool_factory is None:
            raise InvalidArgumentError(
                "use_database requires a session created via hg.connect(...)"
            )
        if name == self._dbname:
            return
        self.drop_scratch()
        if self._scratch_tables or self._stages:
            raise ExecutionError(
                "cannot switch database while session scratch objects could not "
                "be cleaned up"
            )
        try:
            new_pool = self._pool_factory(name)
        except Exception as exc:
            raise self._wrap(exc)
        old_pool = self._pool
        self._pool = new_pool
        self._dbname = name
        if self._conninfo is not None:
            self._conninfo = conninfo.make_conninfo(self._conninfo, dbname=name)
        self._schema_name = None
        # The new pool may reach a different instance; re-probe on demand.
        self._server_version = None
        self._capability_cache.clear()
        self._deployed_remote_functions.clear()
        old_pool.close()

    def current_schema(self) -> str:
        """Return the current schema (``SELECT current_schema()``)."""
        rows, _ = self.execute(sql.SQL("SELECT current_schema()"))
        return rows[0][0] if rows else self._schema_name or "public"

    def current_database(self) -> str:
        """Return the current database (``SELECT current_database()``)."""
        rows, _ = self.execute(sql.SQL("SELECT current_database()"))
        return rows[0][0] if rows else self._dbname

    def server_version(self) -> tuple[int, int, int]:
        """Return the Hologres server version as ``(major, minor, patch)``.

        Read once via ``SELECT hg_version()`` and cached for the life of the
        pool (a server does not change version under a live connection);
        :meth:`use_database` clears the cache since it reconnects.

        Feature gating uses this: the Arrow/stage write path needs Hologres 4.1
        or newer, so ``session.server_version() >= (4, 1, 0)`` decides which
        channel a client-side source materializes through.

        Returns:
            The parsed version, or ``(0, 0, 0)`` when it cannot be determined
            (a non-Hologres PostgreSQL has no ``hg_version()``). Callers should
            treat ``(0, 0, 0)`` as "assume the oldest behavior".

        Example:
            >>> session.server_version() >= (4, 1, 0)  # doctest: +SKIP
            True
        """
        if self._server_version is not None:
            return self._server_version
        try:
            rows, _ = self.execute(sql.SQL("SELECT hg_version()"))
        except ExecutionError:
            # A transient failure must not poison the session with an old
            # version. Return the conservative value for this call, but retry
            # the probe on the next call. Destructive callers reject this
            # unknown value instead of selecting a fallback write path.
            return (0, 0, 0)
        match = _VERSION_RE.search(rows[0][0]) if rows else None
        if match is None:
            return (0, 0, 0)
        self._server_version = (
            int(match.group(1)),
            int(match.group(2)),
            int(match.group(3) or 0),
        )
        return self._server_version

    def list_schemas(self) -> list[str]:
        """List all schema names visible to the current user."""
        rows, _ = self.execute(
            sql.SQL(
                "SELECT schema_name FROM information_schema.schemata "
                "ORDER BY schema_name"
            )
        )
        return [r[0] for r in rows]

    def list_tables(self, schema_name: str | None = None) -> list[str]:
        """List table names in a schema (defaults to the current one).

        Args:
            schema_name: Explicit schema name, or ``None`` to use the current
                one (from ``current_schema()``).

        Returns:
            Table names, sorted alphabetically.
        """
        if schema_name is None:
            schema_name = self.current_schema()
        rows, _ = self.execute(
            sql.SQL(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = {} "
                "ORDER BY table_name"
            ).format(sql.Literal(schema_name))
        )
        return [r[0] for r in rows]

    # -- error wrapping -----------------------------------------------------

    @staticmethod
    def _wrap(exc: BaseException) -> ExecutionError:
        """Convert any driver/library exception to :class:`ExecutionError`.

        We consult ``hologres.HoloClientException.from_pg_error`` when the
        exception is a psycopg error, so the standard Hologres classification
        (auth/permission/timeout/...) applies. Everything else is wrapped as a
        generic execution failure.
        """
        try:
            import psycopg
            from hologres.exceptions import HoloClientException

            if isinstance(exc, psycopg.Error):
                classified = HoloClientException.from_pg_error(exc)
                return ExecutionError.from_holo_client_exception(classified)
        except ImportError:  # pragma: no cover - hologres-client is required
            pass
        return ExecutionError(str(exc), cause=exc)

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        return f"Session(dbname={self._dbname!r}, {state})"


# ------------------------------------------------------------------------
# hg.connect entry point
# ------------------------------------------------------------------------


def connect(
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: str,
    *,
    min_size: int = 1,
    max_size: int = 10,
    timeout: float = 30.0,
    max_idle: float = 600.0,
    max_lifetime: float = 3600.0,
    application_name: str = "hologres-dataframe",
    fc_config: dict[str, Any] | None = None,
    **extra: Any,
) -> Session:
    """Open a :class:`Session` backed by a ``psycopg_pool`` connection pool.

    This is the one entry point for using ``hologres.dataframe`` against a live
    Hologres instance. Prefer using it as a context manager so the pool is
    closed even on error::

        with hg.connect(host="h", port=80, dbname="d", user="u", password="p") as s:
            s.table("public.orders").filter(hg.col("amount") > 100).show()

    Args:
        host: Hologres endpoint hostname.
        port: TCP port; Hologres public endpoints commonly use ``80``.
        dbname: Database name. Hologres/PostgreSQL bind ``dbname`` at connect
            time, so switching databases means rebuilding the Session (see
            :meth:`Session.use_schema` for switching **schemas** within a
            database, which is cheap).
        user: Username (Alibaba Cloud AccessKey ID for cloud instances).
        password: Password (AccessKey Secret for cloud instances).
        min_size: Minimum pool size. Connections are opened eagerly to this
            level so the first query is not slowed by handshake latency.
        max_size: Maximum pool size, i.e. the concurrency ceiling.
        timeout: Seconds to wait for an available connection before raising.
        max_idle: Seconds a pooled connection may stay idle before recycling.
        max_lifetime: Seconds a pooled connection may live before recycling
            (defends against stale server-side state).
        application_name: Value for the ``application_name`` connection option,
            visible in ``pg_stat_activity``. Defaults to ``"hologres-dataframe"``.
        fc_config: Optional Function Compute deployment configuration. It must
            include ``endpoint`` and ``region``; credentials are resolved via
            the standard Alibaba Cloud credential chain when a remote UDF or
            UDTF is first used.
        **extra: Any additional psycopg / libpq connection parameter, passed
            through verbatim to the underlying ``psycopg`` driver. Common ones:

            * **TLS**: ``sslmode`` (``disable``/``allow``/``prefer``/``require``/
              ``verify-ca``/``verify-full``), ``sslrootcert``, ``sslcert``,
              ``sslkey``.
            * **Network**: ``connect_timeout`` (seconds until the TCP handshake
              gives up), ``keepalives``, ``keepalives_idle``, ``tcp_user_timeout``.
            * **Server-side GUCs at connect time**: ``options`` uses libpq's
              command-line syntax ``-c key=value`` (space-separated for
              multiple settings), e.g. ``options="-c statement_timeout=5s
              -c search_path=analytics,public"``. These are applied on every
              new connection the pool opens.

            The full list is `psycopg's connection parameters
            <https://www.psycopg.org/psycopg3/docs/api/connections.html#psycopg.Connection.connect>`_,
            which forward to libpq's parameter keywords.

    Returns:
        A live :class:`Session`.

    Raises:
        InvalidArgumentError: If required arguments are missing or malformed.
        ExecutionError: If the pool cannot open a connection.

    Note:
        To change a server-side GUC *after* connecting (or per-query), run a
        ``SET`` statement through the session instead of using ``options``::

            from psycopg import sql
            session.execute(
                sql.SQL("SET statement_timeout = {}").format(sql.Literal("10s")),
                fetch=False,
            )

        ``SET`` inside a pooled connection is scoped to that connection only,
        so prefer ``options=...`` when you want the setting on every future
        connection the pool opens.

    Example:
        Basic connect with pool sizing::

            >>> import hologres.dataframe as hg  # doctest: +SKIP
            >>> session = hg.connect(host="h", port=80, dbname="d",  # doctest: +SKIP
            ...                      user="u", password="p", max_size=20)

        Force TLS::

            >>> session = hg.connect(host="h", port=80, dbname="d",  # doctest: +SKIP
            ...                      user="u", password="p",
            ...                      sslmode="require")

        Preset server-side GUCs on every pooled connection::

            >>> session = hg.connect(  # doctest: +SKIP
            ...     host="h", port=80, dbname="d", user="u", password="p",
            ...     options="-c statement_timeout=5s -c search_path=analytics,public",
            ... )
    """
    from ._fc import validate_fc_config

    normalized_fc_config = validate_fc_config(fc_config)
    for name, value in (
        ("host", host),
        ("dbname", dbname),
        ("user", user),
        ("password", password),
    ):
        if not isinstance(value, str) or not value:
            raise InvalidArgumentError(f"{name} must be a non-empty string")
    if not isinstance(port, int) or isinstance(port, bool) or port <= 0:
        raise InvalidArgumentError(f"port must be a positive int, got {port!r}")

    built_conninfo = {}

    def make_pool(db: str) -> ConnectionPool:
        """Build a pool bound to ``db``; reused verbatim by use_database."""
        conninfo_str = conninfo.make_conninfo(
            host=host,
            port=port,
            dbname=db,
            user=user,
            password=password,
            application_name=application_name,
            **extra,
        )
        built_conninfo[db] = conninfo_str
        return ConnectionPool(
            conninfo=conninfo_str,
            min_size=min_size,
            max_size=max_size,
            timeout=timeout,
            max_idle=max_idle,
            max_lifetime=max_lifetime,
            kwargs={"autocommit": True},
            open=True,
        )

    pool = make_pool(dbname)
    return Session(
        pool,
        dbname=dbname,
        pool_factory=make_pool,
        conninfo=built_conninfo[dbname],
        fc_config=normalized_fc_config,
    )


def _session_from_connection(
    conn: Connection,
    dbname: str = "test",
    *,
    fc_config=None,
    fc_deployer=None,
) -> Session:
    """Testing helper: build a Session that reuses a single existing connection.

    Not part of the public API. Used by the test suite to inject a
    ``FakeConnection`` without actually starting a pool.
    """
    pool = _SingleConnectionPool(conn)
    return Session(
        pool,
        dbname=dbname,
        fc_config=fc_config,
        fc_deployer=fc_deployer,
    )


class _SingleConnectionPool:
    """A minimal duck-typed ``ConnectionPool`` that always yields the same conn.

    Used only by tests; production code goes through ``psycopg_pool.ConnectionPool``.
    """

    __slots__ = ("_conn", "_closed")

    def __init__(self, conn) -> None:
        self._conn = conn
        self._closed = False

    def connection(self):  # noqa: D401
        conn = self._conn

        class _Ctx:
            def __enter__(self_inner):
                return conn

            def __exit__(self_inner, *exc_info):
                return False

        return _Ctx()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            close = getattr(self._conn, "close", None)
            if callable(close):
                close()
