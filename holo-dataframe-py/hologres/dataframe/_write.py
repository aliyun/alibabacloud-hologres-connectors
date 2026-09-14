"""Write a DataFrame result into an existing Hologres table."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from hologres.table_name import TableName
from psycopg import sql

from . import _plan
from .exceptions import ExecutionError, InvalidArgumentError, UnsupportedOperationError

__all__ = ["write_dataframe"]

_OVERWRITE_MIN_VERSION = (3, 1, 0)


@dataclass(frozen=True)
class _TargetMetadata:
    table_name: TableName
    schema: Any
    primary_keys: tuple[str, ...] = ()
    logical_partition_keys: tuple[str, ...] = ()
    physical_partition_parent: bool = False
    binlog_enabled: bool | None = None


def write_dataframe(
    dataframe,
    table: str,
    *,
    overwrite: bool = False,
    partition: dict[str, Any] | None = None,
    on_conflict: tuple[str, list[str]] | None = None,
    allow_binlog_gap: bool = False,
) -> None:
    """Execute a DataFrame write using Hologres SQL INSERT semantics."""
    session = dataframe._session
    if session is None:
        raise InvalidArgumentError(
            "this DataFrame has no session; obtain it from "
            "session.table() or session.sql() first"
        )
    if not isinstance(table, str) or not table:
        raise InvalidArgumentError("table must be a non-empty string")
    if not isinstance(overwrite, bool):
        raise InvalidArgumentError("overwrite must be a bool")
    if not isinstance(allow_binlog_gap, bool):
        raise InvalidArgumentError("allow_binlog_gap must be a bool")
    if allow_binlog_gap and not overwrite:
        raise InvalidArgumentError(
            "allow_binlog_gap=True is only valid with overwrite=True"
        )
    if overwrite and on_conflict is not None:
        raise InvalidArgumentError("overwrite and on_conflict are mutually exclusive")

    table_name = _parse_table_name(table)
    action, conflict_keys = _parse_on_conflict(on_conflict)
    target = _load_target_metadata(
        session,
        table_name,
        need_primary_keys=conflict_keys is not None,
        need_partition=overwrite or partition is not None,
        need_binlog=overwrite and not allow_binlog_gap,
    )
    source_schema = dataframe.schema
    source_columns = list(source_schema.names)
    _validate_columns(source_columns, target.schema.names)
    partition_values = _validate_partition(
        partition,
        target,
        source_columns,
        overwrite=overwrite,
    )
    _validate_conflict(
        action, conflict_keys, target.primary_keys, source_columns=source_columns
    )
    if allow_binlog_gap:
        require_capability = getattr(session, "_require_capability", None)
        if require_capability is not None:
            require_capability(
                "binlog_gap_guc", operation="write(allow_binlog_gap=True)"
            )

    # Compile before destructive low-version operations. This also resolves a
    # PendingSource exactly once, so stage/scratch materialization happens first.
    source_query = dataframe._prepare_sql()
    source_query = _restrict_source_to_partition(
        source_query, source_columns, partition_values, low_version=False
    )

    if overwrite:
        if target.binlog_enabled is True and not allow_binlog_gap:
            raise UnsupportedOperationError(
                "target table has Binlog enabled; overwrite cannot provide a "
                "complete row-level CDC stream. Pass allow_binlog_gap=True only "
                "if downstream CDC consumers accept this gap."
            )
        server_version = session.server_version()
        if server_version == (0, 0, 0):
            raise UnsupportedOperationError(
                "cannot determine the Hologres server version; refusing overwrite "
                "because the safe overwrite implementation cannot be selected"
            )
        query = _build_overwrite_sql(
            target,
            source_query,
            source_columns,
            partition_values,
        )
        if server_version >= _OVERWRITE_MIN_VERSION:
            if allow_binlog_gap:
                _execute_write_transaction(
                    session,
                    [query],
                    allow_binlog_gap=True,
                    execution_options=dataframe._execution_options,
                )
            else:
                _execute(
                    session,
                    query,
                    fetch=False,
                    execution_options=dataframe._execution_options,
                )
            return

        _execute_legacy_overwrite(
            dataframe,
            session,
            target,
            source_query,
            source_columns,
            partition_values,
            allow_binlog_gap=allow_binlog_gap,
        )
        return

    if action == "replace":
        source_query, source_columns = _replace_source_projection(
            target, source_query, source_columns
        )
    query = _build_insert_sql(
        target,
        source_query,
        source_columns,
        action=action,
        conflict_keys=conflict_keys,
    )
    _execute(
        session,
        query,
        fetch=False,
        execution_options=dataframe._execution_options,
    )


def _parse_table_name(name: str) -> TableName:
    try:
        return TableName.valueOf(name)
    except ValueError as exc:
        raise InvalidArgumentError(f"invalid table name {name!r}: {exc}") from exc


def _parse_on_conflict(value):
    if value is None:
        return None, None
    if not isinstance(value, (tuple, list)) or len(value) != 2:
        raise InvalidArgumentError("on_conflict must be a (action, keys) pair")
    action, keys = value
    if action not in {"ignore", "update", "replace"}:
        raise InvalidArgumentError(
            "on_conflict action must be one of ignore, update, replace"
        )
    if not isinstance(keys, (list, tuple)) or not keys:
        raise InvalidArgumentError("on_conflict keys must be a non-empty list")
    if any(not isinstance(key, str) or not key for key in keys):
        raise InvalidArgumentError("on_conflict keys must be non-empty strings")
    if len(set(keys)) != len(keys):
        raise InvalidArgumentError("on_conflict keys must not contain duplicates")
    return action, tuple(keys)


def _load_target_metadata(
    session,
    table_name: TableName,
    *,
    need_primary_keys: bool,
    need_partition: bool,
    need_binlog: bool,
) -> _TargetMetadata:
    schema = session._fetch_table_schema(table_name.schema_name, table_name.table_name)
    primary_keys: tuple[str, ...] = ()
    logical_partition_keys: tuple[str, ...] = ()
    physical_partition_parent = False
    binlog_enabled: bool | None = None
    table_properties = None

    qualified = sql.Literal(f'"{table_name.schema_name}"."{table_name.table_name}"')
    if need_primary_keys:
        rows, _ = session.execute(
            sql.SQL(
                "SELECT a.attname "
                "FROM pg_index i "
                "JOIN pg_attribute a ON a.attrelid = i.indrelid "
                "AND a.attnum = ANY(i.indkey) "
                "WHERE i.indrelid = {}::regclass AND i.indisprimary "
                "ORDER BY array_position(i.indkey, a.attnum)"
            ).format(qualified)
        )
        primary_keys = tuple(row[0] for row in rows)

    if need_partition:
        rows, _ = session.execute(
            sql.SQL(
                "SELECT part.partstrat, a.attname, "
                "EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i "
                "WHERE i.inhparent = c.oid) "
                "FROM pg_catalog.pg_class c "
                "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                "LEFT JOIN pg_catalog.pg_partitioned_table part "
                "ON c.oid = part.partrelid "
                "LEFT JOIN pg_catalog.pg_attribute a "
                "ON a.attrelid = c.oid AND a.attnum = part.partattrs[1] "
                "WHERE n.nspname = {} AND c.relname = {}"
            ).format(
                sql.Literal(table_name.schema_name),
                sql.Literal(table_name.table_name),
            )
        )
        if rows and rows[0][0] is not None:
            physical_partition_parent = True

    if need_partition or need_binlog:
        try:
            rows, _ = session.execute(
                sql.SQL(
                    "SELECT property_key, property_value "
                    "FROM hologres.hg_table_properties "
                    "WHERE table_namespace = {} AND table_name = {}"
                ).format(
                    sql.Literal(table_name.schema_name),
                    sql.Literal(table_name.table_name),
                )
            )
            table_properties = rows
        except ExecutionError:
            if need_partition:
                raise
            # Older/non-Hologres metadata surfaces may not expose this catalog.
            # Binlog-only callers leave the state unknown rather than guessing
            # disabled. Destructive overwrite callers must know the partition
            # metadata and therefore propagate the error above.
            table_properties = None

    if table_properties is not None:
        if need_partition:
            properties = _table_properties(table_properties)
            if _property_is_true(properties.get("is_logical_partitioned_table")):
                columns = properties.get("logical_partition_columns", "")
                logical_partition_keys = tuple(
                    column.strip()
                    for column in str(columns).split(",")
                    if column.strip()
                )
        if need_binlog:
            binlog_enabled = _property_enables_binlog(table_properties)

    return _TargetMetadata(
        table_name=table_name,
        schema=schema,
        primary_keys=primary_keys,
        logical_partition_keys=logical_partition_keys,
        physical_partition_parent=physical_partition_parent,
        binlog_enabled=binlog_enabled,
    )


def _property_enables_binlog(rows) -> bool:
    for key, value in rows:
        key_text = str(key).lower()
        if "binlog" not in key_text:
            continue
        value_text = str(value).strip().lower()
        if value_text in {"on", "true", "1", "enabled", "full"}:
            return True
        if value_text in {"off", "false", "0", "disabled", "none", ""}:
            return False
        # Hologres may expose levels such as "replica" rather than booleans.
        return True
    return False


def _table_properties(rows) -> dict[str, str]:
    return {str(key): str(value) for key, value in rows}


def _property_is_true(value) -> bool:
    return str(value).strip().lower() in {"on", "true", "1", "yes"}


def _validate_columns(source_columns, target_columns) -> None:
    if len(source_columns) != len(set(source_columns)):
        raise InvalidArgumentError("DataFrame output contains duplicate columns")
    missing = [name for name in source_columns if name not in target_columns]
    if missing:
        raise InvalidArgumentError(
            f"target table is missing DataFrame column(s): {', '.join(missing)}"
        )


def _validate_partition(partition, target, source_columns, *, overwrite):
    if overwrite and target.physical_partition_parent:
        raise UnsupportedOperationError(
            "overwrite does not support a physical partition parent; "
            "write the partition child table"
        )
    if partition is None:
        if overwrite and target.logical_partition_keys:
            raise UnsupportedOperationError(
                "logical partition tables require partition= for overwrite"
            )
        return None
    if not overwrite:
        raise InvalidArgumentError("partition is only valid with overwrite=True")
    if not isinstance(partition, dict) or not partition:
        raise InvalidArgumentError("partition must be a non-empty dict")
    if any(not isinstance(key, str) or not key for key in partition):
        raise InvalidArgumentError("partition keys must be non-empty strings")
    expected = set(target.logical_partition_keys)
    if expected and set(partition) != expected:
        raise InvalidArgumentError(
            f"partition keys must match the logical partition keys {sorted(expected)}"
        )
    if not expected:
        raise InvalidArgumentError(
            "partition is only supported for logical partition tables"
        )
    missing = [key for key in partition if key not in source_columns]
    if missing:
        raise InvalidArgumentError(
            f"DataFrame output is missing partition column(s): {', '.join(missing)}"
        )
    return tuple(partition.items())


def _validate_conflict(action, keys, primary_keys, *, source_columns) -> None:
    if keys is None:
        return
    if not primary_keys:
        raise InvalidArgumentError("on_conflict requires a target primary key")
    if set(keys) != set(primary_keys):
        raise InvalidArgumentError(
            f"on_conflict keys must match target primary key {list(primary_keys)}"
        )
    missing = [key for key in keys if key not in source_columns]
    if missing:
        raise InvalidArgumentError(
            f"DataFrame output is missing on_conflict key(s): {', '.join(missing)}"
        )


def _replace_source_projection(target, source_query, source_columns):
    source_names = set(source_columns)
    target_columns = list(target.schema.names)
    projections = []
    for name in target_columns:
        if name in source_names:
            projections.append(sql.Identifier(name))
        else:
            projections.append(sql.SQL("NULL AS {}").format(sql.Identifier(name)))
    query = sql.SQL("SELECT {} FROM ({}) AS __hg_replace_source").format(
        sql.SQL(", ").join(projections), source_query
    )
    return query, target_columns


def _build_insert_sql(target, source_query, source_columns, *, action, conflict_keys):
    query = sql.SQL("INSERT INTO {} ({}) {}").format(
        target.table_name.full_name,
        _identifiers(source_columns),
        source_query,
    )
    if action is None:
        return query
    conflict = sql.SQL(" ON CONFLICT ({}) ").format(_identifiers(conflict_keys))
    if action == "ignore":
        return sql.Composed([query, conflict, sql.SQL("DO NOTHING")])
    if action == "update":
        update_columns = [
            name for name in source_columns if name not in set(conflict_keys)
        ]
    else:
        update_columns = [
            field.name
            for field in target.schema.fields
            if field.name not in set(conflict_keys)
        ]
    if not update_columns:
        return sql.Composed([query, conflict, sql.SQL("DO NOTHING")])
    assignments = sql.SQL(", ").join(
        sql.SQL("{} = excluded.{}").format(sql.Identifier(name), sql.Identifier(name))
        for name in update_columns
    )
    return sql.Composed([query, conflict, sql.SQL("DO UPDATE SET "), assignments])


def _build_overwrite_sql(
    target,
    source_query,
    source_columns,
    partition_values,
):
    if target.physical_partition_parent:
        raise UnsupportedOperationError(
            "overwrite does not support a physical partition parent; "
            "write the partition child table"
        )
    parts = [sql.SQL("INSERT OVERWRITE "), target.table_name.full_name]
    if partition_values:
        parts.extend(
            [
                sql.SQL(" PARTITION ("),
                sql.SQL(", ").join(
                    sql.SQL("{} = {}").format(sql.Identifier(key), sql.Literal(value))
                    for key, value in partition_values
                ),
                sql.SQL(")"),
            ]
        )
    parts.extend(
        [sql.SQL(" ("), _identifiers(source_columns), sql.SQL(") "), source_query]
    )
    return sql.Composed(parts)


def _execute_legacy_overwrite(
    dataframe,
    session,
    target,
    source_query,
    source_columns,
    partition_values,
    *,
    allow_binlog_gap,
):
    source_query_for_insert = _restrict_source_to_partition(
        source_query, source_columns, partition_values, low_version=True
    )
    statements = []
    if partition_values:
        predicates = sql.SQL(" AND ").join(
            sql.SQL("{} = {}").format(sql.Identifier(key), sql.Literal(value))
            for key, value in partition_values
        )
        statements.append(
            sql.SQL("DELETE FROM {} WHERE ").format(target.table_name.full_name)
            + predicates
        )
    else:
        statements.append(
            sql.SQL("TRUNCATE TABLE {}").format(target.table_name.full_name)
        )
    statements.append(
        sql.SQL("INSERT INTO {} ({}) {}").format(
            target.table_name.full_name,
            _identifiers(source_columns),
            source_query_for_insert,
        )
    )

    prelude = []
    cleanup = []
    if _plan_references_table(dataframe._plan, target.table_name):
        temp = f"__hg_write_source_{uuid.uuid4().hex[:12]}"
        prelude.append(
            sql.SQL("CREATE TEMP TABLE {} AS ").format(sql.Identifier(temp))
            + source_query
        )
        cleanup.append(sql.SQL("DROP TABLE IF EXISTS {} ").format(sql.Identifier(temp)))
        temp_source = sql.SQL("SELECT {} FROM {}").format(
            _identifiers(source_columns), sql.Identifier(temp)
        )
        temp_source = _restrict_source_to_partition(
            temp_source, source_columns, partition_values, low_version=True
        )
        statements[-1] = sql.SQL("INSERT INTO {} ({}) {}").format(
            target.table_name.full_name, _identifiers(source_columns), temp_source
        )
    _execute_write_transaction(
        session,
        statements,
        prelude=prelude,
        cleanup=cleanup,
        allow_binlog_gap=allow_binlog_gap,
        execution_options=dataframe._execution_options,
    )


def _execute(session, query, *, fetch, execution_options):
    if execution_options is None:
        return session.execute(query, fetch=fetch)
    return session.execute(query, fetch=fetch, _execution_options=execution_options)


def _execute_write_transaction(
    session,
    statements,
    *,
    prelude=(),
    cleanup=(),
    allow_binlog_gap=False,
    execution_options=None,
):
    kwargs = {
        "prelude": prelude,
        "cleanup": cleanup,
        "allow_binlog_gap": allow_binlog_gap,
    }
    if execution_options is not None:
        kwargs["execution_options"] = execution_options
    return session._execute_write_transaction(statements, **kwargs)


def _restrict_source_to_partition(
    source_query, source_columns, partition_values, *, low_version
):
    if not partition_values or not low_version:
        return source_query
    missing = [key for key, _ in partition_values if key not in source_columns]
    if missing:
        raise InvalidArgumentError(
            f"DataFrame output is missing partition column(s): {', '.join(missing)}"
        )
    predicates = sql.SQL(" AND ").join(
        sql.SQL("{} = {}").format(sql.Identifier(key), sql.Literal(value))
        for key, value in partition_values
    )
    return sql.SQL("SELECT * FROM ({}) __hg_partition_source WHERE {} ").format(
        source_query, predicates
    )


def _plan_references_table(plan, target: TableName) -> bool:
    for node in _plan.iter_nodes(plan):
        if isinstance(node, _plan.TableScan) and node.table_name == target:
            return True
        if isinstance(node, _plan.RawSql):
            # Raw SQL is intentionally opaque. Materialize it before a legacy
            # TRUNCATE/DELETE because it may reference the target indirectly.
            return True
        if (
            isinstance(node, _plan.PendingSource)
            and node.resolved is not None
            and _plan_references_table(node.resolved, target)
        ):
            return True
    return False


def _identifiers(names) -> sql.Composed:
    return sql.SQL(", ").join(sql.Identifier(name) for name in names)
