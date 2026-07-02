"""SQL generation for INSERT/DELETE/SELECT statements.

Uses unnest-based batch inserts matching the Java HoloClient approach:
  INSERT INTO t (c1, c2) SELECT unnest(?::int4[]), unnest(?::text[])
  ON CONFLICT (...) DO UPDATE SET ...

This is more efficient than multi-row VALUES for large batches because
the SQL text stays constant regardless of batch size, enabling better
server-side prepared statement caching.
"""

from __future__ import annotations

from typing import Any, List, Tuple

from psycopg import sql

from .column import (
    ARRAY,
    BIGINT,
    BINARY,
    BIT,
    BOOLEAN,
    CHAR,
    DATE,
    DECIMAL,
    DOUBLE,
    INTEGER,
    NUMERIC,
    OTHER,
    REAL,
    SMALLINT,
    TIME,
    TIMESTAMP,
    TIMESTAMP_WITH_TIMEZONE,
    VARCHAR,
    Column,
)
from .record import Record
from .table_schema import TableSchema
from .types import OnConflictAction

# Text types where \u0000 should be stripped
_TEXT_TYPES = frozenset({VARCHAR, CHAR})


def _remove_u0000(value: Any, col_type: int, remove: bool) -> Any:
    """Strip \\u0000 from string values for text columns when enabled."""
    if remove and isinstance(value, str) and col_type in _TEXT_TYPES:
        if "\x00" in value:
            return value.replace("\x00", "")
    return value


# Map from Column sql type constant to the PG type name used in unnest casts.
# Falls back to Column.type_name if not found here.
_UNNEST_TYPE_MAP: dict[int, str] = {
    BOOLEAN: "bool",
    BIT: "bool",
    SMALLINT: "int2",
    INTEGER: "int4",
    BIGINT: "int8",
    REAL: "float4",
    DOUBLE: "float8",
    NUMERIC: "numeric",
    DECIMAL: "numeric",
    CHAR: "text",
    VARCHAR: "text",
    DATE: "date",
    TIME: "time",
    TIMESTAMP: "timestamp",
    TIMESTAMP_WITH_TIMEZONE: "timestamptz",
    BINARY: "bytea",
}

# Types supported by unnest (matching Java's isTypeSupportForUnnest)
_UNNEST_SUPPORTED_TYPES: set[int] = {
    BOOLEAN,
    BIT,
    SMALLINT,
    INTEGER,
    BIGINT,
    REAL,
    DOUBLE,
    NUMERIC,
    DECIMAL,
    CHAR,
    VARCHAR,
    DATE,
    TIME,
    TIMESTAMP,
    TIMESTAMP_WITH_TIMEZONE,
    BINARY,
}


def _get_unnest_type_name(column: Column) -> str | None:
    """Get the PG type name for unnest cast, or None if unsupported.

    Matches Java's StatementBuilderUtil.getRealTypeName() and
    isTypeSupportForUnnest().
    """
    col_type = column.type
    type_name = column.type_name.lower().strip()

    # Serial types → use the underlying integer type
    if type_name == "serial":
        return "int4"
    if type_name == "smallserial":
        return "int2"
    if type_name == "bigserial":
        return "int8"

    # json/jsonb: Java maps OTHER to json/jsonb for unnest
    if type_name in ("json", "jsonb"):
        return type_name

    # uuid: Hologres does not support uuid[] in unnest, fall back to VALUES
    if type_name == "uuid":
        return None

    # roaringbitmap: extension type stored as BINARY but can't use bytea[] unnest
    if type_name == "roaringbitmap":
        return None

    # Array types: not supported in unnest
    if col_type == ARRAY:
        return None

    # OTHER types (except json/jsonb/uuid handled above): not supported
    if col_type == OTHER:
        return None

    # Standard types
    return _UNNEST_TYPE_MAP.get(col_type)


def _all_columns_support_unnest(schema: TableSchema, col_indices: List[int]) -> bool:
    """Check if all columns support unnest-based insert."""
    for i in col_indices:
        col = schema.get_column(i)
        if col.is_generated_column:
            continue
        if _get_unnest_type_name(col) is None:
            return False
    return True


def build_insert_sql(
    schema: TableSchema,
    records: List[Record],
    on_conflict: OnConflictAction,
    remove_u0000_in_text: bool = True,
) -> Tuple[str, List[Any]]:
    """Build a batch INSERT statement.

    Uses unnest-based columnar format when all columns support it:
      INSERT INTO t (c1, c2) SELECT unnest($1::int4[]), unnest($2::text[])

    Falls back to multi-row VALUES when any column doesn't support unnest.

    Returns (sql_string, params) tuple.
    """
    if not records:
        return "", []

    # Determine the union of all set columns across records
    set_columns: set[int] = set()
    only_insert_columns: set[int] = set()
    for r in records:
        set_columns |= r.set_columns
        only_insert_columns |= r.only_insert_columns

    sorted_cols = sorted(set_columns)

    if _all_columns_support_unnest(schema, sorted_cols):
        return _build_unnest_insert_sql(
            schema,
            records,
            on_conflict,
            sorted_cols,
            only_insert_columns,
            remove_u0000_in_text,
        )
    else:
        return _build_values_insert_sql(
            schema,
            records,
            on_conflict,
            sorted_cols,
            only_insert_columns,
            remove_u0000_in_text,
        )


def _build_unnest_insert_sql(
    schema: TableSchema,
    records: List[Record],
    on_conflict: OnConflictAction,
    sorted_cols: List[int],
    only_insert_columns: set[int],
    remove_u0000_in_text: bool = True,
) -> Tuple[str, List[Any]]:
    """Build unnest-based INSERT matching Java's UnnestUpsertStatementBuilder."""
    table_name = f'"{schema.schema_name}"."{schema.table_name}"'

    col_name_parts = []
    unnest_parts = []
    for i in sorted_cols:
        col = schema.get_column(i)
        col_name_parts.append(f'"{col.name}"')
        type_name = _get_unnest_type_name(col)
        unnest_parts.append(f"unnest(${{}}::{type_name}[])")

    cols_str = ", ".join(col_name_parts)

    # Build SELECT unnest(...) parts with positional placeholders
    select_parts = []
    for idx, i in enumerate(sorted_cols):
        col = schema.get_column(i)
        type_name = _get_unnest_type_name(col)
        select_parts.append(f"unnest(%s::{type_name}[])")

    select_str = ", ".join(select_parts)
    stmt = f"INSERT INTO {table_name} ({cols_str}) SELECT {select_str}"

    # ON CONFLICT clause
    if schema.has_primary_key:
        pk_names = [f'"{schema.get_column(i).name}"' for i in schema.pk_index]
        pk_str = ", ".join(pk_names)

        if on_conflict == OnConflictAction.INSERT_OR_IGNORE:
            stmt += f" ON CONFLICT ({pk_str}) DO NOTHING"
        elif on_conflict in (
            OnConflictAction.INSERT_OR_UPDATE,
            OnConflictAction.INSERT_OR_REPLACE,
        ):
            update_parts = []
            for i in sorted_cols:
                if i in only_insert_columns:
                    continue
                col_name = f'"{schema.get_column(i).name}"'
                update_parts.append(f"{col_name}=EXCLUDED.{col_name}")

            if update_parts:
                stmt += (
                    f" ON CONFLICT ({pk_str}) DO UPDATE SET {', '.join(update_parts)}"
                )
            else:
                stmt += f" ON CONFLICT ({pk_str}) DO NOTHING"

    # Build columnar params: one list per column
    params: List[Any] = []
    for i in sorted_cols:
        col = schema.get_column(i)
        col_values = []
        for r in records:
            v = r.values[i] if r.is_set(i) else None
            col_values.append(_remove_u0000(v, col.type, remove_u0000_in_text))
        params.append(col_values)

    return stmt, params


def _build_values_insert_sql(
    schema: TableSchema,
    records: List[Record],
    on_conflict: OnConflictAction,
    sorted_cols: List[int],
    only_insert_columns: set[int],
    remove_u0000_in_text: bool = True,
) -> Tuple[str, List[Any]]:
    """Fallback: multi-row VALUES insert for columns that don't support unnest."""
    col_names = [sql.Identifier(schema.get_column(i).name) for i in sorted_cols]

    placeholder_row = sql.SQL("({})").format(
        sql.SQL(", ").join([sql.Placeholder()] * len(sorted_cols))
    )
    values_clause = sql.SQL(", ").join([placeholder_row] * len(records))

    table_name = sql.SQL("{}.{}").format(
        sql.Identifier(schema.schema_name),
        sql.Identifier(schema.table_name),
    )

    stmt = sql.SQL("INSERT INTO {table} ({cols}) VALUES {values}").format(
        table=table_name,
        cols=sql.SQL(", ").join(col_names),
        values=values_clause,
    )

    # ON CONFLICT clause
    if schema.has_primary_key:
        pk_names = [sql.Identifier(schema.get_column(i).name) for i in schema.pk_index]
        pk_clause = sql.SQL(", ").join(pk_names)

        if on_conflict == OnConflictAction.INSERT_OR_IGNORE:
            stmt = sql.SQL("{} ON CONFLICT ({}) DO NOTHING").format(stmt, pk_clause)
        elif on_conflict in (
            OnConflictAction.INSERT_OR_UPDATE,
            OnConflictAction.INSERT_OR_REPLACE,
        ):
            update_parts = []
            for i in sorted_cols:
                if i in only_insert_columns:
                    continue
                col = sql.Identifier(schema.get_column(i).name)
                update_parts.append(sql.SQL("{col}=EXCLUDED.{col}").format(col=col))

            if update_parts:
                stmt = sql.SQL("{} ON CONFLICT ({}) DO UPDATE SET {}").format(
                    stmt, pk_clause, sql.SQL(", ").join(update_parts)
                )
            else:
                stmt = sql.SQL("{} ON CONFLICT ({}) DO NOTHING").format(stmt, pk_clause)

    params: List[Any] = []
    for r in records:
        for i in sorted_cols:
            v = r.values[i] if r.is_set(i) else None
            col = schema.get_column(i)
            params.append(_remove_u0000(v, col.type, remove_u0000_in_text))

    return stmt, params


def build_delete_sql(
    schema: TableSchema,
    records: List[Record],
) -> Tuple[str, List[Any]]:
    """Build a DELETE statement for records with DELETE mutation type.

    Uses row-based WHERE clause (matching Java — no unnest for deletes).
    Returns (sql_string, params) tuple.
    """
    if not records:
        return "", []

    pk_indices = schema.pk_index
    pk_names = [sql.Identifier(schema.get_column(i).name) for i in pk_indices]

    one_clause = sql.SQL(" AND ").join(
        sql.SQL("{}={}").format(pk, sql.Placeholder()) for pk in pk_names
    )
    where_clause = sql.SQL(" OR ").join(
        sql.SQL("({})").format(one_clause) for _ in records
    )

    table_id = sql.SQL("{}.{}").format(
        sql.Identifier(schema.schema_name),
        sql.Identifier(schema.table_name),
    )

    stmt = sql.SQL("DELETE FROM {} WHERE {}").format(table_id, where_clause)

    params: List[Any] = []
    for r in records:
        for i in pk_indices:
            params.append(r.values[i])

    return stmt, params


def build_get_sql(
    schema: TableSchema,
    records: List[Record],
    selected_columns: set[int],
) -> Tuple[str, List[Any]]:
    """Build a SELECT statement for point queries by primary key.

    Returns (sql_string, params) tuple.
    """
    if not records:
        return "", []

    # SELECT columns
    sorted_sel = sorted(selected_columns)
    sel_names = [sql.Identifier(schema.get_column(i).name) for i in sorted_sel]

    # WHERE clause
    pk_indices = schema.pk_index
    pk_col_names = [sql.Identifier(schema.get_column(i).name) for i in pk_indices]
    one_clause = sql.SQL(" AND ").join(
        sql.SQL("{}={}").format(pk, sql.Placeholder()) for pk in pk_col_names
    )
    where_clause = sql.SQL(" OR ").join(
        sql.SQL("({})").format(one_clause) for _ in records
    )

    table_name = sql.SQL("{}.{}").format(
        sql.Identifier(schema.schema_name),
        sql.Identifier(schema.table_name),
    )

    stmt = sql.SQL("SELECT {} FROM {} WHERE {}").format(
        sql.SQL(", ").join(sel_names),
        table_name,
        where_clause,
    )

    params: List[Any] = []
    for r in records:
        for i in pk_indices:
            params.append(r.values[i])

    return stmt, params


def build_scan_sql(scan) -> Tuple[str, List[Any]]:
    """Build a SELECT … WHERE … ORDER BY … query from a Scan object.

    Returns (composed_sql, params) suitable for ``cursor.execute(stmt, params)``.
    """
    from .scan import EqualsFilter, RangeFilter, SortKeys

    schema = scan.schema

    # ---------- SELECT columns ----------
    if scan.selected_columns is not None:
        sorted_sel = sorted(scan.selected_columns)
    else:
        sorted_sel = list(range(schema.column_count))

    sel_names = [sql.Identifier(schema.get_column(i).name) for i in sorted_sel]

    table_name = sql.SQL("{}.{}").format(
        sql.Identifier(schema.schema_name),
        sql.Identifier(schema.table_name),
    )

    # ---------- WHERE clause ----------
    params: List[Any] = []
    where_parts: List[sql.Composable] = []

    for f in scan.filters:
        col_name = sql.Identifier(schema.get_column(f.column_index).name)
        if isinstance(f, EqualsFilter):
            where_parts.append(sql.SQL("{} = {}").format(col_name, sql.Placeholder()))
            params.append(f.value)
        elif isinstance(f, RangeFilter):
            if f.start is not None:
                op = ">=" if f.start_inclusive else ">"
                where_parts.append(
                    sql.SQL("{} " + op + " {}").format(col_name, sql.Placeholder())
                )
                params.append(f.start)
            if f.end is not None:
                op = "<=" if f.end_inclusive else "<"
                where_parts.append(
                    sql.SQL("{} " + op + " {}").format(col_name, sql.Placeholder())
                )
                params.append(f.end)

    # ---------- ORDER BY ----------
    order_parts: List[sql.Composable] = []
    if scan.sort_keys == SortKeys.PRIMARY_KEY:
        for pk in schema.primary_keys:
            order_parts.append(sql.Identifier(pk))
    elif scan.sort_keys == SortKeys.CLUSTERING_KEY:
        if not schema.clustering_keys:
            raise ValueError(
                "SortKeys.CLUSTERING_KEY requested but schema has no clustering keys"
            )
        for ck in schema.clustering_keys:
            # Strip :asc / :desc suffix if present
            col_name = ck.split(":")[0]
            order_parts.append(sql.Identifier(col_name))
    # SortKeys.NONE → no ORDER BY

    # ---------- Assemble ----------
    if where_parts and order_parts:
        stmt = sql.SQL("SELECT {} FROM {} WHERE {} ORDER BY {}").format(
            sql.SQL(", ").join(sel_names),
            table_name,
            sql.SQL(" AND ").join(where_parts),
            sql.SQL(", ").join(order_parts),
        )
    elif where_parts:
        stmt = sql.SQL("SELECT {} FROM {} WHERE {}").format(
            sql.SQL(", ").join(sel_names),
            table_name,
            sql.SQL(" AND ").join(where_parts),
        )
    elif order_parts:
        stmt = sql.SQL("SELECT {} FROM {} ORDER BY {}").format(
            sql.SQL(", ").join(sel_names),
            table_name,
            sql.SQL(", ").join(order_parts),
        )
    else:
        stmt = sql.SQL("SELECT {} FROM {}").format(
            sql.SQL(", ").join(sel_names),
            table_name,
        )

    return stmt, params
