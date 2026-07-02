"""Load table schema from a PostgreSQL/Hologres database."""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

from psycopg import sql

from .column import Column
from .table_name import TableName
from .table_schema import TableSchema

if TYPE_CHECKING:
    import psycopg


def _qualified_name(table_name: TableName) -> sql.Composed:
    """Build a qualified table identifier: schema.table."""
    return sql.SQL("{}.{}").format(
        sql.Identifier(table_name.schema_name),
        sql.Identifier(table_name.table_name),
    )


def _qualified_name_literal(table_name: TableName) -> sql.Literal:
    """Build a qualified name as a string literal for ::regclass casts."""
    schema = table_name.schema_name.replace('"', '""')
    table = table_name.table_name.replace('"', '""')
    return sql.Literal(f'"{schema}"."{table}"')


_SCHEMA_SQL = sql.SQL("""\
SELECT
    a.attname AS column_name,
    format_type(a.atttypid, a.atttypmod) AS type_name,
    a.attnotnull AS not_null,
    pg_get_expr(d.adbin, d.adrelid) AS default_value,
    a.attnum AS ordinal
FROM pg_attribute a
LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
WHERE a.attrelid = {}::regclass
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum
""")

_PK_SQL = sql.SQL("""\
SELECT a.attname
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = {}::regclass
  AND i.indisprimary
ORDER BY array_position(i.indkey, a.attnum)
""")

_TABLE_PROPERTIES_SQL = sql.SQL("""\
SELECT property_key, property_value
FROM hologres.hg_table_properties
WHERE table_namespace = {} AND table_name = {}
  AND property_key IN ('distribution_key', 'table_id', 'schema_version')
""")

_PARTITION_SQL = sql.SQL("""\
SELECT a.attname
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_partitioned_table part ON c.oid = part.partrelid
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = part.partattrs[1]
WHERE n.nspname = {} AND c.relname = {}
  AND part.partstrat = 'l'
LIMIT 1
""")


def _extract_precision_scale(raw: str) -> tuple[int, int]:
    """Extract precision and scale from a type like ``numeric(38,18)``.

    Returns (precision, scale) or (0, 0) if not present.
    """
    lower = raw.lower().strip()
    paren = lower.find("(")
    if paren < 0:
        return 0, 0
    close = lower.find(")", paren)
    if close < 0:
        return 0, 0
    inside = lower[paren + 1 : close]
    parts = inside.split(",")
    try:
        precision = int(parts[0].strip())
        scale = int(parts[1].strip()) if len(parts) > 1 else 0
        return precision, scale
    except (ValueError, IndexError):
        return 0, 0


def _normalize_type_name(raw: str) -> str:
    """Normalize a PostgreSQL type name for Column creation.

    Strips length modifiers like ``character varying(255)`` -> ``varchar``.
    """
    lower = raw.lower().strip()
    paren = lower.find("(")
    if paren > 0:
        lower = lower[:paren].rstrip()
    aliases = {
        "character varying": "varchar",
        "character": "char",
        "double precision": "float8",
        "timestamp without time zone": "timestamp",
        "timestamp with time zone": "timestamptz",
        "time without time zone": "time",
        "time with time zone": "timetz",
    }
    return aliases.get(lower, lower)


def load_table_schema_sync(
    conn: psycopg.Connection,
    table_name: TableName,
) -> TableSchema:
    """Load a TableSchema from the database (synchronous)."""
    qualified = _qualified_name_literal(table_name)

    # Load columns
    columns: List[Column] = []
    with conn.cursor() as cur:
        cur.execute(_SCHEMA_SQL.format(qualified))
        rows = cur.fetchall()
    for col_name, raw_type, not_null, default_val, _ordinal in rows:
        precision, scale = _extract_precision_scale(raw_type)
        norm_type = _normalize_type_name(raw_type)
        col = Column.from_pg_type_name(
            name=col_name,
            pg_type_name=norm_type,
            allow_null=not not_null,
            default_value=default_val,
            precision=precision,
            scale=scale,
        )
        columns.append(col)

    # Load primary keys
    pk_names: set[str] = set()
    with conn.cursor() as cur:
        cur.execute(_PK_SQL.format(qualified))

        for (pk_name,) in cur.fetchall():
            pk_names.add(pk_name)
    for col in columns:
        if col.name in pk_names:
            col.is_primary_key = True

    schema_id = sql.Literal(table_name.schema_name)
    table_id_lit = sql.Literal(table_name.table_name)

    # Load table properties (distribution_key, table_id, schema_version)
    dist_keys: List[str] = []
    table_id = ""
    schema_version = ""
    with conn.cursor() as cur:
        cur.execute(_TABLE_PROPERTIES_SQL.format(schema_id, table_id_lit))
        for key, value in cur.fetchall():
            if key == "distribution_key" and value:
                dist_keys = [k.strip() for k in value.split(",")]
            elif key == "table_id":
                table_id = value or ""
            elif key == "schema_version":
                schema_version = value or ""

    # Load partition info
    partition_column: Optional[str] = None
    with conn.cursor() as cur:
        cur.execute(_PARTITION_SQL.format(schema_id, table_id_lit))
        row = cur.fetchone()
        if row:
            partition_column = row[0]

    return TableSchema(
        table_name=table_name,
        columns=columns,
        table_id=table_id,
        schema_version=schema_version,
        distribution_keys=dist_keys,
        partition_column=partition_column,
    )


async def load_table_schema_async(
    conn,  # psycopg.AsyncConnection
    table_name: TableName,
) -> TableSchema:
    """Load a TableSchema from the database (asynchronous)."""
    qualified = _qualified_name_literal(table_name)

    # Load columns
    columns: List[Column] = []
    async with conn.cursor() as cur:
        await cur.execute(_SCHEMA_SQL.format(qualified))
        rows = await cur.fetchall()
    for col_name, raw_type, not_null, default_val, _ordinal in rows:
        precision, scale = _extract_precision_scale(raw_type)
        norm_type = _normalize_type_name(raw_type)
        col = Column.from_pg_type_name(
            name=col_name,
            pg_type_name=norm_type,
            allow_null=not not_null,
            default_value=default_val,
            precision=precision,
            scale=scale,
        )
        columns.append(col)

    # Load primary keys
    pk_names: set[str] = set()
    async with conn.cursor() as cur:
        await cur.execute(_PK_SQL.format(qualified))
        for (pk_name,) in await cur.fetchall():
            pk_names.add(pk_name)
    for col in columns:
        if col.name in pk_names:
            col.is_primary_key = True

    schema_id = sql.Literal(table_name.schema_name)
    table_id_lit = sql.Literal(table_name.table_name)

    # Load table properties (distribution_key, table_id, schema_version)
    dist_keys: List[str] = []
    table_id = ""
    schema_version = ""
    async with conn.cursor() as cur:
        await cur.execute(_TABLE_PROPERTIES_SQL.format(schema_id, table_id_lit))
        for key, value in await cur.fetchall():
            if key == "distribution_key" and value:
                dist_keys = [k.strip() for k in value.split(",")]
            elif key == "table_id":
                table_id = value or ""
            elif key == "schema_version":
                schema_version = value or ""

    # Load partition info
    partition_column: Optional[str] = None
    async with conn.cursor() as cur:
        await cur.execute(_PARTITION_SQL.format(schema_id, table_id_lit))
        row = await cur.fetchone()
        if row:
            partition_column = row[0]

    return TableSchema(
        table_name=table_name,
        columns=columns,
        table_id=table_id,
        schema_version=schema_version,
        distribution_keys=dist_keys,
        partition_column=partition_column,
    )
