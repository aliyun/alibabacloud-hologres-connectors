"""SQL builders for Hologres internal stage operations.

Generates SQL statements for:
- Uploading Arrow data to an internal stage via COPY protocol
- Loading data from a stage into a target table via INSERT...SELECT
- Creating and dropping internal stages
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

from psycopg import sql

from .column import BIGINT, DECIMAL, INTEGER, NUMERIC
from .types import OnConflictAction

if TYPE_CHECKING:
    from .column import Column
    from .table_schema import TableSchema


def _get_stage_type_name(column: Column) -> str:
    """Get the real type name for use in external_files AS clause.

    Handles serial -> int/bigint mapping and decimal precision/scale.
    JSON/JSONB columns are NOT remapped here; the caller handles that.
    """
    type_name = column.type_name.strip().lower()

    # Serial types -> base integer types
    if column.type == INTEGER and type_name == "serial":
        return "int"
    if column.type == BIGINT and type_name == "bigserial":
        return "bigint"

    # Decimal with explicit precision/scale
    if column.type in (NUMERIC, DECIMAL):
        precision = column.precision if column.precision > 0 else 38
        scale = column.scale if column.scale >= 0 else 0
        return f"decimal({precision},{scale})"

    return type_name


def build_copy_in_stage_sql(stage_name: str, file_name: str) -> str:
    """Build COPY SQL for uploading a file to an internal stage.

    Returns SQL like:
        copy external_files(path='internal_stage://my_stage/file_0.arrow') from stdin;
    """
    return (
        f"copy external_files(path='internal_stage://{stage_name}/{file_name}') "
        f"from stdin;"
    )


def build_insert_select_from_stage_sql(
    schema: TableSchema,
    column_names: List[str],
    stages: List[str],
    conflict_action: Optional[OnConflictAction] = None,
    is_overwrite: bool = False,
) -> str:
    """Build INSERT...SELECT FROM external_files SQL to load stage data into a table.

    Generates SQL like:
        insert into "schema"."table" ("col1", "col2")
        select "col1", "col2"::jsonb
        from external_files(path='internal_stage://stage1') as ("col1" int4, "col2" text)
        on conflict ("pk") do update set "col1"=excluded."col1", "col2"=excluded."col2"
    """
    # Resolve type names for each column
    type_names = []
    for name in column_names:
        idx = schema.get_column_index(name)
        column = schema.get_column(idx)
        type_names.append(_get_stage_type_name(column))

    full_table = (
        sql.SQL("{}.{}")
        .format(
            sql.Identifier(schema.schema_name),
            sql.Identifier(schema.table_name),
        )
        .as_string()
    )

    # INSERT INTO / INSERT OVERWRITE
    parts = []
    if is_overwrite:
        parts.append(f"insert overwrite {full_table} (")
    else:
        parts.append(f"insert into {full_table} (")

    # Column list
    col_ids = sql.SQL(",").join(sql.Identifier(c) for c in column_names)
    parts.append(col_ids.as_string())
    parts.append(") ")

    # SELECT clause (with json/jsonb casts)
    parts.append("select ")
    select_items = []
    for i, col_name in enumerate(column_names):
        quoted = sql.Identifier(col_name).as_string()
        tn = type_names[i]
        if tn in ("json", "jsonb"):
            select_items.append(f"{quoted}::{tn}")
        else:
            select_items.append(quoted)
    parts.append(", ".join(select_items))

    # FROM external_files with stage paths
    stage_paths = ",".join(f"internal_stage://{s}" for s in stages)
    parts.append(f" from external_files(path='{stage_paths}') as (")

    # AS clause (with json/jsonb -> text remapping)
    as_items = []
    for i, col_name in enumerate(column_names):
        quoted = sql.Identifier(col_name).as_string()
        tn = type_names[i]
        if tn in ("json", "jsonb"):
            tn = "text"
        as_items.append(f"{quoted} {tn}")
    parts.append(", ".join(as_items))
    parts.append(")")

    # ON CONFLICT clause (only for non-overwrite with PK)
    if not is_overwrite and schema.has_primary_key:
        pk_ids = sql.SQL(",").join(sql.Identifier(pk) for pk in schema.primary_keys)
        parts.append(f" on conflict ({pk_ids.as_string()}) do ")
        if conflict_action == OnConflictAction.INSERT_OR_IGNORE:
            parts.append("nothing")
        else:
            # UPDATE SET col=excluded.col for all columns
            set_items = []
            for col_name in column_names:
                qc = sql.Identifier(col_name).as_string()
                set_items.append(f"{qc}=excluded.{qc}")
            parts.append("update set ")
            parts.append(",".join(set_items))

    return "".join(parts)


def build_create_stage_sql(stage_name: str, group_name: str, ttl_seconds: int) -> str:
    """Build SQL to create an internal stage.

    Returns SQL like:
        call hologres.hg_create_internal_stage('my_stage', 'default', 3600);
    """
    return (
        f"call hologres.hg_create_internal_stage("
        f"'{stage_name}', '{group_name}', {ttl_seconds});"
    )


def build_drop_stage_sql(stage_name: str) -> str:
    """Build SQL to drop an internal stage.

    Returns SQL like:
        call hologres.hg_drop_internal_stage('my_stage');
    """
    return f"call hologres.hg_drop_internal_stage('{stage_name}');"
