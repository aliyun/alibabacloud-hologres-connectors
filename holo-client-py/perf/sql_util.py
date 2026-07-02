"""DDL helpers for creating and managing test tables.

Mirrors Java SqlUtil.createTable().
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import psycopg
    from .config import PutTestConf

logger = logging.getLogger(__name__)


def create_table(conn: psycopg.Connection, conf: PutTestConf) -> None:
    """Create the test table, dropping it first if it exists.

    Generates DDL matching the Java SqlUtil.createTable().
    """
    parts = []
    parts.append("BEGIN;")
    parts.append(f"DROP TABLE IF EXISTS {conf.table_name};")

    # Column definitions
    cols = ["id int"]
    if conf.prefix_pk:
        cols.append("id1 int")
    for i in range(conf.column_count):
        cols.append(f"name{i} {conf.data_column_type}")
    if conf.addition_ts_column:
        cols.append("ts timestamptz not null")
    if conf.partition:
        cols.append("ds int not null")

    # Primary key
    if conf.has_pk:
        pk_cols = ["id"]
        if conf.prefix_pk:
            pk_cols.append("id1")
        if conf.partition:
            pk_cols.append("ds")
        cols.append(f"primary key({','.join(pk_cols)})")

    col_str = ",".join(cols)
    ddl = f"create table {conf.table_name}({col_str})"
    if conf.partition:
        ddl += " partition by list(ds)"
    parts.append(ddl + ";")

    # Table properties
    if not conf.has_pk or (conf.has_pk and conf.prefix_pk):
        parts.append(
            f"call set_table_property('{conf.table_name}','distribution_key','id');"
        )
    parts.append(
        f"call set_table_property('{conf.table_name}','orientation','{conf.orientation}');"
    )
    if conf.shard_count > 0:
        parts.append(
            f"call set_table_property('{conf.table_name}','shard_count','{conf.shard_count}');"
        )
    if not conf.enable_bitmap:
        parts.append(
            f"call set_table_property('{conf.table_name}','bitmap_columns','');"
        )

    parts.append("END;")

    sql = "\n".join(parts)
    logger.info("DDL:\n%s", sql)
    with conn.cursor() as cur:
        cur.execute(sql)


def vacuum_table(conn: psycopg.Connection, table_name: str) -> None:
    """Run VACUUM on the table."""
    logger.info("VACUUM %s", table_name)
    with conn.cursor() as cur:
        cur.execute(f"VACUUM {table_name}")


def drop_table(conn: psycopg.Connection, table_name: str) -> None:
    """Drop the table if it exists."""
    logger.info("DROP TABLE IF EXISTS %s", table_name)
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
