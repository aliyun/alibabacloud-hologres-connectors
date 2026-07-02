"""Unit tests for stage SQL builder functions."""

import pytest

from hologres import Column, OnConflictAction, TableName, TableSchema
from hologres.column import BIGINT, DECIMAL, INTEGER, NUMERIC, VARCHAR
from hologres._stage_sql import (
    _get_stage_type_name,
    build_copy_in_stage_sql,
    build_create_stage_sql,
    build_drop_stage_sql,
    build_insert_select_from_stage_sql,
)


class TestBuildCopyInStageSql:
    def test_basic(self):
        sql = build_copy_in_stage_sql("my_stage", "file_0.arrow")
        assert sql == (
            "copy external_files(path='internal_stage://my_stage/file_0.arrow') "
            "from stdin;"
        )

    def test_with_prefix(self):
        sql = build_copy_in_stage_sql("stage1", "tbl_abc123_3.arrow")
        assert "internal_stage://stage1/tbl_abc123_3.arrow" in sql


class TestBuildCreateDropStageSql:
    def test_create(self):
        sql = build_create_stage_sql("my_stage", "default", 3600)
        assert sql == (
            "call hologres.hg_create_internal_stage('my_stage', 'default', 3600);"
        )

    def test_drop(self):
        sql = build_drop_stage_sql("my_stage")
        assert sql == "call hologres.hg_drop_internal_stage('my_stage');"


class TestGetStageTypeName:
    def test_serial_to_int(self):
        col = Column(name="id", type_name="serial", type=INTEGER)
        assert _get_stage_type_name(col) == "int"

    def test_bigserial_to_bigint(self):
        col = Column(name="id", type_name="bigserial", type=BIGINT)
        assert _get_stage_type_name(col) == "bigint"

    def test_regular_int(self):
        col = Column(name="id", type_name="int4", type=INTEGER)
        assert _get_stage_type_name(col) == "int4"

    def test_decimal_with_precision(self):
        col = Column(
            name="amount", type_name="numeric", type=NUMERIC, precision=10, scale=2
        )
        assert _get_stage_type_name(col) == "decimal(10,2)"

    def test_decimal_defaults(self):
        col = Column(name="amount", type_name="numeric", type=NUMERIC)
        assert _get_stage_type_name(col) == "decimal(38,0)"

    def test_varchar(self):
        col = Column(name="name", type_name="text", type=VARCHAR)
        assert _get_stage_type_name(col) == "text"


def _make_schema(with_pk=True, with_json=False):
    table_name = TableName.valueOf("test_table")
    columns = [
        Column(
            name="id",
            type_name="int4",
            type=INTEGER,
            allow_null=False,
            is_primary_key=with_pk,
        ),
        Column(name="name", type_name="text", type=VARCHAR),
    ]
    if with_json:
        columns.append(Column(name="data", type_name="jsonb", type=VARCHAR))
    return TableSchema(table_name, columns)


class TestBuildInsertSelectFromStageSql:
    def test_basic_with_pk(self):
        schema = _make_schema(with_pk=True)
        sql = build_insert_select_from_stage_sql(
            schema,
            ["id", "name"],
            ["stage1"],
            OnConflictAction.INSERT_OR_REPLACE,
        )
        assert 'insert into "public"."test_table"' in sql
        assert 'select "id", "name"' in sql
        assert "internal_stage://stage1" in sql
        assert '"id" int4, "name" text' in sql
        assert 'on conflict ("id") do update set' in sql
        assert '"id"=excluded."id"' in sql
        assert '"name"=excluded."name"' in sql

    def test_no_pk(self):
        schema = _make_schema(with_pk=False)
        sql = build_insert_select_from_stage_sql(
            schema,
            ["id", "name"],
            ["stage1"],
            OnConflictAction.INSERT_OR_REPLACE,
        )
        assert "on conflict" not in sql

    def test_insert_or_ignore(self):
        schema = _make_schema(with_pk=True)
        sql = build_insert_select_from_stage_sql(
            schema,
            ["id", "name"],
            ["stage1"],
            OnConflictAction.INSERT_OR_IGNORE,
        )
        assert "do nothing" in sql

    def test_overwrite(self):
        schema = _make_schema(with_pk=True)
        sql = build_insert_select_from_stage_sql(
            schema,
            ["id", "name"],
            ["stage1"],
            is_overwrite=True,
        )
        assert "insert overwrite" in sql
        assert "on conflict" not in sql

    def test_multiple_stages(self):
        schema = _make_schema(with_pk=False)
        sql = build_insert_select_from_stage_sql(
            schema,
            ["id", "name"],
            ["s1", "s2", "s3"],
            OnConflictAction.INSERT_OR_REPLACE,
        )
        assert "internal_stage://s1,internal_stage://s2,internal_stage://s3" in sql

    def test_json_column(self):
        schema = _make_schema(with_pk=True, with_json=True)
        sql = build_insert_select_from_stage_sql(
            schema,
            ["id", "name", "data"],
            ["stage1"],
            OnConflictAction.INSERT_OR_REPLACE,
        )
        # SELECT should have ::jsonb cast
        assert '"data"::jsonb' in sql
        # AS clause should have text instead of jsonb
        assert '"data" text' in sql
