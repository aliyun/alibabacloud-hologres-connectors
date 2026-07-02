"""Unit tests for SQL generation functions."""

import pytest

from hologres import (
    Column,
    Record,
    TableSchema,
    TableName,
    OnConflictAction,
    MutationType,
)
from hologres._sql import (
    build_insert_sql,
    build_delete_sql,
    build_get_sql,
    build_scan_sql,
)
from hologres.scan import Scan, SortKeys
from hologres.column import INTEGER, VARCHAR, OTHER


class TestBuildInsertSql:
    """Tests for build_insert_sql function."""

    def _create_schema(self) -> TableSchema:
        """Create a test schema."""
        table_name = TableName.valueOf("test_table")
        columns = [
            Column(
                name="id",
                type_name="int4",
                type=INTEGER,
                allow_null=False,
                is_primary_key=True,
            ),
            Column(name="name", type_name="text", type=VARCHAR),
            Column(name="age", type_name="int4", type=INTEGER),
        ]
        return TableSchema(table_name, columns)

    def _to_str(self, result) -> str:
        """Convert SQL result to string (handles both str and Composed)."""
        if isinstance(result, str):
            return result
        return result.as_string()

    def test_build_insert_empty_records(self):
        """Test building INSERT with empty records list."""
        schema = self._create_schema()
        sql, params = build_insert_sql(schema, [], OnConflictAction.INSERT_OR_REPLACE)

        assert self._to_str(sql) == ""
        assert len(params) == 0

    def test_build_insert_single_record(self):
        """Test building INSERT for single record (unnest)."""
        schema = self._create_schema()
        record = Record(schema)
        record.set_object(0, 1)
        record.set_object(1, "Alice")
        record.set_object(2, 30)

        sql, params = build_insert_sql(
            schema, [record], OnConflictAction.INSERT_OR_REPLACE
        )

        sql_str = self._to_str(sql)
        assert 'INSERT INTO "public"."test_table"' in sql_str
        assert "unnest(%s::int4[])" in sql_str
        assert "unnest(%s::text[])" in sql_str
        assert 'ON CONFLICT ("id") DO UPDATE SET' in sql_str
        # Params are columnar: one list per column
        assert params == [[1], ["Alice"], [30]]

    def test_build_insert_multiple_records(self):
        """Test building INSERT for multiple records (unnest)."""
        schema = self._create_schema()

        record1 = Record(schema)
        record1.set_object(0, 1)
        record1.set_object(1, "Alice")

        record2 = Record(schema)
        record2.set_object(0, 2)
        record2.set_object(1, "Bob")

        sql, params = build_insert_sql(
            schema, [record1, record2], OnConflictAction.INSERT_OR_REPLACE
        )

        sql_str = self._to_str(sql)
        assert "SELECT unnest(%s::int4[]), unnest(%s::text[])" in sql_str
        assert 'ON CONFLICT ("id") DO UPDATE SET' in sql_str
        # Columnar params
        assert params == [[1, 2], ["Alice", "Bob"]]

    def test_build_insert_with_on_conflict_ignore(self):
        """Test building INSERT with ON CONFLICT DO NOTHING."""
        schema = self._create_schema()
        record = Record(schema)
        record.set_object(0, 1)
        record.set_object(1, "Alice")

        sql, params = build_insert_sql(
            schema, [record], OnConflictAction.INSERT_OR_IGNORE
        )

        sql_str = self._to_str(sql)
        assert 'ON CONFLICT ("id") DO NOTHING' in sql_str

    def test_build_insert_with_on_conflict_update(self):
        """Test building INSERT with ON CONFLICT DO UPDATE."""
        schema = self._create_schema()
        record = Record(schema)
        record.set_object(0, 1)
        record.set_object(1, "Alice")
        record.set_object(2, 30)

        sql, params = build_insert_sql(
            schema, [record], OnConflictAction.INSERT_OR_UPDATE
        )

        sql_str = self._to_str(sql)
        assert 'ON CONFLICT ("id") DO UPDATE SET' in sql_str
        assert '"name"=EXCLUDED."name"' in sql_str
        assert '"age"=EXCLUDED."age"' in sql_str

    def test_build_insert_with_on_conflict_replace(self):
        """Test building INSERT with ON CONFLICT DO UPDATE for replace."""
        schema = self._create_schema()
        record = Record(schema)
        record.set_object(0, 1)
        record.set_object(1, "Alice")

        sql, params = build_insert_sql(
            schema, [record], OnConflictAction.INSERT_OR_REPLACE
        )

        sql_str = self._to_str(sql)
        assert 'ON CONFLICT ("id") DO UPDATE SET' in sql_str

    def test_build_insert_partial_columns(self):
        """Test building INSERT with only some columns set."""
        schema = self._create_schema()
        record = Record(schema)
        record.set_object(0, 1)  # Only set id, not name or age

        sql, params = build_insert_sql(
            schema, [record], OnConflictAction.INSERT_OR_REPLACE
        )

        sql_str = self._to_str(sql)
        assert '"id"' in sql_str
        assert "unnest(%s::int4[])" in sql_str
        assert params == [[1]]

    def test_build_insert_with_only_insert_columns(self):
        """Test building INSERT with only_insert columns (excluded from UPDATE SET)."""
        schema = self._create_schema()
        record = Record(schema)
        record.set_object(0, 1)
        record.set_object(1, "Alice", only_insert=True)  # create_time-like column
        record.set_object(2, 30)

        sql, params = build_insert_sql(
            schema, [record], OnConflictAction.INSERT_OR_UPDATE
        )

        sql_str = self._to_str(sql)
        # name is excluded from UPDATE SET because only_insert=True
        assert '"name"=EXCLUDED."name"' not in sql_str
        assert '"age"=EXCLUDED."age"' in sql_str

    def test_build_insert_no_pk_table(self):
        """Test building INSERT for table without primary key."""
        table_name = TableName.valueOf("test_table")
        columns = [
            Column(name="id", type_name="int4", type=INTEGER),
            Column(name="name", type_name="text", type=VARCHAR),
        ]
        schema = TableSchema(table_name, columns)

        record = Record(schema)
        record.set_object(0, 1)
        record.set_object(1, "Alice")

        sql, params = build_insert_sql(
            schema, [record], OnConflictAction.INSERT_OR_REPLACE
        )

        sql_str = self._to_str(sql)
        # No ON CONFLICT for tables without PK
        assert "ON CONFLICT" not in sql_str
        assert "INSERT INTO" in sql_str

    def test_build_insert_fallback_to_values(self):
        """Test that unsupported column types fall back to VALUES-based insert."""
        table_name = TableName.valueOf("test_table")
        columns = [
            Column(
                name="id",
                type_name="int4",
                type=INTEGER,
                allow_null=False,
                is_primary_key=True,
            ),
            Column(name="geo", type_name="point", type=OTHER),  # unnest-unsupported
        ]
        schema = TableSchema(table_name, columns)

        record = Record(schema)
        record.set_object(0, 1)
        record.set_object(1, "(1,2)")

        sql, params = build_insert_sql(
            schema, [record], OnConflictAction.INSERT_OR_REPLACE
        )

        sql_str = self._to_str(sql)
        # Falls back to VALUES
        assert "VALUES" in sql_str
        assert "unnest" not in sql_str
        assert params == [1, "(1,2)"]


class TestBuildDeleteSql:
    """Tests for build_delete_sql function."""

    def _create_schema(self) -> TableSchema:
        """Create a test schema."""
        table_name = TableName.valueOf("test_table")
        columns = [
            Column(
                name="id",
                type_name="int4",
                type=INTEGER,
                allow_null=False,
                is_primary_key=True,
            ),
            Column(name="name", type_name="text", type=VARCHAR),
        ]
        return TableSchema(table_name, columns)

    def _to_str(self, result) -> str:
        if isinstance(result, str):
            return result
        return result.as_string()

    def test_build_delete_empty_records(self):
        """Test building DELETE with empty records list."""
        schema = self._create_schema()
        sql, params = build_delete_sql(schema, [])

        assert len(params) == 0

    def test_build_delete_single_record(self):
        """Test building DELETE for single record."""
        schema = self._create_schema()
        record = Record(schema)
        record.set_object(0, 1)
        record.type = MutationType.DELETE

        sql, params = build_delete_sql(schema, [record])

        sql_str = self._to_str(sql)
        assert sql_str == 'DELETE FROM "public"."test_table" WHERE ("id"=%s)'
        assert params == [1]

    def test_build_delete_multiple_records(self):
        """Test building DELETE for multiple records."""
        schema = self._create_schema()

        record1 = Record(schema)
        record1.set_object(0, 1)

        record2 = Record(schema)
        record2.set_object(0, 2)

        sql, params = build_delete_sql(schema, [record1, record2])

        sql_str = self._to_str(sql)
        assert (
            sql_str == 'DELETE FROM "public"."test_table" WHERE ("id"=%s) OR ("id"=%s)'
        )
        assert params == [1, 2]

    def test_build_delete_composite_pk(self):
        """Test building DELETE with composite primary key."""
        table_name = TableName.valueOf("test_table")
        columns = [
            Column(name="id1", type_name="int4", type=INTEGER, is_primary_key=True),
            Column(name="id2", type_name="int4", type=INTEGER, is_primary_key=True),
        ]
        schema = TableSchema(table_name, columns)

        record = Record(schema)
        record.set_object(0, 1)
        record.set_object(1, 2)

        sql, params = build_delete_sql(schema, [record])

        sql_str = self._to_str(sql)
        assert (
            sql_str == 'DELETE FROM "public"."test_table" WHERE ("id1"=%s AND "id2"=%s)'
        )
        assert params == [1, 2]


class TestBuildGetSql:
    """Tests for build_get_sql function."""

    def _create_schema(self) -> TableSchema:
        """Create a test schema."""
        table_name = TableName.valueOf("test_table")
        columns = [
            Column(
                name="id",
                type_name="int4",
                type=INTEGER,
                allow_null=False,
                is_primary_key=True,
            ),
            Column(name="name", type_name="text", type=VARCHAR),
            Column(name="age", type_name="int4", type=INTEGER),
        ]
        return TableSchema(table_name, columns)

    def _to_str(self, result) -> str:
        if isinstance(result, str):
            return result
        return result.as_string()

    def test_build_get_empty_records(self):
        """Test building SELECT with empty records list."""
        schema = self._create_schema()
        sql, params = build_get_sql(schema, [], {0, 1, 2})

        assert self._to_str(sql) == ""
        assert len(params) == 0

    def test_build_get_single_record(self):
        """Test building SELECT for single record."""
        schema = self._create_schema()
        record = Record(schema)
        record.set_object(0, 1)

        sql, params = build_get_sql(schema, [record], {0, 1, 2})

        sql_str = self._to_str(sql)
        assert (
            sql_str
            == 'SELECT "id", "name", "age" FROM "public"."test_table" WHERE ("id"=%s)'
        )
        assert params == [1]

    def test_build_get_multiple_records(self):
        """Test building SELECT for multiple records."""
        schema = self._create_schema()

        record1 = Record(schema)
        record1.set_object(0, 1)

        record2 = Record(schema)
        record2.set_object(0, 2)

        sql, params = build_get_sql(schema, [record1, record2], {0, 1, 2})

        sql_str = self._to_str(sql)
        assert (
            sql_str
            == 'SELECT "id", "name", "age" FROM "public"."test_table" WHERE ("id"=%s) OR ("id"=%s)'
        )
        assert params == [1, 2]

    def test_build_get_selected_columns(self):
        """Test building SELECT with specific columns."""
        schema = self._create_schema()
        record = Record(schema)
        record.set_object(0, 1)

        # Select only name column
        sql, params = build_get_sql(schema, [record], {1})

        sql_str = self._to_str(sql)
        assert sql_str == 'SELECT "name" FROM "public"."test_table" WHERE ("id"=%s)'
        assert params == [1]

    def test_build_get_composite_pk(self):
        """Test building SELECT with composite primary key."""
        table_name = TableName.valueOf("test_table")
        columns = [
            Column(name="id1", type_name="int4", type=INTEGER, is_primary_key=True),
            Column(name="id2", type_name="int4", type=INTEGER, is_primary_key=True),
            Column(name="value", type_name="text", type=VARCHAR),
        ]
        schema = TableSchema(table_name, columns)

        record = Record(schema)
        record.set_object(0, 1)
        record.set_object(1, 2)

        sql, params = build_get_sql(schema, [record], {0, 1, 2})

        sql_str = self._to_str(sql)
        assert (
            sql_str
            == 'SELECT "id1", "id2", "value" FROM "public"."test_table" WHERE ("id1"=%s AND "id2"=%s)'
        )
        assert params == [1, 2]


class TestBuildScanSql:
    """Tests for build_scan_sql function."""

    def _create_schema(self, clustering_keys=None) -> TableSchema:
        table_name = TableName.valueOf("test_table")
        columns = [
            Column(
                name="id",
                type_name="int4",
                type=INTEGER,
                allow_null=False,
                is_primary_key=True,
            ),
            Column(
                name="id1",
                type_name="int4",
                type=INTEGER,
                allow_null=False,
                is_primary_key=True,
            ),
            Column(name="name", type_name="text", type=VARCHAR),
            Column(name="age", type_name="int4", type=INTEGER),
        ]
        return TableSchema(table_name, columns, clustering_keys=clustering_keys)

    def _to_str(self, result) -> str:
        if isinstance(result, str):
            return result
        return result.as_string()

    def test_equal_filter(self):
        schema = self._create_schema()
        scan = (
            Scan.builder(schema)
            .add_equal_filter("id", 42)
            .set_sort_keys(SortKeys.NONE)
            .build()
        )
        stmt, params = build_scan_sql(scan)
        sql_str = self._to_str(stmt)
        assert (
            sql_str
            == 'SELECT "id", "id1", "name", "age" FROM "public"."test_table" WHERE "id" = %s'
        )
        assert params == [42]

    def test_range_filter(self):
        schema = self._create_schema()
        scan = (
            Scan.builder(schema)
            .add_range_filter(
                "age", start=10, end=50, start_inclusive=True, end_inclusive=False
            )
            .set_sort_keys(SortKeys.NONE)
            .build()
        )
        stmt, params = build_scan_sql(scan)
        sql_str = self._to_str(stmt)
        assert '"age" >= %s' in sql_str
        assert '"age" < %s' in sql_str
        assert params == [10, 50]

    def test_range_filter_exclusive_start_inclusive_end(self):
        schema = self._create_schema()
        scan = (
            Scan.builder(schema)
            .add_range_filter(
                "age", start=10, end=50, start_inclusive=False, end_inclusive=True
            )
            .set_sort_keys(SortKeys.NONE)
            .build()
        )
        stmt, params = build_scan_sql(scan)
        sql_str = self._to_str(stmt)
        assert '"age" > %s' in sql_str
        assert '"age" <= %s' in sql_str

    def test_multiple_filters(self):
        schema = self._create_schema()
        scan = (
            Scan.builder(schema)
            .add_equal_filter("id", 1)
            .add_range_filter("age", start=18)
            .set_sort_keys(SortKeys.NONE)
            .build()
        )
        stmt, params = build_scan_sql(scan)
        sql_str = self._to_str(stmt)
        assert '"id" = %s' in sql_str
        assert '"age" >= %s' in sql_str
        assert " AND " in sql_str
        assert params == [1, 18]

    def test_sort_keys_primary_key(self):
        schema = self._create_schema()
        scan = (
            Scan.builder(schema)
            .add_equal_filter("id", 1)
            .set_sort_keys(SortKeys.PRIMARY_KEY)
            .build()
        )
        stmt, params = build_scan_sql(scan)
        sql_str = self._to_str(stmt)
        assert 'ORDER BY "id", "id1"' in sql_str

    def test_sort_keys_clustering_key(self):
        schema = self._create_schema(clustering_keys=["name:asc", "age:desc"])
        scan = (
            Scan.builder(schema)
            .add_equal_filter("id", 1)
            .set_sort_keys(SortKeys.CLUSTERING_KEY)
            .build()
        )
        stmt, params = build_scan_sql(scan)
        sql_str = self._to_str(stmt)
        assert 'ORDER BY "name", "age"' in sql_str

    def test_sort_keys_clustering_key_missing_raises(self):
        schema = self._create_schema()  # no clustering keys
        scan = (
            Scan.builder(schema)
            .add_equal_filter("id", 1)
            .set_sort_keys(SortKeys.CLUSTERING_KEY)
            .build()
        )
        with pytest.raises(ValueError, match="clustering keys"):
            build_scan_sql(scan)

    def test_sort_keys_none(self):
        schema = self._create_schema()
        scan = (
            Scan.builder(schema)
            .add_equal_filter("id", 1)
            .set_sort_keys(SortKeys.NONE)
            .build()
        )
        stmt, params = build_scan_sql(scan)
        sql_str = self._to_str(stmt)
        assert "ORDER BY" not in sql_str

    def test_selected_columns(self):
        schema = self._create_schema()
        scan = (
            Scan.builder(schema)
            .add_equal_filter("id", 1)
            .with_selected_columns(["id", "name"])
            .set_sort_keys(SortKeys.NONE)
            .build()
        )
        stmt, params = build_scan_sql(scan)
        sql_str = self._to_str(stmt)
        assert (
            sql_str == 'SELECT "id", "name" FROM "public"."test_table" WHERE "id" = %s'
        )

    def test_no_filters_no_order(self):
        schema = self._create_schema()
        scan = Scan.builder(schema).set_sort_keys(SortKeys.NONE).build()
        stmt, params = build_scan_sql(scan)
        sql_str = self._to_str(stmt)
        assert sql_str == 'SELECT "id", "id1", "name", "age" FROM "public"."test_table"'
        assert params == []

    def test_no_filters_with_order(self):
        schema = self._create_schema()
        scan = Scan.builder(schema).set_sort_keys(SortKeys.PRIMARY_KEY).build()
        stmt, params = build_scan_sql(scan)
        sql_str = self._to_str(stmt)
        assert (
            sql_str
            == 'SELECT "id", "id1", "name", "age" FROM "public"."test_table" ORDER BY "id", "id1"'
        )
        assert params == []
