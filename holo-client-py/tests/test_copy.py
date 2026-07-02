"""Unit tests for COPY-related classes."""

import pytest

from hologres import (
    Column,
    CopyFormat,
    CopyMode,
    OnConflictAction,
    TableSchema,
    TableName,
)
from hologres.column import INTEGER, VARCHAR
from hologres.copy import _build_copy_sql, _encode_binary_header, _record_to_row


class TestCopyFormat:
    """Tests for CopyFormat enum."""

    def test_text_format(self):
        """Test TEXT format value."""
        assert CopyFormat.TEXT.value == "text"

    def test_binary_format(self):
        """Test BINARY format value."""
        assert CopyFormat.BINARY.value == "binary"


class TestCopyMode:
    """Tests for CopyMode enum."""

    def test_stream_mode(self):
        """Test STREAM mode value."""
        assert CopyMode.STREAM.value == "stream"

    def test_bulk_load_mode(self):
        """Test BULK_LOAD mode value."""
        assert CopyMode.BULK_LOAD.value == "bulk_load"

    def test_bulk_load_on_conflict_mode(self):
        """Test BULK_LOAD_ON_CONFLICT mode value."""
        assert CopyMode.BULK_LOAD_ON_CONFLICT.value == "bulk_load_on_conflict"


class TestBuildCopySql:
    """Tests for _build_copy_sql function."""

    def _create_schema(self) -> TableSchema:
        """Create a test schema with primary key."""
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

    def _create_schema_no_pk(self) -> TableSchema:
        """Create a test schema without primary key."""
        table_name = TableName.valueOf("test_table")
        columns = [
            Column(name="id", type_name="int4", type=INTEGER),
            Column(name="name", type_name="text", type=VARCHAR),
        ]
        return TableSchema(table_name, columns)

    def test_build_copy_sql_stream_mode(self):
        """Test building COPY SQL for STREAM mode."""
        schema = self._create_schema()
        sql = _build_copy_sql(
            table_name=schema.table_name_obj,
            columns=["id", "name"],
            fmt=CopyFormat.TEXT,
            mode=CopyMode.STREAM,
            on_conflict=OnConflictAction.INSERT_OR_REPLACE,
            has_pk=True,
        )
        sql_str = sql.as_string()
        # Note: psycopg wraps plain strings in sql.Literal, adding quotes
        assert sql_str == (
            'COPY "public"."test_table"("id", "name") '
            "FROM STDIN WITH (FORMAT 'text', STREAM_MODE 'true', ON_CONFLICT 'update')"
        )

    def test_build_copy_sql_bulk_mode(self):
        """Test building COPY SQL for BULK_LOAD mode."""
        schema = self._create_schema()
        sql = _build_copy_sql(
            table_name=schema.table_name_obj,
            columns=["id", "name"],
            fmt=CopyFormat.TEXT,
            mode=CopyMode.BULK_LOAD,
            on_conflict=OnConflictAction.INSERT_OR_REPLACE,
            has_pk=True,
        )
        sql_str = sql.as_string()
        assert sql_str == (
            'COPY "public"."test_table"("id", "name") '
            "FROM STDIN WITH (FORMAT 'text', STREAM_MODE 'false')"
        )

    def test_build_copy_sql_binary_format(self):
        """Test building COPY SQL for BINARY format."""
        schema = self._create_schema()
        sql = _build_copy_sql(
            table_name=schema.table_name_obj,
            columns=["id", "name"],
            fmt=CopyFormat.BINARY,
            mode=CopyMode.STREAM,
            on_conflict=OnConflictAction.INSERT_OR_REPLACE,
            has_pk=True,
        )
        sql_str = sql.as_string()
        assert sql_str == (
            'COPY "public"."test_table"("id", "name") '
            "FROM STDIN WITH (FORMAT 'binary', STREAM_MODE 'true', ON_CONFLICT 'update')"
        )

    def test_build_copy_sql_on_conflict_ignore(self):
        """Test building COPY SQL with ON CONFLICT ignore."""
        schema = self._create_schema()
        sql = _build_copy_sql(
            table_name=schema.table_name_obj,
            columns=["id", "name"],
            fmt=CopyFormat.TEXT,
            mode=CopyMode.STREAM,
            on_conflict=OnConflictAction.INSERT_OR_IGNORE,
            has_pk=True,
        )
        sql_str = sql.as_string()
        assert sql_str == (
            'COPY "public"."test_table"("id", "name") '
            "FROM STDIN WITH (FORMAT 'text', STREAM_MODE 'true', ON_CONFLICT 'ignore')"
        )

    def test_build_copy_sql_on_conflict_update(self):
        """Test building COPY SQL with ON CONFLICT update."""
        schema = self._create_schema()
        sql = _build_copy_sql(
            table_name=schema.table_name_obj,
            columns=["id", "name"],
            fmt=CopyFormat.TEXT,
            mode=CopyMode.STREAM,
            on_conflict=OnConflictAction.INSERT_OR_UPDATE,
            has_pk=True,
        )
        sql_str = sql.as_string()
        assert sql_str == (
            'COPY "public"."test_table"("id", "name") '
            "FROM STDIN WITH (FORMAT 'text', STREAM_MODE 'true', ON_CONFLICT 'update')"
        )

    def test_build_copy_sql_no_pk(self):
        """Test building COPY SQL for table without PK."""
        schema = self._create_schema_no_pk()
        sql = _build_copy_sql(
            table_name=schema.table_name_obj,
            columns=["id", "name"],
            fmt=CopyFormat.TEXT,
            mode=CopyMode.STREAM,
            on_conflict=OnConflictAction.INSERT_OR_REPLACE,
            has_pk=False,
        )
        sql_str = sql.as_string()
        assert sql_str == (
            'COPY "public"."test_table"("id", "name") '
            "FROM STDIN WITH (FORMAT 'text', STREAM_MODE 'true')"
        )

    def test_build_copy_sql_bulk_on_conflict_mode(self):
        """Test building COPY SQL for BULK_LOAD_ON_CONFLICT mode."""
        schema = self._create_schema()
        sql = _build_copy_sql(
            table_name=schema.table_name_obj,
            columns=["id", "name"],
            fmt=CopyFormat.TEXT,
            mode=CopyMode.BULK_LOAD_ON_CONFLICT,
            on_conflict=OnConflictAction.INSERT_OR_UPDATE,
            has_pk=True,
        )
        sql_str = sql.as_string()
        # BULK_LOAD_ON_CONFLICT has stream_mode=false but still has ON_CONFLICT
        assert sql_str == (
            'COPY "public"."test_table"("id", "name") '
            "FROM STDIN WITH (FORMAT 'text', STREAM_MODE 'false', ON_CONFLICT 'update')"
        )


class TestEncodeBinaryHeader:
    """Tests for _encode_binary_header function."""

    def test_encode_binary_header(self):
        """Test that binary header is correctly encoded."""
        header = _encode_binary_header()

        # Should start with PGCOPY signature
        assert header.startswith(b"PGCOPY\n")
        # Should have flags and extension length
        assert len(header) == 19  # 11 (signature) + 4 (flags) + 4 (ext length)


class TestRecordToRow:
    """Tests for _record_to_row function."""

    def _create_schema(self) -> TableSchema:
        """Create a test schema."""
        table_name = TableName.valueOf("test_table")
        columns = [
            Column(name="id", type_name="int4", type=INTEGER),
            Column(name="name", type_name="text", type=VARCHAR),
            Column(name="age", type_name="int4", type=INTEGER),
        ]
        return TableSchema(table_name, columns)

    def test_record_to_row_all_set(self):
        """Test converting record with all columns set."""
        from hologres import Record

        schema = self._create_schema()
        record = Record(schema)
        record.set_object(0, 1)
        record.set_object(1, "Alice")
        record.set_object(2, 30)

        row = _record_to_row(record, [0, 1, 2])

        assert row == (1, "Alice", 30)

    def test_record_to_row_partial_set(self):
        """Test converting record with some columns unset."""
        from hologres import Record

        schema = self._create_schema()
        record = Record(schema)
        record.set_object(0, 1)
        # name not set
        record.set_object(2, 30)

        row = _record_to_row(record, [0, 1, 2])

        assert row == (1, None, 30)

    def test_record_to_row_specific_columns(self):
        """Test converting record with specific column order."""
        from hologres import Record

        schema = self._create_schema()
        record = Record(schema)
        record.set_object(0, 1)
        record.set_object(1, "Alice")
        record.set_object(2, 30)

        # Only get id and age in that order
        row = _record_to_row(record, [0, 2])

        assert row == (1, 30)
