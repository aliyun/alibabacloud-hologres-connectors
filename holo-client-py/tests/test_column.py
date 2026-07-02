"""Unit tests for Column."""

import pytest

from hologres import Column
from hologres.column import (
    BOOLEAN,
    BIGINT,
    INTEGER,
    SMALLINT,
    DOUBLE,
    REAL,
    VARCHAR,
    CHAR,
    TIMESTAMP,
    DATE,
    BINARY,
    ARRAY,
    OTHER,
)


class TestColumn:
    """Tests for Column class."""

    def test_column_basic_properties(self):
        """Test basic column properties."""
        col = Column(
            name="my_column",
            type_name="int4",
            type=INTEGER,
            allow_null=True,
            is_primary_key=False,
        )
        assert col.name == "my_column"
        assert col.type_name == "int4"
        assert col.type == INTEGER
        assert col.allow_null is True
        assert col.is_primary_key is False
        assert col.is_serial is False

    def test_column_from_pg_type_name_varchar(self):
        """Test creating a column from PostgreSQL varchar type."""
        col = Column.from_pg_type_name("name_col", "varchar")
        assert col.name == "name_col"
        assert col.type == VARCHAR
        assert col.is_array_type is False

    def test_column_from_pg_type_name_int4(self):
        """Test creating a column from PostgreSQL int4 type."""
        col = Column.from_pg_type_name("id", "int4")
        assert col.name == "id"
        assert col.type == INTEGER

    def test_column_from_pg_type_name_int8(self):
        """Test creating a column from PostgreSQL int8 type."""
        col = Column.from_pg_type_name("big_id", "int8")
        assert col.name == "big_id"
        assert col.type == BIGINT

    def test_column_from_pg_type_name_text(self):
        """Test creating a column from PostgreSQL text type."""
        col = Column.from_pg_type_name("description", "text")
        assert col.name == "description"
        assert col.type == VARCHAR

    def test_column_from_pg_type_name_timestamp(self):
        """Test creating a column from PostgreSQL timestamp type."""
        col = Column.from_pg_type_name("created_at", "timestamp")
        assert col.name == "created_at"
        assert col.type == TIMESTAMP

    def test_column_from_pg_type_name_timestamptz(self):
        """Test creating a column from PostgreSQL timestamptz type."""
        col = Column.from_pg_type_name("updated_at", "timestamptz")
        assert col.name == "updated_at"
        assert col.type == 2014  # TIMESTAMP_WITH_TIMEZONE

    def test_column_from_pg_type_name_bool(self):
        """Test creating a column from PostgreSQL bool type."""
        col = Column.from_pg_type_name("is_active", "bool")
        assert col.name == "is_active"
        assert col.type == BOOLEAN

    def test_column_from_pg_type_name_float8(self):
        """Test creating a column from PostgreSQL float8 type."""
        col = Column.from_pg_type_name("score", "float8")
        assert col.name == "score"
        assert col.type == DOUBLE

    def test_column_from_pg_type_name_float4(self):
        """Test creating a column from PostgreSQL float4 type."""
        col = Column.from_pg_type_name("rate", "float4")
        assert col.name == "rate"
        assert col.type == REAL

    def test_column_from_pg_type_name_date(self):
        """Test creating a column from PostgreSQL date type."""
        col = Column.from_pg_type_name("birth_date", "date")
        assert col.name == "birth_date"
        assert col.type == DATE

    def test_column_from_pg_type_name_bytea(self):
        """Test creating a column from PostgreSQL bytea type."""
        col = Column.from_pg_type_name("data", "bytea")
        assert col.name == "data"
        assert col.type == BINARY

    def test_column_from_pg_type_name_array_int4(self):
        """Test creating a column from PostgreSQL int4[] array type."""
        col = Column.from_pg_type_name("ids", "int4[]")
        assert col.name == "ids"
        assert col.type == ARRAY
        assert col.is_array_type is True
        assert col.array_element_type == INTEGER

    def test_column_from_pg_type_name_array_text(self):
        """Test creating a column from PostgreSQL text[] array type."""
        col = Column.from_pg_type_name("tags", "text[]")
        assert col.name == "tags"
        assert col.type == ARRAY
        assert col.is_array_type is True
        assert col.array_element_type == VARCHAR

    def test_column_from_pg_type_name_array_with_prefix(self):
        """Test creating a column from PostgreSQL _int4 array type."""
        col = Column.from_pg_type_name("numbers", "_int4")
        assert col.name == "numbers"
        assert col.type == ARRAY
        assert col.is_array_type is True
        assert col.array_element_type == INTEGER

    def test_column_from_pg_type_name_unknown(self):
        """Test creating a column from unknown type defaults to OTHER."""
        col = Column.from_pg_type_name("custom", "custom_type")
        assert col.name == "custom"
        assert col.type == OTHER

    def test_column_from_pg_type_name_with_kwargs(self):
        """Test creating a column with additional kwargs."""
        col = Column.from_pg_type_name(
            "id",
            "int4",
            allow_null=False,
            is_primary_key=True,
            comment="Primary key",
        )
        assert col.name == "id"
        assert col.type == INTEGER
        assert col.allow_null is False
        assert col.is_primary_key is True
        assert col.comment == "Primary key"

    def test_column_is_serial(self):
        """Test is_serial property for serial types."""
        col = Column.from_pg_type_name("id", "serial")
        assert col.is_serial is True

        col = Column.from_pg_type_name("big_id", "bigserial")
        assert col.is_serial is True

        col = Column.from_pg_type_name("small_id", "smallserial")
        assert col.is_serial is True

        col = Column.from_pg_type_name("id", "int4")
        assert col.is_serial is False

    def test_column_repr(self):
        """Test string representation of Column."""
        col = Column(
            name="id",
            type_name="int4",
            type=INTEGER,
            allow_null=False,
            is_primary_key=True,
        )
        repr_str = repr(col)
        assert "id" in repr_str
        assert "int4" in repr_str
        assert "PK" in repr_str
        assert "NOT NULL" in repr_str
