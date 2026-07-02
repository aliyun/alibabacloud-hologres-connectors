"""Unit tests for Put class."""

import pytest

from hologres import Column, Put, TableSchema, TableName
from hologres.column import INTEGER, VARCHAR
from hologres.types import MutationType


class TestPut:
    """Tests for Put class."""

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
            Column(name="name", type_name="text", type=VARCHAR, allow_null=True),
            Column(name="age", type_name="int4", type=INTEGER, allow_null=True),
        ]
        return TableSchema(table_name, columns)

    def test_put_from_schema(self):
        """Test creating Put from TableSchema."""
        schema = self._create_schema()
        put = Put(schema)

        assert put.schema is schema
        assert put.record.schema is schema
        assert put.mutation_type == MutationType.INSERT

    def test_put_from_record(self):
        """Test creating Put from Record."""
        schema = self._create_schema()
        from hologres import Record

        record = Record(schema)
        record.set_object(0, 1)

        put = Put(record)
        assert put.record is record
        assert put.schema is schema

    def test_put_invalid_type(self):
        """Test that creating Put with invalid type raises TypeError."""
        with pytest.raises(TypeError, match="Expected TableSchema or Record"):
            Put("invalid")

    def test_put_set_object_by_name(self):
        """Test setting column value by name."""
        schema = self._create_schema()
        put = Put(schema)

        put.set_object("id", 1)
        put.set_object("name", "Alice")

        assert put.get_object("id") == 1
        assert put.get_object("name") == "Alice"

    def test_put_set_object_by_index(self):
        """Test setting column value by index."""
        schema = self._create_schema()
        put = Put(schema)

        put.set_object(0, 1)
        put.set_object(1, "Alice")

        assert put.get_object(0) == 1
        assert put.get_object(1) == "Alice"

    def test_put_set_object_returns_self(self):
        """Test that set_object returns self for chaining."""
        schema = self._create_schema()
        put = Put(schema)

        result = put.set_object("id", 1).set_object("name", "Alice")
        assert result is put

    def test_put_set_object_invalid_column_name(self):
        """Test that setting with invalid column name raises ValueError."""
        schema = self._create_schema()
        put = Put(schema)

        with pytest.raises(ValueError, match="not found"):
            put.set_object("nonexistent", "value")

    def test_put_get_object(self):
        """Test getting column value."""
        schema = self._create_schema()
        put = Put(schema)
        put.set_object("id", 42)

        assert put.get_object("id") == 42
        assert put.get_object(0) == 42

    def test_put_is_set(self):
        """Test checking if column is set."""
        schema = self._create_schema()
        put = Put(schema)

        assert not put.is_set(0)
        put.set_object("id", 1)
        assert put.is_set(0)

    def test_put_mutation_type(self):
        """Test mutation type property."""
        schema = self._create_schema()
        put = Put(schema)

        assert put.mutation_type == MutationType.INSERT

        put.mutation_type = MutationType.DELETE
        assert put.mutation_type == MutationType.DELETE

    def test_put_only_insert_flag(self):
        """Test only_insert flag for columns."""
        schema = self._create_schema()
        put = Put(schema)

        put.set_object("id", 1)
        put.set_object("name", "Alice", only_insert=True)

        assert 1 in put.record.only_insert_columns

    def test_put_repr(self):
        """Test string representation."""
        schema = self._create_schema()
        put = Put(schema)
        put.set_object("id", 1)
        assert (
            str(put) == 'Put(Record("public"."test_table", set=[\'id\'], type=INSERT))'
        )
