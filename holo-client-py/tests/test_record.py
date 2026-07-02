"""Unit tests for Record."""

import pytest

from hologres import Column, Record, TableSchema, TableName
from hologres.column import INTEGER, VARCHAR, BIGINT, BOOLEAN
from hologres.types import MutationType


class TestRecord:
    """Tests for Record class."""

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

    def test_record_initialization(self):
        """Test that Record is initialized correctly."""
        schema = self._create_schema()
        record = Record(schema)

        assert record.schema is schema
        assert record.table_name == TableName.valueOf("test_table")
        assert len(record.values) == 3
        assert all(v is None for v in record.values)
        assert record.type == MutationType.INSERT
        assert record.byte_size == 0
        assert len(record.set_columns) == 0

    def test_record_set_object_by_index(self):
        """Test setting column value by index."""
        schema = self._create_schema()
        record = Record(schema)

        record.set_object(0, 123)
        assert record.values[0] == 123
        assert record.is_set(0)
        assert 0 in record.set_columns

    def test_record_set_object_by_name(self):
        """Test setting column value by name."""
        schema = self._create_schema()
        record = Record(schema)

        record.set_object("name", "Alice")
        assert record.values[1] == "Alice"
        assert record.is_set(1)

    def test_record_get_object_by_index(self):
        """Test getting column value by index."""
        schema = self._create_schema()
        record = Record(schema)
        record.set_object(0, 123)

        assert record.get_object(0) == 123

    def test_record_get_object_by_name(self):
        """Test getting column value by name."""
        schema = self._create_schema()
        record = Record(schema)
        record.set_object(1, "Alice")

        assert record.get_object("name") == "Alice"

    def test_record_get_object_invalid_name_raises(self):
        """Test that getting with invalid name raises KeyError."""
        schema = self._create_schema()
        record = Record(schema)

        with pytest.raises(KeyError, match="not found"):
            record.get_object("nonexistent")

    def test_record_set_object_out_of_range(self):
        """Test that setting with invalid index raises IndexError."""
        schema = self._create_schema()
        record = Record(schema)

        with pytest.raises(IndexError, match="out of range"):
            record.set_object(100, "value")

    def test_record_is_set(self):
        """Test is_set method."""
        schema = self._create_schema()
        record = Record(schema)

        assert not record.is_set(0)
        record.set_object(0, 123)
        assert record.is_set(0)

    def test_record_length(self):
        """Test length property (number of set columns)."""
        schema = self._create_schema()
        record = Record(schema)

        assert record.length == 0
        record.set_object(0, 1)
        assert record.length == 1
        record.set_object(1, "test")
        assert record.length == 2

    def test_record_size(self):
        """Test size property (total columns)."""
        schema = self._create_schema()
        record = Record(schema)

        assert record.size == 3

    def test_record_get_key_values(self):
        """Test getting primary key values."""
        schema = self._create_schema()
        record = Record(schema)
        record.set_object(0, 42)

        assert record.get_key_values() == (42,)

    def test_record_get_key_values_composite(self):
        """Test getting composite primary key values."""
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

        assert record.get_key_values() == (1, 2)

    def test_record_clone(self):
        """Test cloning a record."""
        schema = self._create_schema()
        record = Record(schema)
        record.set_object(0, 123)
        record.set_object(1, "Alice")
        record.type = MutationType.DELETE

        cloned = record.clone()

        assert cloned.values == record.values
        assert cloned.set_columns == record.set_columns
        assert cloned.type == record.type
        assert cloned.byte_size == record.byte_size

        # Ensure it's a deep copy
        cloned.set_object(0, 999)
        assert record.values[0] == 123

    def test_record_merge(self):
        """Test merging records."""
        schema = self._create_schema()
        record1 = Record(schema)
        record1.set_object(0, 1)
        record1.set_object(1, "Alice")

        record2 = Record(schema)
        record2.set_object(0, 1)
        record2.set_object(2, 30)  # Set age

        record1.merge(record2)

        assert record1.values[0] == 1
        assert record1.values[1] == "Alice"
        assert record1.values[2] == 30

    def test_record_byte_size_estimation(self):
        """Test byte size estimation for different types."""
        schema = self._create_schema()
        record = Record(schema)

        # Integer should be 4 bytes
        record.set_object(0, 123)
        assert record.byte_size == 4

        # String should be its length
        record.set_object(1, "hello")
        assert record.byte_size == 4 + 5  # int + string length

    def test_record_byte_size_update_on_overwrite(self):
        """Test that byte size is updated when overwriting a value."""
        schema = self._create_schema()
        record = Record(schema)

        record.set_object(1, "hello")
        assert record.byte_size == 5

        record.set_object(1, "hi")  # Shorter string
        assert record.byte_size == 2

        record.set_object(1, "hello world")  # Longer string
        assert record.byte_size == 11

    def test_record_mutation_type(self):
        """Test mutation type property."""
        schema = self._create_schema()
        record = Record(schema)

        assert record.type == MutationType.INSERT

        record.type = MutationType.DELETE
        assert record.type == MutationType.DELETE

    def test_record_only_insert_columns(self):
        """Test only_insert_columns tracking."""
        schema = self._create_schema()
        record = Record(schema)

        record.set_object(0, 1)
        record.only_insert_columns.add(0)

        assert 0 in record.only_insert_columns

    def test_record_repr(self):
        """Test string representation."""
        # Note: The current implementation has a bug where __repr__ fails
        # due to TableName.__str__ returning a non-string. Skip for now.
        schema = self._create_schema()
        record = Record(schema)
        record.set_object(0, 1)
        record.set_object(1, "test")

        assert (
            str(record)
            == "Record(\"public\".\"test_table\", set=['id', 'name'], type=INSERT)"
        )


class TestRecordByteSizeEstimation:
    """Tests for Record byte size estimation with different types."""

    def _create_schema_with_types(self, columns_spec) -> TableSchema:
        """Create schema with specified column types."""
        table_name = TableName.valueOf("test_table")
        columns = []
        for name, type_code in columns_spec:
            type_name = {
                INTEGER: "int4",
                BIGINT: "int8",
                VARCHAR: "text",
                BOOLEAN: "bool",
            }.get(type_code, "text")
            columns.append(Column(name=name, type_name=type_name, type=type_code))
        return TableSchema(table_name, columns)

    def test_byte_size_integer(self):
        """Test byte size for INTEGER (4 bytes)."""
        schema = self._create_schema_with_types([("id", INTEGER)])
        record = Record(schema)
        record.set_object(0, 123)
        assert record.byte_size == 4

    def test_byte_size_bigint(self):
        """Test byte size for BIGINT (8 bytes)."""
        schema = self._create_schema_with_types([("id", BIGINT)])
        record = Record(schema)
        record.set_object(0, 123456789012)
        assert record.byte_size == 8

    def test_byte_size_boolean(self):
        """Test byte size for BOOLEAN (1 byte)."""
        schema = self._create_schema_with_types([("flag", BOOLEAN)])
        record = Record(schema)
        record.set_object(0, True)
        assert record.byte_size == 1

    def test_byte_size_null(self):
        """Test byte size for NULL (4 bytes)."""
        schema = self._create_schema_with_types([("id", INTEGER)])
        record = Record(schema)
        record.set_object(0, None)
        assert record.byte_size == 4

    def test_byte_size_string(self):
        """Test byte size for VARCHAR (length of string)."""
        schema = self._create_schema_with_types([("name", VARCHAR)])
        record = Record(schema)
        record.set_object(0, "hello")
        assert record.byte_size == 5

    def test_byte_size_empty_string(self):
        """Test byte size for empty string."""
        schema = self._create_schema_with_types([("name", VARCHAR)])
        record = Record(schema)
        record.set_object(0, "")
        assert record.byte_size == 0
