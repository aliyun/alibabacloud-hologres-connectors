"""Unit tests for TableSchema."""

import pytest

from hologres import Column, TableName, TableSchema
from hologres.column import INTEGER, VARCHAR, BIGINT


class TestTableSchema:
    """Tests for TableSchema class."""

    def _create_simple_schema(self) -> TableSchema:
        """Create a simple test schema with id and name columns."""
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
                name="name",
                type_name="text",
                type=VARCHAR,
                allow_null=True,
                is_primary_key=False,
            ),
        ]
        return TableSchema(table_name, columns)

    def _create_composite_pk_schema(self) -> TableSchema:
        """Create a schema with composite primary key."""
        table_name = TableName.valueOf("test_table")
        columns = [
            Column(
                name="id1",
                type_name="int4",
                type=INTEGER,
                allow_null=False,
                is_primary_key=True,
            ),
            Column(
                name="id2",
                type_name="int4",
                type=INTEGER,
                allow_null=False,
                is_primary_key=True,
            ),
            Column(
                name="value",
                type_name="text",
                type=VARCHAR,
                allow_null=True,
                is_primary_key=False,
            ),
        ]
        return TableSchema(table_name, columns)

    def test_table_schema_with_pk(self):
        """Test that column_count returns correct number."""
        schema = self._create_simple_schema()
        assert schema.column_count == 2
        assert schema.column_names == ["id", "name"]
        assert schema.primary_keys == ["id"]
        assert schema.has_primary_key is True
        assert schema.pk_index == [0]

    def test_table_schema_composite_primary_key(self):
        """Test schema with composite primary key."""
        schema = self._create_composite_pk_schema()
        assert schema.primary_keys == ["id1", "id2"]
        assert schema.pk_index == [0, 1]
        assert schema.has_primary_key is True

    def test_table_schema_no_primary_key(self):
        """Test schema without primary key."""
        table_name = TableName.valueOf("test_table")
        columns = [
            Column(name="id", type_name="int4", type=INTEGER, allow_null=True),
            Column(name="name", type_name="text", type=VARCHAR, allow_null=True),
        ]
        schema = TableSchema(table_name, columns)
        assert schema.has_primary_key is False
        assert schema.primary_keys == []
        assert schema.pk_index == []

    def test_table_schema_get_column_index(self):
        """Test getting column index by name."""
        schema = self._create_simple_schema()
        assert schema.get_column_index("id") == 0
        assert schema.get_column_index("name") == 1
        assert schema.get_column_index("nonexistent") is None
        assert schema.get_column_index("ID") is None

    def test_table_schema_get_column(self):
        """Test getting column by index."""
        schema = self._create_simple_schema()
        col = schema.get_column(0)
        assert col.name == "id"
        assert col.type == INTEGER

        col = schema.get_column(1)
        assert col.name == "name"
        assert col.type == VARCHAR

    def test_table_schema_is_primary_key(self):
        """Test checking if a column is primary key."""
        schema = self._create_simple_schema()
        assert schema.is_primary_key("id") is True
        assert schema.is_primary_key("name") is False

    def test_table_schema_properties(self):
        """Test table schema property accessors."""
        schema = self._create_simple_schema()
        assert schema.schema_name == "public"
        assert schema.table_name == "test_table"

    def test_table_schema_distribution_keys(self):
        """Test schema with distribution keys."""
        table_name = TableName.valueOf("test_table")
        columns = [
            Column(name="id", type_name="int4", type=INTEGER),
            Column(name="name", type_name="text", type=VARCHAR),
        ]
        schema = TableSchema(table_name, columns, distribution_keys=["id"])
        assert schema.distribution_keys == ["id"]
        assert schema.distribution_key_index == [0]

    def test_table_schema_partition_column(self):
        """Test schema with partition column."""
        table_name = TableName.valueOf("test_table")
        columns = [
            Column(name="id", type_name="int4", type=INTEGER),
            Column(name="dt", type_name="text", type=VARCHAR),
        ]
        schema = TableSchema(table_name, columns, partition_column="dt")
        assert schema.partition_column == "dt"
        assert schema.partition_index == 1

    def test_table_schema_is_partition_parent_table(self):
        """Test is_partition_parent_table property."""
        table_name = TableName.valueOf("test_table")
        columns = [
            Column(name="id", type_name="int4", type=INTEGER),
            Column(name="dt", type_name="text", type=VARCHAR),
        ]
        schema_with_partition = TableSchema(table_name, columns, partition_column="dt")
        assert schema_with_partition.is_partition_parent_table is True

        schema_without_partition = TableSchema(table_name, columns)
        assert schema_without_partition.is_partition_parent_table is False

    def test_table_schema_equality(self):
        """Test TableSchema equality based on table_id and schema_version."""
        tn1 = TableName.valueOf("table1")
        tn2 = TableName.valueOf("table2")
        columns = [Column(name="id", type_name="int4", type=INTEGER)]

        schema1 = TableSchema(tn1, columns, table_id="123", schema_version="v1")
        schema2 = TableSchema(tn2, columns, table_id="123", schema_version="v1")
        schema3 = TableSchema(tn1, columns, table_id="456", schema_version="v1")

        assert schema1 == schema2  # Same table_id and schema_version
        assert schema1 != schema3  # Different table_id

    def test_table_schema_hash(self):
        """Test that TableSchema can be used as dict key."""
        tn = TableName.valueOf("test_table")
        columns = [Column(name="id", type_name="int4", type=INTEGER)]
        schema1 = TableSchema(tn, columns, table_id="123", schema_version="v1")
        schema2 = TableSchema(tn, columns, table_id="123", schema_version="v1")

        d = {schema1: "value"}
        assert d[schema2] == "value"

    def test_table_schema_column_types(self):
        """Test that column_types returns list of type codes."""
        schema = self._create_simple_schema()
        assert schema.column_types == [INTEGER, VARCHAR]

    def test_table_schema_repr(self):
        """Test string representation of TableSchema."""
        schema = self._create_simple_schema()
        assert (
            str(schema) == 'TableSchema("public"."test_table", columns=2, pk=[\'id\'])'
        )
