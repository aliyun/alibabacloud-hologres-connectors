"""Unit tests for Get class."""

import pytest

from hologres import Column, Get, TableSchema, TableName
from hologres.column import INTEGER, VARCHAR


class TestGet:
    """Tests for Get class."""

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
            Column(name="value", type_name="text", type=VARCHAR),
        ]
        return TableSchema(table_name, columns)

    def test_get_initialization(self):
        """Test Get initialization."""
        schema = self._create_schema()
        get = Get(schema)

        assert get.schema is schema
        assert get.record.schema is schema
        assert get.full_column is True
        assert len(get.selected_columns) == 3  # All columns by default

    def test_get_set_primary_key_by_name(self):
        """Test setting primary key by name."""
        schema = self._create_schema()
        get = Get(schema)

        get.set_primary_key("id", 42)
        assert get.record.get_object("id") == 42

    def test_get_set_primary_key_by_index(self):
        """Test setting primary key by index."""
        schema = self._create_schema()
        get = Get(schema)

        get.set_primary_key(0, 42)
        assert get.record.get_object(0) == 42

    def test_get_set_primary_key_invalid_name(self):
        """Test that setting invalid column name raises ValueError."""
        schema = self._create_schema()
        get = Get(schema)

        with pytest.raises(ValueError, match="not found"):
            get.set_primary_key("nonexistent", 1)

    def test_get_set_primary_key_non_pk_column(self):
        """Test that setting non-PK column as primary key raises ValueError."""
        schema = self._create_schema()
        get = Get(schema)

        with pytest.raises(ValueError, match="is not a primary key"):
            get.set_primary_key("name", "Alice")

    def test_get_set_primary_key_none_raises(self):
        """Test that setting None as primary key raises ValueError."""
        schema = self._create_schema()
        get = Get(schema)

        with pytest.raises(ValueError, match="cannot be None"):
            get.set_primary_key("id", None)

    def test_get_set_primary_key_returns_self(self):
        """Test that set_primary_key returns self for chaining."""
        schema = self._create_schema()
        get = Get(schema)

        result = get.set_primary_key("id", 42)
        assert result is get

    def test_get_add_select_column(self):
        """Test adding columns to select."""
        schema = self._create_schema()
        get = Get(schema)

        get.add_select_column("name")
        assert get.full_column is False
        assert 1 in get.selected_columns
        assert len(get.selected_columns) == 1

    def test_get_add_select_column_by_index(self):
        """Test adding columns to select by index."""
        schema = self._create_schema()
        get = Get(schema)

        get.add_select_column(0)
        assert 0 in get.selected_columns

    def test_get_add_select_columns_multiple(self):
        """Test adding multiple columns to select."""
        schema = self._create_schema()
        get = Get(schema)

        get.add_select_columns(["id", "name"])
        assert 0 in get.selected_columns
        assert 1 in get.selected_columns
        assert len(get.selected_columns) == 2

    def test_get_selected_columns_full_column(self):
        """Test selected_columns when full_column is True."""
        schema = self._create_schema()
        get = Get(schema)

        # When full_column is True, all columns should be selected
        assert get.selected_columns == {0, 1, 2}

    def test_get_add_select_column_invalid(self):
        """Test that adding invalid column raises ValueError."""
        schema = self._create_schema()
        get = Get(schema)

        with pytest.raises(ValueError, match="not found"):
            get.add_select_column("nonexistent")


class TestGetBuilder:
    """Tests for GetBuilder class."""

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

    def _create_composite_pk_schema(self) -> TableSchema:
        """Create a schema with composite primary key."""
        table_name = TableName.valueOf("test_table")
        columns = [
            Column(name="id1", type_name="int4", type=INTEGER, is_primary_key=True),
            Column(name="id2", type_name="int4", type=INTEGER, is_primary_key=True),
            Column(name="value", type_name="text", type=VARCHAR),
        ]
        return TableSchema(table_name, columns)

    def _create_no_pk_schema(self) -> TableSchema:
        """Create a schema without primary key."""
        table_name = TableName.valueOf("test_table")
        columns = [
            Column(name="id", type_name="int4", type=INTEGER),
            Column(name="name", type_name="text", type=VARCHAR),
        ]
        return TableSchema(table_name, columns)

    def test_builder_requires_pk(self):
        """Test that builder requires a table with primary key."""
        schema = self._create_no_pk_schema()

        with pytest.raises(ValueError, match="must have a primary key"):
            Get.builder(schema)

    def test_builder_set_primary_key(self):
        """Test setting primary key via builder."""
        schema = self._create_schema()
        get = Get.builder(schema).set_primary_key("id", 42).build()

        assert get.record.get_object("id") == 42

    def test_builder_with_selected_column(self):
        """Test selecting single column via builder."""
        schema = self._create_schema()
        get = (
            Get.builder(schema)
            .set_primary_key("id", 42)
            .with_selected_column("name")
            .build()
        )

        assert 1 in get.selected_columns
        assert get.full_column is False

    def test_builder_with_selected_columns(self):
        """Test selecting multiple columns via builder."""
        schema = self._create_schema()
        get = (
            Get.builder(schema)
            .set_primary_key("id", 42)
            .with_selected_columns(["id", "name"])
            .build()
        )

        assert 0 in get.selected_columns
        assert 1 in get.selected_columns

    def test_builder_validates_all_pk_set(self):
        """Test that builder validates all PK columns are set."""
        schema = self._create_composite_pk_schema()

        with pytest.raises(ValueError, match="has not been set"):
            (Get.builder(schema).set_primary_key("id1", 1).build())  # Missing id2

    def test_builder_composite_pk(self):
        """Test builder with composite primary key."""
        schema = self._create_composite_pk_schema()
        get = (
            Get.builder(schema)
            .set_primary_key("id1", 1)
            .set_primary_key("id2", 2)
            .build()
        )

        assert get.record.get_object("id1") == 1
        assert get.record.get_object("id2") == 2

    def test_get_repr(self):
        """Test string representation of Get."""
        schema = self._create_schema()
        get = Get(schema)
        get.set_primary_key("id", 42)
        assert str(get) == 'Get("public"."test_table", pk={\'id\': 42})'
