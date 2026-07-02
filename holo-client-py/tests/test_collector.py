"""Unit tests for ActionCollector and RecordCollector."""

import time

import pytest

from hologres import (
    Column,
    Record,
    TableSchema,
    TableName,
    OnConflictAction,
    MutationType,
)
from hologres._collector import ActionCollector, RecordCollector
from hologres.column import INTEGER, VARCHAR


class TestRecordCollector:
    """Tests for RecordCollector class."""

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

    def _create_record(
        self, pk: int, name: str, mutation_type: MutationType = MutationType.INSERT
    ) -> Record:
        """Create a test record."""
        schema = self._create_schema()
        record = Record(schema)
        record.set_object(0, pk)
        record.set_object(1, name)
        record.type = mutation_type
        return record

    def test_collector_initialization(self):
        """Test collector initialization."""
        collector = RecordCollector()
        assert collector.size == 0
        assert collector.is_empty()
        assert collector.byte_size == 0

    def test_collector_append_insert(self):
        """Test appending an insert record."""
        collector = RecordCollector()
        record = self._create_record(1, "Alice")

        collector.append(record)

        assert collector.size == 1
        assert not collector.is_empty()

    def test_collector_append_delete(self):
        """Test appending a delete record."""
        collector = RecordCollector()
        record = self._create_record(1, "Alice", MutationType.DELETE)

        collector.append(record)

        assert collector.size == 1

    def test_collector_deduplication_replace(self):
        """Test deduplication with INSERT_OR_REPLACE (default)."""
        collector = RecordCollector(
            on_conflict=OnConflictAction.INSERT_OR_REPLACE,
            enable_deduplication=True,
        )

        record1 = self._create_record(1, "Alice")
        record2 = self._create_record(1, "Bob")

        collector.append(record1)
        collector.append(record2)  # Should replace record1

        assert collector.size == 1
        deletes, inserts = collector.get_records()
        assert len(inserts) == 1
        assert inserts[0].get_object("name") == "Bob"

    # TODO: Test INSERT_OR_REPLACE should add null for additional columns

    def test_collector_deduplication_update(self):
        """Test deduplication with INSERT_OR_UPDATE."""
        collector = RecordCollector(
            on_conflict=OnConflictAction.INSERT_OR_UPDATE,
            enable_deduplication=True,
        )

        # Create records with additional column
        schema = self._create_schema()
        record1 = Record(schema)
        record1.set_object(0, 1)
        record1.set_object(1, "Alice")
        record1.type = MutationType.INSERT

        record2 = Record(schema)
        record2.set_object(0, 1)
        record2.set_object(1, "Bob")  # Update name
        record2.type = MutationType.INSERT

        collector.append(record1)
        collector.append(record2)

        deletes, inserts = collector.get_records()
        assert len(inserts) == 1
        # With INSERT_OR_UPDATE, the second record's values should be merged
        assert inserts[0].get_object("name") == "Bob"

    def test_collector_deduplication_ignore(self):
        """Test deduplication with INSERT_OR_IGNORE."""
        collector = RecordCollector(
            on_conflict=OnConflictAction.INSERT_OR_IGNORE,
            enable_deduplication=True,
        )

        schema = self._create_schema()
        record1 = Record(schema)
        record1.set_object(0, 1)
        record1.set_object(1, "Alice")

        record2 = Record(schema)
        record2.set_object(0, 1)
        record2.set_object(1, "Bob")

        collector.append(record1)
        collector.append(record2)  # Should be ignored

        deletes, inserts = collector.get_records()
        assert len(inserts) == 1
        assert inserts[0].get_object("name") == "Alice"  # Original value kept

    def test_collector_delete_removes_insert(self):
        """Test that delete removes pending insert for same key."""
        collector = RecordCollector()

        schema = self._create_schema()
        insert_record = Record(schema)
        insert_record.set_object(0, 1)
        insert_record.set_object(1, "Alice")

        delete_record = Record(schema)
        delete_record.set_object(0, 1)
        delete_record.type = MutationType.DELETE

        collector.append(insert_record)
        collector.append(delete_record)

        deletes, inserts = collector.get_records()
        assert len(deletes) == 1
        assert len(inserts) == 0

    def test_collector_insert_removes_delete(self):
        """Test that insert removes pending delete for same key."""
        collector = RecordCollector()

        schema = self._create_schema()
        delete_record = Record(schema)
        delete_record.set_object(0, 1)
        delete_record.type = MutationType.DELETE

        insert_record = Record(schema)
        insert_record.set_object(0, 1)
        insert_record.set_object(1, "Alice")

        collector.append(delete_record)
        collector.append(insert_record)

        deletes, inserts = collector.get_records()
        assert len(deletes) == 0
        assert len(inserts) == 1

    def test_collector_batch_ready_by_size(self):
        """Test batch is ready when size threshold is met."""
        collector = RecordCollector(max_records=2)

        schema = self._create_schema()
        record1 = Record(schema)
        record1.set_object(0, 1)

        record2 = Record(schema)
        record2.set_object(0, 2)

        assert not collector.append(record1)  # Not ready
        assert collector.append(record2)  # Ready (size >= 2)

    def test_collector_batch_ready_by_byte_size(self):
        """Test batch is ready when byte size threshold is met."""
        collector = RecordCollector(max_byte_size=10)

        schema = self._create_schema()
        record = Record(schema)
        record.set_object(0, 1)
        record.set_object(1, "a" * 100)  # Large string

        # Byte size should exceed threshold
        assert collector.append(record)

    def test_collector_get_records_clears_buffer(self):
        """Test that get_records clears the buffer."""
        collector = RecordCollector()

        record = self._create_record(1, "Alice")
        collector.append(record)

        assert collector.size == 1
        deletes, inserts = collector.get_records()
        assert collector.size == 0
        assert collector.is_empty()

    def test_collector_clear(self):
        """Test clearing the collector."""
        collector = RecordCollector()

        record = self._create_record(1, "Alice")
        collector.append(record)
        collector.clear()

        assert collector.is_empty()
        assert collector.byte_size == 0


class TestActionCollector:
    """Tests for ActionCollector class."""

    def _create_schema(self, table_name: str = "test_table") -> TableSchema:
        """Create a test schema."""
        tn = TableName.valueOf(table_name)
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
        return TableSchema(tn, columns)

    def _create_record(self, table_name: str, pk: int, name: str) -> Record:
        """Create a test record."""
        schema = self._create_schema(table_name)
        record = Record(schema)
        record.set_object(0, pk)
        record.set_object(1, name)
        return record

    def test_collector_initialization(self):
        """Test ActionCollector initialization."""
        collector = ActionCollector()
        assert not collector.has_pending()
        assert collector.total_byte_size == 0

    def test_collector_append_single_table(self):
        """Test appending records to a single table."""
        collector = ActionCollector()
        record = self._create_record("test_table", 1, "Alice")

        collector.append(record)
        assert collector.has_pending()

    def test_collector_append_multiple_tables(self):
        """Test appending records to multiple tables."""
        collector = ActionCollector()

        record1 = self._create_record("table1", 1, "Alice")
        record2 = self._create_record("table2", 2, "Bob")

        collector.append(record1)
        collector.append(record2)

        assert collector.has_pending()
        # Both tables should have records
        tables = collector.get_flushable_tables(force=True)
        assert len(tables) == 2

    def test_collector_get_flushable_tables_force(self):
        """Test get_flushable_tables with force=True."""
        collector = ActionCollector()

        record = self._create_record("test_table", 1, "Alice")
        collector.append(record)

        tables = collector.get_flushable_tables(force=True)
        assert len(tables) == 1
        assert tables[0].table_name == "test_table"

    def test_collector_get_flushable_tables_not_ready(self):
        """Test get_flushable_tables when batch not ready."""
        collector = ActionCollector(max_records=100)

        record = self._create_record("test_table", 1, "Alice")
        collector.append(record)

        tables = collector.get_flushable_tables(force=False)
        # Not ready since we only have 1 record and threshold is 100
        assert len(tables) == 0

    def test_collector_get_flushable_tables_ready_by_size(self):
        """Test get_flushable_tables when batch is ready by size."""
        collector = ActionCollector(max_records=2)

        record1 = self._create_record("test_table", 1, "Alice")
        record2 = self._create_record("test_table", 2, "Bob")

        collector.append(record1)
        collector.append(record2)

        tables = collector.get_flushable_tables(force=False)
        assert len(tables) == 1

    def test_collector_get_records(self):
        """Test getting records for a specific table."""
        collector = ActionCollector()

        tn = TableName.valueOf("test_table")
        record = self._create_record("test_table", 1, "Alice")
        collector.append(record)

        schema, deletes, inserts = collector.get_records(tn)
        assert schema is not None
        assert len(inserts) == 1
        assert len(deletes) == 0

    def test_collector_get_records_empty_table(self):
        """Test getting records for a table with no records."""
        collector = ActionCollector()

        tn = TableName.valueOf("nonexistent")
        schema, deletes, inserts = collector.get_records(tn)

        assert schema is None
        assert len(deletes) == 0
        assert len(inserts) == 0

    def test_collector_total_byte_size(self):
        """Test total byte size calculation."""
        collector = ActionCollector()

        record1 = self._create_record("table1", 1, "Alice")
        record2 = self._create_record("table2", 2, "Bob")

        collector.append(record1)
        size1 = collector.total_byte_size

        collector.append(record2)
        size2 = collector.total_byte_size

        assert size2 > size1

    def test_collector_batch_ready_by_total_byte_size(self):
        """Test batch ready when total byte size exceeds threshold."""
        collector = ActionCollector(
            max_records=100,
            max_byte_size=10 * 1024 * 1024,
            max_total_byte_size=10,  # Very small threshold
        )

        record = self._create_record("test_table", 1, "a" * 100)
        # Should trigger batch ready due to total byte size
        assert collector.append(record)
