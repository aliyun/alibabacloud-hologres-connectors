"""Unit tests for multi-process write support."""

import pickle

import pytest

from hologres import Column, HoloConfig, Record, TableName, TableSchema
from hologres._shard import compute_shard
from hologres.column import BIGINT, INTEGER, VARCHAR
from hologres.types import MutationType


def _create_schema(table_name: str = "test_table") -> TableSchema:
    """Create a test schema with distribution key."""
    tn = TableName.valueOf(table_name)
    columns = [
        Column(
            name="id",
            type_name="int4",
            type=INTEGER,
            allow_null=False,
            is_primary_key=True,
        ),
        Column(name="name", type_name="text", type=VARCHAR, allow_null=True),
        Column(name="value", type_name="int8", type=BIGINT, allow_null=True),
    ]
    return TableSchema(tn, columns)


class TestRecordPickle:
    """Tests for Record pickling (required for multi-process IPC)."""

    def test_record_pickle_roundtrip(self):
        schema = _create_schema()
        record = Record(schema)
        record.set_object("id", 42)
        record.set_object("name", "hello")
        record.set_object("value", 12345)

        data = pickle.dumps(record)
        restored = pickle.loads(data)

        assert restored.values[0] == 42
        assert restored.values[1] == "hello"
        assert restored.values[2] == 12345
        assert restored.type == MutationType.INSERT
        assert restored._set_columns == {0, 1, 2}
        assert restored._futures == []

    def test_record_pickle_strips_futures(self):
        """Verify _futures is always empty after unpickling."""
        import concurrent.futures

        schema = _create_schema()
        record = Record(schema)
        record.set_object("id", 1)
        # Simulate a future being attached (as putAsync would do)
        record._futures = [concurrent.futures.Future()]

        data = pickle.dumps(record)
        restored = pickle.loads(data)

        assert restored._futures == []

    def test_record_pickle_none_values(self):
        schema = _create_schema()
        record = Record(schema)
        record.set_object("id", 1)
        # name and value are None (not set)

        data = pickle.dumps(record)
        restored = pickle.loads(data)

        assert restored.values[0] == 1
        assert restored.values[1] is None
        assert restored.values[2] is None
        assert restored._set_columns == {0}

    def test_record_pickle_delete_type(self):
        schema = _create_schema()
        record = Record(schema)
        record.set_object("id", 1)
        record.type = MutationType.DELETE

        data = pickle.dumps(record)
        restored = pickle.loads(data)

        assert restored.type == MutationType.DELETE

    def test_schema_pickle_roundtrip(self):
        """TableSchema must also survive pickling (it's embedded in Record)."""
        schema = _create_schema()

        data = pickle.dumps(schema)
        restored = pickle.loads(data)

        assert restored.column_count == 3
        assert restored.get_column(0).name == "id"
        assert restored.table_name_obj == TableName.valueOf("test_table")


class TestConfigMultiProcess:
    """Tests for write_parallelism config validation."""

    def test_default_write_parallelism(self):
        config = HoloConfig(
            host="localhost", port=80, database="db", username="u", password="p"
        )
        config.validate()
        assert config.write_parallelism == 4

    def test_write_parallelism_invalid(self):
        config = HoloConfig(
            host="localhost",
            port=80,
            database="db",
            username="u",
            password="p",
            write_parallelism=0,
        )
        with pytest.raises(ValueError, match="write_parallelism must be >= 1"):
            config.validate()


class TestShardRouting:
    """Tests for distribution key based process routing."""

    def test_deterministic_routing(self):
        """Same record always routes to the same shard."""
        schema = _create_schema()
        record = Record(schema)
        record.set_object("id", 42)

        shard1 = compute_shard(record, 4)
        shard2 = compute_shard(record, 4)
        assert shard1 == shard2
        assert 0 <= shard1 < 4

    def test_different_keys_distribute(self):
        """Different distribution keys should spread across shards."""
        schema = _create_schema()
        shards_seen = set()
        for i in range(100):
            record = Record(schema)
            record.set_object("id", i)
            shard = compute_shard(record, 4)
            shards_seen.add(shard)

        # With 100 different keys and 4 shards, we expect all shards used
        assert len(shards_seen) == 4

    def test_single_process_always_zero(self):
        """With 1 process, all records go to shard 0."""
        schema = _create_schema()
        for i in range(10):
            record = Record(schema)
            record.set_object("id", i)
            assert compute_shard(record, 1) == 0
