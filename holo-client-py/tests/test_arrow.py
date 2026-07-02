"""Unit tests for Arrow type mapping and serialization."""

import pytest

pa = pytest.importorskip("pyarrow")

from hologres import Column, Record, TableName, TableSchema
from hologres.column import (
    ARRAY,
    BIGINT,
    BINARY,
    BIT,
    BOOLEAN,
    CHAR,
    DATE,
    DECIMAL,
    DOUBLE,
    INTEGER,
    NUMERIC,
    OTHER,
    REAL,
    SMALLINT,
    TIME,
    TIMESTAMP,
    TIMESTAMP_WITH_TIMEZONE,
    VARCHAR,
)
from hologres._arrow import ArrowBatchWriter, build_arrow_schema, column_to_arrow_field


class TestColumnToArrowField:
    def test_smallint(self):
        col = Column(name="x", type_name="int2", type=SMALLINT)
        assert column_to_arrow_field(col).type == pa.int16()

    def test_integer(self):
        col = Column(name="x", type_name="int4", type=INTEGER)
        assert column_to_arrow_field(col).type == pa.int32()

    def test_bigint(self):
        col = Column(name="x", type_name="int8", type=BIGINT)
        assert column_to_arrow_field(col).type == pa.int64()

    def test_real(self):
        col = Column(name="x", type_name="float4", type=REAL)
        assert column_to_arrow_field(col).type == pa.float32()

    def test_double(self):
        col = Column(name="x", type_name="float8", type=DOUBLE)
        assert column_to_arrow_field(col).type == pa.float64()

    def test_decimal(self):
        col = Column(name="x", type_name="numeric", type=NUMERIC, precision=10, scale=2)
        field = column_to_arrow_field(col)
        assert field.type == pa.decimal128(10, 2)

    def test_boolean(self):
        col = Column(name="x", type_name="bool", type=BOOLEAN)
        assert column_to_arrow_field(col).type == pa.bool_()

    def test_bit(self):
        col = Column(name="x", type_name="bit", type=BIT)
        assert column_to_arrow_field(col).type == pa.bool_()

    def test_char(self):
        col = Column(name="x", type_name="char", type=CHAR)
        assert column_to_arrow_field(col).type == pa.utf8()

    def test_varchar(self):
        col = Column(name="x", type_name="text", type=VARCHAR)
        assert column_to_arrow_field(col).type == pa.utf8()

    def test_binary(self):
        col = Column(name="x", type_name="bytea", type=BINARY)
        assert column_to_arrow_field(col).type == pa.binary()

    def test_timestamp(self):
        col = Column(name="x", type_name="timestamp", type=TIMESTAMP)
        assert column_to_arrow_field(col).type == pa.timestamp("us")

    def test_timestamptz(self):
        col = Column(name="x", type_name="timestamptz", type=TIMESTAMP_WITH_TIMEZONE)
        assert column_to_arrow_field(col).type == pa.date64()

    def test_date(self):
        col = Column(name="x", type_name="date", type=DATE)
        assert column_to_arrow_field(col).type == pa.date32()

    def test_time(self):
        col = Column(name="x", type_name="time", type=TIME)
        assert column_to_arrow_field(col).type == pa.time64("us")

    def test_timetz(self):
        col = Column(name="x", type_name="timetz", type=TIME)
        assert column_to_arrow_field(col).type == pa.binary(16)

    def test_array_int4(self):
        col = Column(name="x", type_name="_int4", type=ARRAY, is_array_type=True)
        field = column_to_arrow_field(col)
        assert pa.types.is_list(field.type)

    def test_array_text(self):
        col = Column(name="x", type_name="_text", type=ARRAY, is_array_type=True)
        field = column_to_arrow_field(col)
        assert pa.types.is_list(field.type)

    def test_roaringbitmap(self):
        col = Column(name="x", type_name="roaringbitmap", type=OTHER)
        assert column_to_arrow_field(col).type == pa.binary()

    def test_other_defaults_to_utf8(self):
        col = Column(name="x", type_name="unknown_type", type=OTHER)
        assert column_to_arrow_field(col).type == pa.utf8()

    def test_nullable(self):
        col = Column(name="x", type_name="int4", type=INTEGER)
        assert column_to_arrow_field(col).nullable is True


class TestBuildArrowSchema:
    def _make_schema(self):
        table_name = TableName.valueOf("test_table")
        columns = [
            Column(name="id", type_name="int4", type=INTEGER),
            Column(name="name", type_name="text", type=VARCHAR),
            Column(name="score", type_name="float8", type=DOUBLE),
        ]
        return TableSchema(table_name, columns)

    def test_all_columns(self):
        schema = self._make_schema()
        arrow_schema = build_arrow_schema(schema, ["id", "name", "score"])
        assert len(arrow_schema) == 3
        assert arrow_schema.field("id").type == pa.int32()
        assert arrow_schema.field("name").type == pa.utf8()
        assert arrow_schema.field("score").type == pa.float64()

    def test_subset(self):
        schema = self._make_schema()
        arrow_schema = build_arrow_schema(schema, ["id", "name"])
        assert len(arrow_schema) == 2

    def test_invalid_column(self):
        schema = self._make_schema()
        with pytest.raises(ValueError, match="not found"):
            build_arrow_schema(schema, ["nonexistent"])


class TestArrowBatchWriter:
    def _make_schema(self):
        table_name = TableName.valueOf("test_table")
        columns = [
            Column(name="id", type_name="int4", type=INTEGER),
            Column(name="name", type_name="text", type=VARCHAR),
        ]
        return TableSchema(table_name, columns)

    def test_basic_write(self):
        schema = self._make_schema()
        writer = ArrowBatchWriter(schema, ["id", "name"], [0, 1], max_batch_size=10)

        record = Record(schema)
        record.set_object(0, 1)
        record.set_object(1, "Alice")
        writer.put(record)

        data = writer.end_and_get_bytes()
        assert len(data) > 0

        # Verify the data is valid Arrow IPC
        reader = pa.ipc.open_stream(data)
        table = reader.read_all()
        assert table.num_rows == 1
        assert table.column("id").to_pylist() == [1]
        assert table.column("name").to_pylist() == ["Alice"]
        writer.close()

    def test_multiple_records(self):
        schema = self._make_schema()
        writer = ArrowBatchWriter(schema, ["id", "name"], [0, 1], max_batch_size=100)

        for i in range(50):
            record = Record(schema)
            record.set_object(0, i)
            record.set_object(1, f"name_{i}")
            writer.put(record)

        data = writer.end_and_get_bytes()
        reader = pa.ipc.open_stream(data)
        table = reader.read_all()
        assert table.num_rows == 50
        writer.close()

    def test_batch_splitting(self):
        schema = self._make_schema()
        writer = ArrowBatchWriter(schema, ["id", "name"], [0, 1], max_batch_size=5)

        for i in range(12):
            record = Record(schema)
            record.set_object(0, i)
            record.set_object(1, f"name_{i}")
            writer.put(record)

        # Should have written 2 batches (5+5), with 2 remaining
        assert writer.data_size > 0

        data = writer.end_and_get_bytes()
        reader = pa.ipc.open_stream(data)
        table = reader.read_all()
        assert table.num_rows == 12
        writer.close()

    def test_null_values(self):
        schema = self._make_schema()
        writer = ArrowBatchWriter(schema, ["id", "name"], [0, 1], max_batch_size=10)

        record = Record(schema)
        record.set_object(0, 1)
        # name not set -> None
        writer.put(record)

        data = writer.end_and_get_bytes()
        reader = pa.ipc.open_stream(data)
        table = reader.read_all()
        assert table.column("name").to_pylist() == [None]
        writer.close()

    def test_empty_returns_empty_bytes(self):
        schema = self._make_schema()
        writer = ArrowBatchWriter(schema, ["id", "name"], [0, 1])
        data = writer.end_and_get_bytes()
        assert data == b""
        writer.close()

    def test_reset_after_end(self):
        schema = self._make_schema()
        writer = ArrowBatchWriter(schema, ["id", "name"], [0, 1], max_batch_size=10)

        record = Record(schema)
        record.set_object(0, 1)
        record.set_object(1, "Alice")
        writer.put(record)

        data1 = writer.end_and_get_bytes()
        assert len(data1) > 0
        assert writer.data_size == 0

        # Write more data after reset
        record2 = Record(schema)
        record2.set_object(0, 2)
        record2.set_object(1, "Bob")
        writer.put(record2)

        data2 = writer.end_and_get_bytes()
        assert len(data2) > 0

        # Both should be independently valid
        t1 = pa.ipc.open_stream(data1).read_all()
        t2 = pa.ipc.open_stream(data2).read_all()
        assert t1.num_rows == 1
        assert t2.num_rows == 1
        writer.close()
