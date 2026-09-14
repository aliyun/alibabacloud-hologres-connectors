package com.alibaba.hologres.connector.flink.source.bulkread;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import com.alibaba.hologres.client.copy.CopyFormat;
import com.alibaba.hologres.client.copy.out.CopyOutWrapper;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.TableSchema;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Tests for {@link HologresCopyBulkReader}. */
public class HologresCopyBulkReaderTest {

    private BufferAllocator allocator;
    private HologresCopyBulkReader reader;
    private Method readFromArrowVector;

    @Before
    public void setUp() throws Exception {
        allocator = new RootAllocator(Long.MAX_VALUE);
        reader = new HologresCopyBulkReader(null, null, null, null, null, null, null, -1);
        readFromArrowVector =
                HologresCopyBulkReader.class.getDeclaredMethod(
                        "readFromArrowVector",
                        FieldVector.class,
                        int.class,
                        DataType.class,
                        String.class,
                        Column.class);
        readFromArrowVector.setAccessible(true);
    }

    @After
    public void tearDown() {
        allocator.close();
    }

    @Test
    public void testCopyFormat() {
        assertEquals(CopyFormat.ARROW_LZ4, HologresCopyBulkReader.getCopyFormat(true));
        assertEquals(CopyFormat.ARROW, HologresCopyBulkReader.getCopyFormat(false));
    }

    @Test
    public void testBuildCopyFilter() {
        assertNull(HologresCopyBulkReader.buildCopyFilter(null, -1));
        assertNull(HologresCopyBulkReader.buildCopyFilter("  ", -1));
        assertEquals("where id > 10", HologresCopyBulkReader.buildCopyFilter(" id > 10 ", -1));
        assertEquals("where true limit 100", HologresCopyBulkReader.buildCopyFilter(null, 100));
        assertEquals(
                "where id > 10 limit 100", HologresCopyBulkReader.buildCopyFilter("id > 10", 100));
    }

    @Test
    public void testBooleanConversions() throws Exception {
        try (BitVector bit = new BitVector("bit", allocator);
                UInt1Vector uint = new UInt1Vector("uint", allocator)) {
            bit.allocateNew(2);
            bit.setSafe(0, 1);
            bit.setSafe(1, 0);
            assertTrue((Boolean) read(bit, DataTypes.BOOLEAN(), "bool"));
            assertFalse((Boolean) read(bit, 1, DataTypes.BOOLEAN(), "bool"));

            uint.allocateNew(1);
            uint.setSafe(0, 1);
            assertTrue((Boolean) read(uint, DataTypes.BOOLEAN(), "bool"));
        }
    }

    @Test
    public void testIntegralConversions() throws Exception {
        try (SmallIntVector small = new SmallIntVector("small", allocator);
                IntVector integer = new IntVector("integer", allocator);
                BigIntVector bigint = new BigIntVector("bigint", allocator)) {
            small.allocateNew(1);
            small.setSafe(0, (short) 127);
            assertEquals((byte) 127, read(small, DataTypes.TINYINT(), "int2"));
            assertEquals((short) 127, read(small, DataTypes.SMALLINT(), "int2"));

            integer.allocateNew(1);
            integer.setSafe(0, 123456);
            assertEquals(123456, read(integer, DataTypes.INT(), "int4"));

            bigint.allocateNew(1);
            bigint.setSafe(0, Long.MAX_VALUE);
            assertEquals(Long.MAX_VALUE, read(bigint, DataTypes.BIGINT(), "int8"));
        }
    }

    @Test
    public void testFloatingPointConversions() throws Exception {
        try (Float4Vector float4 = new Float4Vector("float4", allocator);
                Float8Vector float8 = new Float8Vector("float8", allocator)) {
            float4.allocateNew(1);
            float4.setSafe(0, 1.25F);
            assertEquals(1.25F, (Float) read(float4, DataTypes.FLOAT(), "float4"), 0F);

            float8.allocateNew(1);
            float8.setSafe(0, 2.5D);
            assertEquals(2.5D, (Double) read(float8, DataTypes.DOUBLE(), "float8"), 0D);
        }
    }

    @Test
    public void testStringAndBinaryConversions() throws Exception {
        byte[] utf8 = "Hologres-中文-🙂".getBytes(StandardCharsets.UTF_8);
        byte[] binary = new byte[] {0, 1, -1, 127};
        try (VarCharVector varchar = new VarCharVector("varchar", allocator);
                VarBinaryVector varbinary = new VarBinaryVector("varbinary", allocator)) {
            varchar.allocateNew();
            varchar.setSafe(0, utf8);
            assertEquals(StringData.fromBytes(utf8), read(varchar, DataTypes.STRING(), "text"));
            assertEquals(StringData.fromBytes(utf8), read(varchar, DataTypes.CHAR(32), "bpchar"));

            varbinary.allocateNew();
            varbinary.setSafe(0, binary);
            assertArrayEquals(binary, (byte[]) read(varbinary, DataTypes.BYTES(), "bytea"));
            assertArrayEquals(binary, (byte[]) read(varbinary, DataTypes.BINARY(4), "bytea"));
        }
    }

    @Test
    public void testDecimalConversion() throws Exception {
        BigDecimal value = new BigDecimal("1234567890123456.123456");
        try (DecimalVector decimal = new DecimalVector("decimal", allocator, 38, 6)) {
            decimal.allocateNew();
            decimal.setSafe(0, value);
            DecimalData actual = (DecimalData) read(decimal, DataTypes.DECIMAL(38, 6), "numeric");
            assertEquals(value, actual.toBigDecimal());
        }
    }

    @Test
    public void testDateTimeConversions() throws Exception {
        long millis = 1710000000123L;
        long micros = 1710000000123456L;
        try (DateDayVector date = new DateDayVector("date", allocator);
                TimeMicroVector time = new TimeMicroVector("time", allocator);
                TimeStampMicroVector timestamp = new TimeStampMicroVector("timestamp", allocator);
                DateMilliVector timestamptz = new DateMilliVector("timestamptz", allocator)) {
            date.allocateNew(1);
            date.setSafe(0, 19782);
            assertEquals(19782, read(date, DataTypes.DATE(), "date"));

            time.allocateNew(1);
            time.setSafe(0, 123456789L);
            assertEquals(123456, read(time, DataTypes.TIME(6), "time"));

            timestamp.allocateNew(1);
            timestamp.setSafe(0, micros);
            assertEquals(
                    TimestampData.fromEpochMillis(
                            Math.floorDiv(micros, 1000), (int) Math.floorMod(micros, 1000) * 1000),
                    HologresCopyBulkReader.timestampDataFromMicros(micros));
            assertEquals(
                    TimestampData.fromEpochMillis(
                            Math.floorDiv(micros, 1000), (int) Math.floorMod(micros, 1000) * 1000),
                    read(timestamp, DataTypes.TIMESTAMP(6), "timestamp"));

            timestamp.setSafe(0, -1L);
            assertEquals(
                    TimestampData.fromEpochMillis(-1L, 999000),
                    HologresCopyBulkReader.timestampDataFromMicros(-1L));
            assertEquals(
                    TimestampData.fromEpochMillis(-1L, 999000),
                    read(timestamp, DataTypes.TIMESTAMP(6), "timestamp"));

            timestamptz.allocateNew(1);
            timestamptz.setSafe(0, millis);
            assertEquals(
                    TimestampData.fromTimestamp(new Timestamp(millis)),
                    read(timestamptz, DataTypes.TIMESTAMP(3), "timestamptz"));
            assertEquals(
                    TimestampData.fromEpochMillis(millis),
                    read(timestamptz, DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), "timestamptz"));
        }
    }

    @Test
    public void testIntegerArrayConversion() throws Exception {
        try (ListVector list = ListVector.empty("array", allocator)) {
            list.allocateNew();
            UnionListWriter writer = list.getWriter();
            writer.setPosition(0);
            writer.startList();
            writer.integer().writeInt(1);
            writer.integer().writeInt(2);
            writer.integer().writeInt(3);
            writer.endList();
            writer.setValueCount(1);

            Column column = new Column();
            column.setArrayType(true);
            column.setArrayElementType("int4");
            GenericArrayData result =
                    (GenericArrayData)
                            read(list, 0, DataTypes.ARRAY(DataTypes.INT()), "_int4", column);
            assertEquals(3, result.size());
            assertEquals(1, result.getInt(0));
            assertEquals(2, result.getInt(1));
            assertEquals(3, result.getInt(2));
        }
    }

    @Test
    public void testUnsupportedVectorConversionHasClearError() throws Exception {
        try (IntVector integer = new IntVector("integer", allocator)) {
            integer.allocateNew(1);
            integer.setSafe(0, 1);
            try {
                read(integer, DataTypes.STRING(), "text");
                fail("Expected unsupported conversion to fail");
            } catch (InvocationTargetException e) {
                assertTrue(e.getCause() instanceof IllegalArgumentException);
                assertTrue(e.getCause().getMessage().contains("Unsupported type conversion"));
            }
        }
    }

    @Test
    public void testConvertBatchPreservesNulls() throws Exception {
        Column column = new Column();
        column.setName("id");
        column.setTypeName("int4");
        TableSchema schema = mock(TableSchema.class);
        doReturn(0).when(schema).getColumnIndex("id");
        doReturn(column).when(schema).getColumn(0);
        HologresCopyBulkReader batchReader =
                new HologresCopyBulkReader(
                        null,
                        null,
                        new String[] {"id"},
                        new DataType[] {DataTypes.INT()},
                        new String[] {"int4"},
                        null,
                        null,
                        -1);
        setField(batchReader, "holoTableSchema", schema);
        Method convert =
                HologresCopyBulkReader.class.getDeclaredMethod(
                        "convertArrowToRowData", VectorSchemaRoot.class);
        convert.setAccessible(true);

        try (IntVector vector = new IntVector("id", allocator)) {
            vector.allocateNew(2);
            vector.setSafe(0, 42);
            vector.setNull(1);
            vector.setValueCount(2);
            try (VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
                root.setRowCount(2);
                @SuppressWarnings("unchecked")
                Iterator<RowData> rows = (Iterator<RowData>) convert.invoke(batchReader, root);
                assertEquals(42, rows.next().getInt(0));
                assertTrue(rows.next().isNullAt(0));
                assertFalse(rows.hasNext());
            }
        }
    }

    @Test
    public void testNextRecordUsesCurrentBatchAndDetectsEnd() throws Exception {
        GenericRowData expected = GenericRowData.of(42);
        setField(reader, "currentBatchIterator", Collections.singletonList(expected).iterator());
        assertSame(expected, reader.nextRecord());

        CopyOutWrapper wrapper = mock(CopyOutWrapper.class);
        doReturn(false).when(wrapper).hasNextBatch();
        setField(reader, "copyOutWrapper", wrapper);
        assertNull(reader.nextRecord());
        verify(wrapper).hasNextBatch();
    }

    @Test
    public void testCloseReleasesResources() throws Exception {
        CopyOutWrapper wrapper = mock(CopyOutWrapper.class);
        Connection connection = mock(Connection.class);
        setField(reader, "copyOutWrapper", wrapper);
        setField(reader, "conn", connection);

        reader.close();

        verify(wrapper).close();
        verify(connection).close();
        assertNull(getField(reader, "copyOutWrapper"));
        assertNull(getField(reader, "conn"));
    }

    @Test
    public void testCloseContinuesAfterWrapperFailureAndReportsConnectionFailure()
            throws Exception {
        CopyOutWrapper wrapper = mock(CopyOutWrapper.class);
        Connection connection = mock(Connection.class);
        doThrow(new java.io.IOException("wrapper close failed")).when(wrapper).close();
        doThrow(new SQLException("connection close failed")).when(connection).close();
        setField(reader, "copyOutWrapper", wrapper);
        setField(reader, "conn", connection);

        try {
            reader.close();
            fail("Expected connection close failure");
        } catch (java.io.IOException e) {
            assertTrue(e.getCause() instanceof SQLException);
        }
        verify(wrapper).close();
        verify(connection).close();
    }

    private Object read(FieldVector vector, DataType dataType, String holoTypeName)
            throws Exception {
        return read(vector, 0, dataType, holoTypeName, new Column());
    }

    private Object read(FieldVector vector, int rowId, DataType dataType, String holoTypeName)
            throws Exception {
        return read(vector, rowId, dataType, holoTypeName, new Column());
    }

    private Object read(
            FieldVector vector, int rowId, DataType dataType, String holoTypeName, Column column)
            throws Exception {
        return readFromArrowVector.invoke(reader, vector, rowId, dataType, holoTypeName, column);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = HologresCopyBulkReader.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getField(Object target, String name) throws Exception {
        Field field = HologresCopyBulkReader.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
