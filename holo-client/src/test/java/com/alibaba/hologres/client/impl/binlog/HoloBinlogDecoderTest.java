package com.alibaba.hologres.client.impl.binlog;

import com.alibaba.blink.dataformat.BinaryRow;
import com.alibaba.blink.dataformat.BinaryRowWriter;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.binlog.BinlogRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

/**
 * {@link HoloBinlogDecoder} 离线单测，不依赖真实实例。
 *
 * <p>直接按 binlog 的 wire format 拼出 ByteBuffer 后交给 decoder 解析，用于覆盖 geometry / geography 等
 * 以二进制透传的扩展类型：这些类型此前会被当成 UTF-8 字符串解析，遇到非法 UTF-8 字节时抛出 "decodeUTF8 error"。
 */
public class HoloBinlogDecoderTest {

    private static final String SCHEMA_VERSION = "14933";

    /** binlog 行的前 3 个字段固定为 lsn、eventType、timestamp. */
    private static final int BINLOG_META_FIELD_COUNT = 3;

    /** binlog header 为 16 字节：protocol version(long) + table version(long). */
    private static final int BINLOG_HEADER_BYTES = 16;

    /** eventType = 2 即 DELETE，取值见 {@link BinlogEventType}. */
    private static final long EVENT_TYPE_DELETE = 2L;

    /**
     * PostGIS POINT(SRID=4326) 在 Hologres 内部的 GSERIALIZED 字节：srid+flags(4) + type(4) + npoints(4) +
     * 两个 double(16)，共 28 字节。其中 0xE6 0x10 这样的字节序列不是合法 UTF-8，是复现问题的关键。
     */
    private static final byte[] POINT_GSERIALIZED =
            new byte[] {
                (byte) 0xE6,
                0x10,
                0x00,
                0x20,
                0x01,
                0x00,
                0x00,
                0x00,
                0x01,
                0x00,
                0x00,
                0x00,
                0x1F,
                (byte) 0x85,
                (byte) 0xEB,
                0x51,
                (byte) 0xB8,
                0x1E,
                0x5C,
                0x40,
                (byte) 0xC3,
                (byte) 0xF5,
                0x28,
                0x5C,
                (byte) 0x8F,
                (byte) 0xC2,
                0x39,
                0x40
            };

    private static final byte[] ROARING_BITMAP_BYTES =
            new byte[] {0x3A, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, (byte) 0xFF};

    private static Column column(String name, int type, String typeName) {
        Column c = new Column();
        c.setName(name);
        c.setType(type);
        c.setTypeName(typeName);
        c.setAllowNull(true);
        return c;
    }

    /**
     * 构造 id(int4) + geom(geometry) + geog(geography) + rb(roaringbitmap) + js(json) + t(text) 的表结构.
     */
    private static TableSchema buildSchema() {
        TableSchema.Builder builder = new TableSchema.Builder("3406", SCHEMA_VERSION);
        builder.setTableName(TableName.valueOf("public.binlog_udt_test"));
        builder.addColumn(column("id", Types.INTEGER, "int4"));
        builder.addColumn(column("geom", Types.OTHER, "geometry"));
        builder.addColumn(column("geog", Types.OTHER, "geography"));
        builder.addColumn(column("rb", Types.OTHER, "roaringbitmap"));
        builder.addColumn(column("js", Types.OTHER, "json"));
        builder.addColumn(column("t", Types.VARCHAR, "text"));
        TableSchema schema = builder.build();
        schema.calculateProperties();
        return schema;
    }

    /** 把一行的字段值写成 BinaryRow 的字节，字段值为 null 时写 null 标记. */
    private static byte[] encodeRow(int columnCount, long lsn, Object... columnValues) {
        BinaryRow row = new BinaryRow(columnCount + BINLOG_META_FIELD_COUNT);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.reset();
        writer.writeLong(0, lsn);
        writer.writeLong(1, EVENT_TYPE_DELETE);
        writer.writeLong(2, 1763669967897828L);
        for (int i = 0; i < columnValues.length; i++) {
            int pos = i + BINLOG_META_FIELD_COUNT;
            Object value = columnValues[i];
            if (value == null) {
                writer.setNullAt(pos);
            } else if (value instanceof Integer) {
                writer.writeInt(pos, (Integer) value);
            } else if (value instanceof byte[]) {
                writer.writeByteArray(pos, (byte[]) value);
            } else if (value instanceof String) {
                writer.writeString(pos, (String) value);
            } else {
                throw new IllegalArgumentException("unsupported test value " + value);
            }
        }
        writer.complete();
        return Arrays.copyOfRange(row.getMemorySegment().getArray(), 0, row.getSizeInBytes());
    }

    /**
     * 按 binlog wire format 组装一个 batch：header 为 16 字节大端(protocol version + table version)， 之后 data
     * 部分为小端的 binlog version(int) + row count(int) + 每行 offset(int) + 各行数据。
     */
    private static ByteBuffer buildBinlogBuffer(List<byte[]> rows) {
        // 生产的BinaryRowSet会把header补齐到8字节，保证每行起始地址8字节对齐
        int dataHeaderLen = Integer.BYTES * (2 + rows.size());
        dataHeaderLen += dataHeaderLen % 8 == 0 ? 0 : 8 - dataHeaderLen % 8;
        int dataLen = dataHeaderLen;
        for (byte[] row : rows) {
            dataLen += row.length;
        }
        ByteBuffer buffer =
                ByteBuffer.allocate(BINLOG_HEADER_BYTES + dataLen).order(ByteOrder.BIG_ENDIAN);
        buffer.putLong(HoloBinlogDecoder.BINLOG_PROTOCOL_VERSION);
        buffer.putLong(Long.parseLong(SCHEMA_VERSION));

        ByteBuffer data = ByteBuffer.allocate(dataLen).order(ByteOrder.LITTLE_ENDIAN);
        data.putInt(0);
        data.putInt(rows.size());
        int offset = dataHeaderLen;
        for (byte[] row : rows) {
            data.putInt(offset);
            offset += row.length;
        }
        data.position(dataHeaderLen);
        for (byte[] row : rows) {
            data.put(row);
        }
        buffer.put(data.array());
        buffer.flip();
        return buffer;
    }

    /**
     * geometry 的 binlog 值是非法 UTF-8 的内部二进制，解析失败时不能抛异常终止消费。
     *
     * <p>改动前这里会抛 "decodeUTF8 error" 并导致整个 shard 的消费终止；现在降级为该列置 null + 打 ERROR 日志， 其余列照常解析，作业可以继续跑。
     */
    @Test
    public void decodeGeometryFallsBackToNullWithoutThrowing() throws Exception {
        TableSchema schema = buildSchema();
        HoloBinlogDecoder decoder = new HoloBinlogDecoder(schema);
        byte[] row =
                encodeRow(
                        schema.getColumnSchema().length,
                        20960678205L,
                        1,
                        POINT_GSERIALIZED,
                        POINT_GSERIALIZED,
                        ROARING_BITMAP_BYTES,
                        "{\"a\":1}",
                        "abc");

        List<BinlogRecord> records = decoder.decode(0, buildBinlogBuffer(Arrays.asList(row)));

        Assert.assertEquals(1, records.size());
        BinlogRecord record = records.get(0);
        // 无法解析的列保持 unset（读出来是 null，但 bitSet 不置位，回写时不会用 NULL 覆盖原值）
        Assert.assertNull(record.getObject("geom"));
        Assert.assertNull(record.getObject("geog"));
        Assert.assertFalse(record.isSet(schema.getColumnIndex("geom")), "geom should stay unset");
        Assert.assertFalse(record.isSet(schema.getColumnIndex("geog")), "geog should stay unset");
        Assert.assertEquals((byte[]) record.getObject("rb"), ROARING_BITMAP_BYTES);
        Assert.assertEquals(record.getObject("js"), "{\"a\":1}");
        Assert.assertEquals(record.getObject("t"), "abc");
        Assert.assertEquals(record.getObject("id"), 1);
    }

    /** 用 projection 排除 geometry 列后可以正常消费，roaringbitmap 仍按 byte[] 返回. */
    @Test
    public void decodeSucceedsWhenGeometryExcludedByProjection() throws Exception {
        TableSchema schema = buildSchema();
        HoloBinlogDecoder decoder =
                new HoloBinlogDecoder(
                        schema, new String[] {"id", "rb", "js", "t"}, false, false, false);
        // projection 只订阅 4 列，行里也只写这 4 列
        byte[] row = encodeRow(4, 20960678205L, 1, ROARING_BITMAP_BYTES, "{\"a\":1}", "abc");

        List<BinlogRecord> records = decoder.decode(0, buildBinlogBuffer(Arrays.asList(row)));

        Assert.assertEquals(1, records.size());
        BinlogRecord record = records.get(0);
        Assert.assertEquals(record.getBinlogLsn(), 20960678205L);
        Assert.assertEquals(record.getObject("id"), 1);
        Assert.assertEquals((byte[]) record.getObject("rb"), ROARING_BITMAP_BYTES);
        Assert.assertEquals(record.getObject("js"), "{\"a\":1}");
        Assert.assertEquals(record.getObject("t"), "abc");
        Assert.assertNull(record.getObject("geom"));
        Assert.assertNull(record.getObject("geog"));
    }

    /** geometry / geography 为 null 时仍然解析为 null. */
    @Test
    public void decodeNullGeometry() throws Exception {
        TableSchema schema = buildSchema();
        HoloBinlogDecoder decoder = new HoloBinlogDecoder(schema);
        byte[] row =
                encodeRow(
                        schema.getColumnSchema().length,
                        1L,
                        2,
                        null,
                        null,
                        null,
                        null,
                        "only text here");

        List<BinlogRecord> records = decoder.decode(0, buildBinlogBuffer(Arrays.asList(row)));

        Assert.assertEquals(1, records.size());
        BinlogRecord record = records.get(0);
        Assert.assertNull(record.getObject("geom"));
        Assert.assertNull(record.getObject("geog"));
        Assert.assertNull(record.getObject("rb"));
        Assert.assertEquals(record.getObject("t"), "only text here");
    }

    /** 一个batch里多行时每行的offset都要算对，顺带覆盖 deserialize 的 offsetNext 逻辑. */
    @Test
    public void decodeMultipleRowsInOneBatch() throws Exception {
        TableSchema schema = buildSchema();
        int columnCount = schema.getColumnSchema().length;
        HoloBinlogDecoder decoder = new HoloBinlogDecoder(schema);
        // geometry/geography 保持 null（null 仍然支持），用 rb 和 text 制造不同长度的变长数据
        byte[] first =
                encodeRow(
                        columnCount, 100L, 1, null, null, ROARING_BITMAP_BYTES, null, "first row");
        byte[] second =
                encodeRow(columnCount, 101L, 2, null, null, null, null, "second row is longer");
        byte[] third =
                encodeRow(columnCount, 102L, 3, null, null, ROARING_BITMAP_BYTES, null, "third");

        List<BinlogRecord> records =
                decoder.decode(0, buildBinlogBuffer(Arrays.asList(first, second, third)));

        Assert.assertEquals(3, records.size());
        Assert.assertEquals(records.get(0).getBinlogLsn(), 100L);
        Assert.assertEquals((byte[]) records.get(0).getObject("rb"), ROARING_BITMAP_BYTES);
        Assert.assertNull(records.get(0).getObject("geom"));
        Assert.assertEquals(records.get(0).getObject("t"), "first row");

        Assert.assertEquals(records.get(1).getBinlogLsn(), 101L);
        Assert.assertNull(records.get(1).getObject("rb"));
        Assert.assertEquals(records.get(1).getObject("t"), "second row is longer");

        Assert.assertEquals(records.get(2).getBinlogLsn(), 102L);
        Assert.assertEquals((byte[]) records.get(2).getObject("rb"), ROARING_BITMAP_BYTES);
        Assert.assertEquals(records.get(2).getObject("t"), "third");
    }

    /**
     * 白名单之外的 {@link Types#OTHER} 类型保持历史行为，不引入兼容性变更。
     *
     * <p>字节恰好是合法 UTF-8 时（实测 interval '1 day'、point(0,0)、inet '0.0.0.0' 等都是这种情况），
     * 仍按字符串返回和改动前一致的内容，只是额外打一次 WARN；只有解析失败时才置 null。
     */
    @Test
    public void decodeUnsupportedOtherTypeKeepsLegacyStringWhenDecodable() throws Exception {
        String[] typeNames = {"uuid", "inet", "tsvector", "interval", "point"};
        // 全部 < 0x80，是合法 UTF-8，改动前后都应该解析成同样的字符串
        byte[] utf8CleanBytes = new byte[] {0x11, 0x22, 0x33, 0x44};
        String expected = new String(utf8CleanBytes, java.nio.charset.StandardCharsets.UTF_8);

        for (String typeName : typeNames) {
            TableSchema.Builder builder = new TableSchema.Builder("3406", SCHEMA_VERSION);
            builder.setTableName(TableName.valueOf("public.binlog_other_type_test"));
            builder.addColumn(column("id", Types.INTEGER, "int4"));
            builder.addColumn(column("v", Types.OTHER, typeName));
            TableSchema schema = builder.build();
            schema.calculateProperties();

            HoloBinlogDecoder decoder = new HoloBinlogDecoder(schema);
            byte[] row = encodeRow(2, 1L, 1, utf8CleanBytes);

            List<BinlogRecord> records = decoder.decode(0, buildBinlogBuffer(Arrays.asList(row)));
            Assert.assertEquals(1, records.size());
            Assert.assertEquals(
                    records.get(0).getObject("v"),
                    expected,
                    "legacy string behavior should be kept for " + typeName);
        }
    }

    /** 白名单之外的 {@link Types#OTHER} 类型解析失败时置 null 并继续，不抛异常. */
    @Test
    public void decodeUnsupportedOtherTypeFallsBackToNullOnFailure() throws Exception {
        // 含 0xE6/0xB8 等 >=0x80 的字节，不是合法 UTF-8
        byte[] invalidUtf8 = new byte[] {(byte) 0xE6, (byte) 0xB8, 0x00, 0x01};

        TableSchema.Builder builder = new TableSchema.Builder("3406", SCHEMA_VERSION);
        builder.setTableName(TableName.valueOf("public.binlog_other_type_test"));
        builder.addColumn(column("id", Types.INTEGER, "int4"));
        builder.addColumn(column("v", Types.OTHER, "uuid"));
        TableSchema schema = builder.build();
        schema.calculateProperties();

        HoloBinlogDecoder decoder = new HoloBinlogDecoder(schema);
        byte[] row = encodeRow(2, 1L, 1, invalidUtf8);

        List<BinlogRecord> records = decoder.decode(0, buildBinlogBuffer(Arrays.asList(row)));

        Assert.assertEquals(1, records.size());
        Assert.assertNull(records.get(0).getObject("v"));
        Assert.assertFalse(
                records.get(0).isSet(schema.getColumnIndex("v")),
                "undecodable column should stay unset instead of being written as NULL");
        Assert.assertEquals(records.get(0).getObject("id"), 1);
    }

    /** 合法的多字节 UTF-8 内容不能被误判成解析失败. */
    @Test
    public void decodeOtherTypeKeepsValidMultiByteUtf8() throws Exception {
        // "中" 的 UTF-8 编码
        byte[] multiByte = new byte[] {(byte) 0xE4, (byte) 0xB8, (byte) 0xAD};

        TableSchema.Builder builder = new TableSchema.Builder("3406", SCHEMA_VERSION);
        builder.setTableName(TableName.valueOf("public.binlog_other_type_test"));
        builder.addColumn(column("id", Types.INTEGER, "int4"));
        builder.addColumn(column("v", Types.OTHER, "json"));
        TableSchema schema = builder.build();
        schema.calculateProperties();

        HoloBinlogDecoder decoder = new HoloBinlogDecoder(schema);
        byte[] row = encodeRow(2, 1L, 1, multiByte);

        List<BinlogRecord> records = decoder.decode(0, buildBinlogBuffer(Arrays.asList(row)));

        Assert.assertEquals(1, records.size());
        Assert.assertEquals(records.get(0).getObject("v"), "\u4e2d");
    }

    /** 同一个 decoder 连续解析多个 batch 时，不支持的列不会每行都写入/漏过，行为要稳定. */
    @Test
    public void decodeUnsupportedOtherTypeIsStableAcrossBatches() throws Exception {
        TableSchema schema = buildSchema();
        int columnCount = schema.getColumnSchema().length;
        HoloBinlogDecoder decoder = new HoloBinlogDecoder(schema);

        for (int i = 0; i < 3; i++) {
            byte[] row =
                    encodeRow(
                            columnCount,
                            100L + i,
                            i,
                            POINT_GSERIALIZED,
                            null,
                            ROARING_BITMAP_BYTES,
                            null,
                            "row" + i);
            List<BinlogRecord> records = decoder.decode(0, buildBinlogBuffer(Arrays.asList(row)));
            Assert.assertEquals(1, records.size());
            BinlogRecord record = records.get(0);
            // 每个 batch 的行为都一致：geom 保持 unset，其余列正常
            Assert.assertFalse(record.isSet(schema.getColumnIndex("geom")));
            Assert.assertEquals((byte[]) record.getObject("rb"), ROARING_BITMAP_BYTES);
            Assert.assertEquals(record.getObject("t"), "row" + i);
        }
    }

    /** 解析失败时异常信息要带上出错的列名和类型，否则线上只能看到一个 decodeUTF8 error 无从定位. */
    @Test
    public void decodeFailureReportsColumnName() throws Exception {
        TableSchema.Builder builder = new TableSchema.Builder("3406", SCHEMA_VERSION);
        builder.setTableName(TableName.valueOf("public.binlog_unsupported_test"));
        builder.addColumn(column("id", Types.INTEGER, "int4"));
        builder.addColumn(column("weird", Types.STRUCT, "some_unsupported_type"));
        TableSchema schema = builder.build();
        schema.calculateProperties();

        HoloBinlogDecoder decoder = new HoloBinlogDecoder(schema);
        byte[] row = encodeRow(schema.getColumnSchema().length, 1L, 3, 4);

        try {
            decoder.decode(0, buildBinlogBuffer(Arrays.asList(row)));
            Assert.fail("expect decode to fail on unsupported type");
        } catch (HoloClientException e) {
            Assert.assertTrue(
                    e.getMessage().contains("weird")
                            && e.getMessage().contains("some_unsupported_type"),
                    "message should contain the failing column, but was: " + e.getMessage());
        }
    }
}
