package com.alibaba.hologres.client.copy.in;

import com.alibaba.hologres.client.copy.in.arrow.NativeLz4CompressionCodec;
import com.alibaba.hologres.client.copy.in.arrow.RecordArrowWriter;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * {@link DryRunCopyInStageWrapper} 单测.
 *
 * <p>dry-run 模式不建立真实数据库连接，所有写入数据以 Arrow 格式保存在内存中，可通过 {@link
 * DryRunCopyInStageWrapper#getDryRunFiles()} 获取验证。
 *
 * <p>注意：testng.xml 配置了 parallel="methods"，各测试方法以多线程并发执行。 所有辅助数据均在测试方法内部以局部变量形式创建，避免实例变量的并发竞争。
 */
public class CopyInStageWrapperDryRunTest {

    private static final String STAGE_NAME = "my_stage";
    private static final String FILE_PREFIX = "test_prefix";
    // 本地测试时 fileSizeLimit 无需设为生产值，1 MB 已足够
    private static final int FILE_SIZE_LIMIT = 1024 * 1024;

    // =========================================================================
    // 辅助方法（static，无状态共享，线程安全）
    // =========================================================================

    /** 构建包含 id(int4 PK)、name(varchar)、value(int8) 三列的 TableSchema. */
    private static TableSchema buildSimpleSchema() {
        Column idCol = new Column();
        idCol.setName("id");
        idCol.setType(Types.INTEGER);
        idCol.setTypeName("int4");
        idCol.setPrimaryKey(true);
        idCol.setAllowNull(false);

        Column nameCol = new Column();
        nameCol.setName("name");
        nameCol.setType(Types.VARCHAR);
        nameCol.setTypeName("varchar");
        nameCol.setAllowNull(true);

        Column valueCol = new Column();
        valueCol.setName("value");
        valueCol.setType(Types.BIGINT);
        valueCol.setTypeName("int8");
        valueCol.setAllowNull(true);

        TableSchema.Builder builder = new TableSchema.Builder();
        builder.setTableName(TableName.valueOf("public.test_table"));
        builder.addColumn(idCol);
        builder.addColumn(nameCol);
        builder.addColumn(valueCol);
        TableSchema schema = builder.build();
        schema.calculateProperties();
        return schema;
    }

    private static List<String> defaultColumns() {
        return Arrays.asList("id", "name", "value");
    }

    private static Record buildRecord(TableSchema schema, int id, String name, Long value) {
        Record record = Record.build(schema);
        record.setObject(0, id);
        record.setObject(1, name);
        record.setObject(2, value);
        return record;
    }

    private static int countArrowRows(byte[] arrowBytes) throws IOException {
        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader =
                        new ArrowStreamReader(
                                Channels.newChannel(new ByteArrayInputStream(arrowBytes)),
                                allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            int totalRows = 0;
            while (reader.loadNextBatch()) {
                totalRows += root.getRowCount();
            }
            return totalRows;
        }
    }

    // =========================================================================
    // 测试用例（每个方法使用局部变量，线程安全）
    // =========================================================================

    /** flush 后 getDryRunFiles 应包含 1 个文件，文件名格式为 prefix_0.arrow. */
    @Test
    public void testFlushProducesSingleFile() throws IOException {
        TableSchema schema = buildSimpleSchema();
        RecordArrowWriter writer = new RecordArrowWriter(schema, defaultColumns(), 100);
        try (DryRunCopyInStageWrapper<Record> wrapper =
                new DryRunCopyInStageWrapper<>(STAGE_NAME, FILE_PREFIX, writer, FILE_SIZE_LIMIT)) {
            wrapper.putRecord(buildRecord(schema, 1, "Alice", 100L));
            wrapper.putRecord(buildRecord(schema, 2, "Bob", 200L));
            wrapper.flush();

            Map<String, byte[]> files = wrapper.getDryRunFiles();
            Assert.assertEquals(files.size(), 1, "flush 一次应产生 1 个文件");
            Assert.assertTrue(files.containsKey(FILE_PREFIX + "_0.arrow"));
        }
    }

    /** close 时自动 flush，Arrow 文件中的行数应与写入记录数一致. */
    @Test
    public void testCloseAutoFlushAndRowCount() throws IOException {
        TableSchema schema = buildSimpleSchema();
        RecordArrowWriter writer = new RecordArrowWriter(schema, defaultColumns(), 100);
        DryRunCopyInStageWrapper<Record> wrapper =
                new DryRunCopyInStageWrapper<>(STAGE_NAME, FILE_PREFIX, writer, FILE_SIZE_LIMIT);
        wrapper.putRecord(buildRecord(schema, 1, "Alice", 10L));
        wrapper.putRecord(buildRecord(schema, 2, "Bob", 20L));
        wrapper.putRecord(buildRecord(schema, 3, "Charlie", 30L));
        wrapper.close();

        Map<String, byte[]> files = wrapper.getDryRunFiles();
        Assert.assertEquals(files.size(), 1, "close 时应产生 1 个文件");
        Assert.assertEquals(countArrowRows(files.values().iterator().next()), 3, "应含 3 行数据");
    }

    /** fileSizeLimit 极小（1 字节）时，每次 putRecord 后数据超阈值，自动触发多次内存写入，产生多个文件. */
    @Test
    public void testMultipleFilesWhenSizeLimitExceeded() throws IOException {
        TableSchema schema = buildSimpleSchema();
        RecordArrowWriter writer = new RecordArrowWriter(schema, defaultColumns(), 1);
        DryRunCopyInStageWrapper<Record> wrapper =
                new DryRunCopyInStageWrapper<>(STAGE_NAME, FILE_PREFIX, writer, 1);
        wrapper.putRecord(buildRecord(schema, 1, "A", 1L));
        wrapper.putRecord(buildRecord(schema, 2, "B", 2L));
        wrapper.putRecord(buildRecord(schema, 3, "C", 3L));
        wrapper.close();

        Map<String, byte[]> files = wrapper.getDryRunFiles();
        Assert.assertTrue(files.size() > 1, "超过 fileSizeLimit 时应产生多个文件，实际: " + files.size());
        Assert.assertEquals(wrapper.getFileIndex(), files.size());
        for (int i = 0; i < files.size(); i++) {
            Assert.assertTrue(
                    files.containsKey(String.format("%s_%d.arrow", FILE_PREFIX, i)),
                    "应存在文件 " + FILE_PREFIX + "_" + i + ".arrow");
        }
    }

    /** 多次 flush 产生多个独立文件，行数分别对应各批次. */
    @Test
    public void testMultipleFlushesProduceMultipleFiles() throws IOException {
        TableSchema schema = buildSimpleSchema();
        RecordArrowWriter writer = new RecordArrowWriter(schema, defaultColumns(), 100);
        DryRunCopyInStageWrapper<Record> wrapper =
                new DryRunCopyInStageWrapper<>(STAGE_NAME, FILE_PREFIX, writer, FILE_SIZE_LIMIT);
        wrapper.putRecord(buildRecord(schema, 1, "Alice", 10L));
        wrapper.putRecord(buildRecord(schema, 2, "Bob", 20L));
        wrapper.flush(); // 产生文件 0

        wrapper.putRecord(buildRecord(schema, 3, "Charlie", 30L));
        wrapper.flush(); // 产生文件 1

        wrapper.close(); // 无剩余数据，不产生新文件

        Map<String, byte[]> files = wrapper.getDryRunFiles();
        Assert.assertEquals(files.size(), 2);
        Assert.assertEquals(wrapper.getFileIndex(), 2);
        Assert.assertEquals(countArrowRows(files.get(FILE_PREFIX + "_0.arrow")), 2, "文件0应有2行");
        Assert.assertEquals(countArrowRows(files.get(FILE_PREFIX + "_1.arrow")), 1, "文件1应有1行");
    }

    /** getFileIndex() 应等于 getDryRunFiles().size(). */
    @Test
    public void testGetFileIndexMatchesFileCount() throws IOException {
        TableSchema schema = buildSimpleSchema();
        RecordArrowWriter writer = new RecordArrowWriter(schema, defaultColumns(), 100);
        DryRunCopyInStageWrapper<Record> wrapper =
                new DryRunCopyInStageWrapper<>(STAGE_NAME, FILE_PREFIX, writer, FILE_SIZE_LIMIT);
        wrapper.putRecord(buildRecord(schema, 10, "X", null));
        wrapper.flush();
        wrapper.putRecord(buildRecord(schema, 20, "Y", 999L));
        wrapper.flush();

        Assert.assertEquals(wrapper.getFileIndex(), wrapper.getDryRunFiles().size());
        wrapper.close();
    }

    /** 重复 flush 空数据不应产生额外文件. */
    @Test
    public void testEmptyFlushProducesNoExtraFile() throws IOException {
        TableSchema schema = buildSimpleSchema();
        RecordArrowWriter writer = new RecordArrowWriter(schema, defaultColumns(), 100);
        try (DryRunCopyInStageWrapper<Record> wrapper =
                new DryRunCopyInStageWrapper<>(STAGE_NAME, FILE_PREFIX, writer, FILE_SIZE_LIMIT)) {
            wrapper.putRecord(buildRecord(schema, 1, "Z", 0L));
            wrapper.flush(); // 有数据，产生文件
            wrapper.flush(); // 无新数据，不产生文件
            Assert.assertEquals(wrapper.getDryRunFiles().size(), 1);
        }
    }

    /** Arrow 字节可被 ArrowStreamReader 反序列化，列数与行数均正确. */
    @Test
    public void testArrowBytesDeserializable() throws IOException {
        TableSchema schema = buildSimpleSchema();
        List<String> columns = defaultColumns();
        RecordArrowWriter writer = new RecordArrowWriter(schema, columns, 100);
        try (DryRunCopyInStageWrapper<Record> wrapper =
                new DryRunCopyInStageWrapper<>(STAGE_NAME, FILE_PREFIX, writer, FILE_SIZE_LIMIT)) {
            wrapper.putRecord(buildRecord(schema, 1, "Alice", 100L));
            wrapper.putRecord(buildRecord(schema, 2, null, null));
            wrapper.flush();

            byte[] arrowBytes = wrapper.getDryRunFiles().get(FILE_PREFIX + "_0.arrow");
            Assert.assertNotNull(arrowBytes);
            try (RootAllocator allocator = new RootAllocator();
                    ArrowStreamReader reader =
                            new ArrowStreamReader(
                                    Channels.newChannel(new ByteArrayInputStream(arrowBytes)),
                                    allocator)) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                Assert.assertEquals(root.getSchema().getFields().size(), columns.size(), "列数应一致");
                int totalRows = 0;
                while (reader.loadNextBatch()) {
                    totalRows += root.getRowCount();
                }
                Assert.assertEquals(totalRows, 2, "应读到 2 行数据");
            }
        }
    }

    /**
     * 验证 Arrow 文件内容的完整正确性：列名、字段值、null 值.
     *
     * <p>这是判断生成的 Arrow 文件是否"正确"的核心测试手段：
     *
     * <ol>
     *   <li>通过 {@code root.getSchema().getFields()} 验证列名顺序。
     *   <li>通过 {@code root.getVector(columnName).getObject(rowIndex)} 验证每行每列的值。
     *   <li>通过 {@code getObject(rowIndex) == null} 验证 null 字段被正确写入。
     * </ol>
     */
    @Test
    public void testArrowFieldValues() throws IOException {
        TableSchema schema = buildSimpleSchema();
        List<String> columns = defaultColumns();
        RecordArrowWriter writer = new RecordArrowWriter(schema, columns, 100);
        try (DryRunCopyInStageWrapper<Record> wrapper =
                new DryRunCopyInStageWrapper<>(STAGE_NAME, FILE_PREFIX, writer, FILE_SIZE_LIMIT)) {
            // 第 0 行：全非 null
            wrapper.putRecord(buildRecord(schema, 42, "Holo", 999L));
            // 第 1 行：name / value 均为 null
            wrapper.putRecord(buildRecord(schema, 7, null, null));
            wrapper.flush();

            byte[] arrowBytes = wrapper.getDryRunFiles().get(FILE_PREFIX + "_0.arrow");
            Assert.assertNotNull(arrowBytes, "Arrow 文件不应为 null");
            try (RootAllocator allocator = new RootAllocator();
                    ArrowStreamReader reader =
                            new ArrowStreamReader(
                                    Channels.newChannel(new ByteArrayInputStream(arrowBytes)),
                                    allocator)) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();

                // 1. 验证列名顺序
                Assert.assertEquals(root.getSchema().getFields().get(0).getName(), "id");
                Assert.assertEquals(root.getSchema().getFields().get(1).getName(), "name");
                Assert.assertEquals(root.getSchema().getFields().get(2).getName(), "value");

                Assert.assertTrue(reader.loadNextBatch(), "应有一批数据");
                Assert.assertEquals(root.getRowCount(), 2);

                // 2. 验证第 0 行（全非 null）
                Assert.assertEquals(
                        ((Number) root.getVector("id").getObject(0)).intValue(), 42, "id=42");
                Assert.assertEquals(
                        root.getVector("name").getObject(0).toString(), "Holo", "name=Holo");
                Assert.assertEquals(
                        ((Number) root.getVector("value").getObject(0)).longValue(),
                        999L,
                        "value=999");

                // 3. 验证第 1 行（含 null）
                Assert.assertEquals(
                        ((Number) root.getVector("id").getObject(1)).intValue(), 7, "id=7");
                Assert.assertNull(root.getVector("name").getObject(1), "name 应为 null");
                Assert.assertNull(root.getVector("value").getObject(1), "value 应为 null");
            }
        }
    }

    // =========================================================================
    // LZ4 压缩相关测试
    // =========================================================================

    /**
     * 使用 NativeLz4CompressionCodec 读取压缩 Arrow 数据的行数. 压缩后的 Arrow IPC V5 数据需要使用
     * CompressionCodec.Factory 才能正确解压。
     */
    private static int countCompressedArrowRows(byte[] arrowBytes) throws IOException {
        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader =
                        new ArrowStreamReader(
                                Channels.newChannel(new ByteArrayInputStream(arrowBytes)),
                                allocator,
                                NativeLz4CompressionCodec.Factory.INSTANCE)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            int totalRows = 0;
            while (reader.loadNextBatch()) {
                totalRows += root.getRowCount();
            }
            return totalRows;
        }
    }

    /** 启用 LZ4 压缩后，生成的 Arrow 文件可被正确反序列化，行数一致. */
    @Test
    public void testCompressedArrowFlushAndRowCount() throws IOException {
        TableSchema schema = buildSimpleSchema();
        // enableCompression = true
        RecordArrowWriter writer = new RecordArrowWriter(schema, defaultColumns(), 100, true);
        try (DryRunCopyInStageWrapper<Record> wrapper =
                new DryRunCopyInStageWrapper<>(STAGE_NAME, FILE_PREFIX, writer, FILE_SIZE_LIMIT)) {
            wrapper.putRecord(buildRecord(schema, 1, "Alice", 100L));
            wrapper.putRecord(buildRecord(schema, 2, "Bob", 200L));
            wrapper.putRecord(buildRecord(schema, 3, "Charlie", 300L));
            wrapper.flush();

            Map<String, byte[]> files = wrapper.getDryRunFiles();
            Assert.assertEquals(files.size(), 1, "flush 一次应产生 1 个文件");
            byte[] arrowBytes = files.values().iterator().next();
            Assert.assertNotNull(arrowBytes);
            // 压缩后的数据必须使用支持 LZ4_FRAME 的 CompressionCodec.Factory 才能读取
            Assert.assertEquals(countCompressedArrowRows(arrowBytes), 3, "压缩文件应含 3 行数据");
        }
    }

    /** 验证压缩后的 Arrow 文件内容正确性：列名、字段值、null 值. */
    @Test
    public void testCompressedArrowFieldValues() throws IOException {
        TableSchema schema = buildSimpleSchema();
        List<String> columns = defaultColumns();
        RecordArrowWriter writer = new RecordArrowWriter(schema, columns, 100, true);
        try (DryRunCopyInStageWrapper<Record> wrapper =
                new DryRunCopyInStageWrapper<>(STAGE_NAME, FILE_PREFIX, writer, FILE_SIZE_LIMIT)) {
            wrapper.putRecord(buildRecord(schema, 42, "Holo", 999L));
            wrapper.putRecord(buildRecord(schema, 7, null, null));
            wrapper.flush();

            byte[] arrowBytes = wrapper.getDryRunFiles().get(FILE_PREFIX + "_0.arrow");
            Assert.assertNotNull(arrowBytes, "Arrow 文件不应为 null");
            try (RootAllocator allocator = new RootAllocator();
                    ArrowStreamReader reader =
                            new ArrowStreamReader(
                                    Channels.newChannel(new ByteArrayInputStream(arrowBytes)),
                                    allocator,
                                    NativeLz4CompressionCodec.Factory.INSTANCE)) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();

                Assert.assertEquals(root.getSchema().getFields().get(0).getName(), "id");
                Assert.assertEquals(root.getSchema().getFields().get(1).getName(), "name");
                Assert.assertEquals(root.getSchema().getFields().get(2).getName(), "value");

                Assert.assertTrue(reader.loadNextBatch(), "应有一批数据");
                Assert.assertEquals(root.getRowCount(), 2);

                // 验证第 0 行
                Assert.assertEquals(
                        ((Number) root.getVector("id").getObject(0)).intValue(), 42, "id=42");
                Assert.assertEquals(
                        root.getVector("name").getObject(0).toString(), "Holo", "name=Holo");
                Assert.assertEquals(
                        ((Number) root.getVector("value").getObject(0)).longValue(),
                        999L,
                        "value=999");

                // 验证第 1 行（含 null）
                Assert.assertEquals(
                        ((Number) root.getVector("id").getObject(1)).intValue(), 7, "id=7");
                Assert.assertNull(root.getVector("name").getObject(1), "name 应为 null");
                Assert.assertNull(root.getVector("value").getObject(1), "value 应为 null");
            }
        }
    }

    /** 验证压缩后的 Arrow 文件大小 < 未压缩的文件大小（对于有一定数据量的场景）. */
    @Test
    public void testCompressedSizeSmallerThanUncompressed() throws IOException {
        TableSchema schema = buildSimpleSchema();
        int rowCount = 1000;

        // 写入不压缩的数据
        RecordArrowWriter uncompressedWriter =
                new RecordArrowWriter(schema, defaultColumns(), 8192, false);
        byte[] uncompressedBytes;
        try (DryRunCopyInStageWrapper<Record> wrapper =
                new DryRunCopyInStageWrapper<>(
                        STAGE_NAME, FILE_PREFIX, uncompressedWriter, FILE_SIZE_LIMIT)) {
            for (int i = 0; i < rowCount; i++) {
                wrapper.putRecord(buildRecord(schema, i, "name_" + i, (long) i * 100));
            }
            wrapper.flush();
            uncompressedBytes = wrapper.getDryRunFiles().values().iterator().next();
        }

        // 写入压缩的数据
        RecordArrowWriter compressedWriter =
                new RecordArrowWriter(schema, defaultColumns(), 8192, true);
        byte[] compressedBytes;
        try (DryRunCopyInStageWrapper<Record> wrapper =
                new DryRunCopyInStageWrapper<>(
                        STAGE_NAME, FILE_PREFIX, compressedWriter, FILE_SIZE_LIMIT)) {
            for (int i = 0; i < rowCount; i++) {
                wrapper.putRecord(buildRecord(schema, i, "name_" + i, (long) i * 100));
            }
            wrapper.flush();
            compressedBytes = wrapper.getDryRunFiles().values().iterator().next();
        }

        // 压缩后的数据应该更小
        Assert.assertTrue(
                compressedBytes.length < uncompressedBytes.length,
                String.format(
                        "压缩后大小(%d) 应小于未压缩大小(%d)",
                        compressedBytes.length, uncompressedBytes.length));

        // 压缩数据仍可正确读取
        Assert.assertEquals(countCompressedArrowRows(compressedBytes), rowCount);
    }

    // =========================================================================
    // abort 相关测试
    // =========================================================================

    /** abort 不会 flush 剩余数据，已写入但未 flush 的记录应丢弃. */
    @Test
    public void testAbortDoesNotFlush() throws IOException {
        TableSchema schema = buildSimpleSchema();
        RecordArrowWriter writer = new RecordArrowWriter(schema, defaultColumns(), 100);
        DryRunCopyInStageWrapper<Record> wrapper =
                new DryRunCopyInStageWrapper<>(STAGE_NAME, FILE_PREFIX, writer, FILE_SIZE_LIMIT);
        wrapper.putRecord(buildRecord(schema, 1, "Alice", 100L));
        wrapper.putRecord(buildRecord(schema, 2, "Bob", 200L));
        // 不 flush，直接 abort
        wrapper.abort();

        // abort 不 flush，所以不应产生任何文件
        Assert.assertEquals(wrapper.getDryRunFiles().size(), 0, "abort 不应产生文件");
    }

    /** abort 后再次调用 abort 应幂等，不抛异常. */
    @Test
    public void testAbortIdempotent() throws IOException {
        TableSchema schema = buildSimpleSchema();
        RecordArrowWriter writer = new RecordArrowWriter(schema, defaultColumns(), 100);
        DryRunCopyInStageWrapper<Record> wrapper =
                new DryRunCopyInStageWrapper<>(STAGE_NAME, FILE_PREFIX, writer, FILE_SIZE_LIMIT);
        wrapper.putRecord(buildRecord(schema, 1, "X", 1L));
        wrapper.abort();
        wrapper.abort(); // 第二次调用应直接返回，不抛异常
        Assert.assertEquals(wrapper.getDryRunFiles().size(), 0);
    }

    /** abort 后再调用 close 应为 no-op，不会再 flush. */
    @Test
    public void testCloseAfterAbortIsNoOp() throws IOException {
        TableSchema schema = buildSimpleSchema();
        RecordArrowWriter writer = new RecordArrowWriter(schema, defaultColumns(), 100);
        DryRunCopyInStageWrapper<Record> wrapper =
                new DryRunCopyInStageWrapper<>(STAGE_NAME, FILE_PREFIX, writer, FILE_SIZE_LIMIT);
        wrapper.putRecord(buildRecord(schema, 1, "Y", 2L));
        wrapper.abort();
        wrapper.close(); // abort 后 close 应幂等
        Assert.assertEquals(wrapper.getDryRunFiles().size(), 0, "abort 后 close 不应产生文件");
    }

    /** close 后再调用 abort 应为 no-op. */
    @Test
    public void testAbortAfterCloseIsNoOp() throws IOException {
        TableSchema schema = buildSimpleSchema();
        RecordArrowWriter writer = new RecordArrowWriter(schema, defaultColumns(), 100);
        DryRunCopyInStageWrapper<Record> wrapper =
                new DryRunCopyInStageWrapper<>(STAGE_NAME, FILE_PREFIX, writer, FILE_SIZE_LIMIT);
        wrapper.putRecord(buildRecord(schema, 1, "Z", 3L));
        wrapper.close(); // 正常 close，会 flush
        Assert.assertEquals(wrapper.getDryRunFiles().size(), 1, "close 应 flush 产生文件");
        wrapper.abort(); // close 后 abort 应幂等
        Assert.assertEquals(wrapper.getDryRunFiles().size(), 1, "abort 不应改变已有文件");
    }

    /** flush 过部分数据后 abort，已 flush 的文件保留，未 flush 的数据丢弃. */
    @Test
    public void testAbortAfterPartialFlush() throws IOException {
        TableSchema schema = buildSimpleSchema();
        RecordArrowWriter writer = new RecordArrowWriter(schema, defaultColumns(), 100);
        DryRunCopyInStageWrapper<Record> wrapper =
                new DryRunCopyInStageWrapper<>(STAGE_NAME, FILE_PREFIX, writer, FILE_SIZE_LIMIT);
        wrapper.putRecord(buildRecord(schema, 1, "First", 10L));
        wrapper.flush(); // 产生文件 0

        wrapper.putRecord(buildRecord(schema, 2, "Second", 20L));
        // 不 flush 第二批，直接 abort
        wrapper.abort();

        Map<String, byte[]> files = wrapper.getDryRunFiles();
        Assert.assertEquals(files.size(), 1, "只有 flush 过的文件应保留");
        Assert.assertTrue(files.containsKey(FILE_PREFIX + "_0.arrow"));
        Assert.assertEquals(countArrowRows(files.get(FILE_PREFIX + "_0.arrow")), 1, "文件0应有1行");
    }

    // =========================================================================
    // LZ4 压缩相关测试（续）
    // =========================================================================

    /**
     * 验证含空字符串列的压缩数据能被正确读取. 回归测试: 修复前空 buffer 写 prefix=0 导致 C++/pyarrow 报 "Lz4 compressed input
     * contains less than one frame". 修复后空 buffer 写 prefix=-1 (sentinel), 所有 reader 都能正确处理.
     */
    @Test
    public void testCompressedWithEmptyStringColumn() throws IOException {
        TableSchema schema = buildSimpleSchema();
        List<String> columns = defaultColumns();
        int rowCount = 100;

        // 使用 BUFFER_LEVEL 压缩
        RecordArrowWriter writer = new RecordArrowWriter(schema, columns, 100, true);
        byte[] compressedBytes;
        try (DryRunCopyInStageWrapper<Record> wrapper =
                new DryRunCopyInStageWrapper<>(STAGE_NAME, FILE_PREFIX, writer, FILE_SIZE_LIMIT)) {
            for (int i = 0; i < rowCount; i++) {
                // name 列为空字符串，value 列偶数行传 null，奇数行传 0L
                wrapper.putRecord(buildRecord(schema, i, "", i % 2 == 0 ? null : 0L));
            }
            wrapper.flush();
            compressedBytes = wrapper.getDryRunFiles().values().iterator().next();
        }

        Assert.assertTrue(compressedBytes.length > 0, "Compressed data should not be empty");

        // 验证行数与字段值均正确
        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader =
                        new ArrowStreamReader(
                                Channels.newChannel(new ByteArrayInputStream(compressedBytes)),
                                allocator,
                                NativeLz4CompressionCodec.Factory.INSTANCE)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            Assert.assertTrue(reader.loadNextBatch(), "应有一批数据");
            Assert.assertEquals(root.getRowCount(), rowCount, "行数应一致");

            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(
                        ((Number) root.getVector("id").getObject(i)).intValue(), i, "id 应一致");
                Object nameObj = root.getVector("name").getObject(i);
                Assert.assertNotNull(nameObj, "空字符串不应被读成 null");
                Assert.assertEquals(nameObj.toString(), "", "name 应为空字符串");
                Object valueObj = root.getVector("value").getObject(i);
                if (i % 2 == 0) {
                    Assert.assertNull(valueObj, "偶数行 value 应为 null");
                } else {
                    Assert.assertNotNull(valueObj, "奇数行 value 不应为 null");
                    Assert.assertEquals(((Number) valueObj).longValue(), 0L, "奇数行 value 应为 0");
                }
            }
        }
    }

    /** 验证混合空字符串与非空字符串的压缩数据字段值正确. 确保空 buffer sentinel 不会污染相邻 batch 或列数据. */
    @Test
    public void testCompressedWithMixedEmptyAndNonEmptyStringColumn() throws IOException {
        TableSchema schema = buildSimpleSchema();
        List<String> columns = defaultColumns();
        int rowCount = 100;

        RecordArrowWriter writer = new RecordArrowWriter(schema, columns, 100, true);
        byte[] compressedBytes;
        try (DryRunCopyInStageWrapper<Record> wrapper =
                new DryRunCopyInStageWrapper<>(STAGE_NAME, FILE_PREFIX, writer, FILE_SIZE_LIMIT)) {
            for (int i = 0; i < rowCount; i++) {
                String name = i % 3 == 0 ? "" : "name_" + i;
                Long value = i % 5 == 0 ? null : (long) i;
                wrapper.putRecord(buildRecord(schema, i, name, value));
            }
            wrapper.flush();
            compressedBytes = wrapper.getDryRunFiles().values().iterator().next();
        }

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader =
                        new ArrowStreamReader(
                                Channels.newChannel(new ByteArrayInputStream(compressedBytes)),
                                allocator,
                                NativeLz4CompressionCodec.Factory.INSTANCE)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            Assert.assertTrue(reader.loadNextBatch(), "应有一批数据");
            Assert.assertEquals(root.getRowCount(), rowCount, "行数应一致");

            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(
                        ((Number) root.getVector("id").getObject(i)).intValue(), i, "id 应一致");
                Object nameObj = root.getVector("name").getObject(i);
                Assert.assertNotNull(nameObj, "name 不应为 null");
                String expectedName = i % 3 == 0 ? "" : "name_" + i;
                Assert.assertEquals(nameObj.toString(), expectedName, "name 字段值应一致");
                Object valueObj = root.getVector("value").getObject(i);
                if (i % 5 == 0) {
                    Assert.assertNull(valueObj, "value 应为 null");
                } else {
                    Assert.assertNotNull(valueObj, "value 不应为 null");
                    Assert.assertEquals(((Number) valueObj).longValue(), (long) i, "value 字段值应一致");
                }
            }
        }
    }
}
