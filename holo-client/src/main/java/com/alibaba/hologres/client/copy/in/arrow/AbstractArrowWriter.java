package com.alibaba.hologres.client.copy.in.arrow;

import com.alibaba.hologres.client.model.TableSchema;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

/** Arrow格式写入器，用于将RECORD数据转换为Arrow格式对应的bytes数组. */
public abstract class AbstractArrowWriter<RECORD> implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractArrowWriter.class);

    protected final TableSchema schema;
    protected final List<String> columns;
    private final RootAllocator allocator;
    private final Schema arrowSchema;
    private VectorSchemaRoot root;
    private ArrowStreamWriter writer;
    private boolean writerInitialized = false;
    private final int maxBatchSize;

    // Arrow IPC V5 buffer-level LZ4 压缩相关
    private final CompressionCodec.Factory compressionFactory;
    private final CompressionUtil.CodecType compressionCodecType;

    // 攒批相关属性
    private final List<RECORD> batchRecords = new ArrayList<>();
    private final ByteArrayOutputStream baOs = new ByteArrayOutputStream();

    public AbstractArrowWriter(TableSchema schema, List<String> columns, int maxBatchSize) {
        this(schema, columns, maxBatchSize, false);
    }

    /**
     * 创建 ArrowWriter，支持可选的 Arrow IPC V5 buffer-level LZ4 压缩. 启用压缩后，ArrowStreamWriter 会在序列化每个
     * RecordBatch 时自动对各 buffer 做 LZ4_FRAME 压缩， 读取端的高版本 Arrow C++ 可透明解压。
     *
     * @param schema 表 schema
     * @param columns 列名列表
     * @param maxBatchSize 每批最大行数
     * @param enableCompression 是否启用 LZ4 压缩
     */
    public AbstractArrowWriter(
            TableSchema schema, List<String> columns, int maxBatchSize, boolean enableCompression) {
        this.schema = schema;
        this.columns = columns;
        this.allocator = new RootAllocator();
        this.arrowSchema = ArrowVectorCreatorUtil.createArrowSchema(schema, columns);
        this.maxBatchSize = maxBatchSize;

        if (enableCompression) {
            // 使用 lz4-java JNI native 压缩
            this.compressionFactory = NativeLz4CompressionCodec.Factory.INSTANCE;
            this.compressionCodecType = CompressionUtil.CodecType.LZ4_FRAME;
            LOGGER.info("ArrowWriter created with LZ4_FRAME compression enabled");
        } else {
            this.compressionFactory = null;
            this.compressionCodecType = null;
        }
    }

    /**
     * 是否启用了 Arrow IPC V5 buffer-level LZ4 压缩.
     *
     * @return true 表示已启用压缩
     */
    public boolean isCompressionEnabled() {
        return compressionFactory != null && compressionCodecType != null;
    }

    /**
     * 初始化ArrowStreamWriter.
     *
     * @throws IOException 如果初始化失败
     */
    private void initializeWriter() throws IOException {
        if (!writerInitialized) {
            root = VectorSchemaRoot.create(arrowSchema, allocator);

            // 根据是否启用压缩，选择不同的 ArrowStreamWriter 构造方式
            if (compressionFactory != null && compressionCodecType != null) {
                // 使用 Arrow IPC V5 buffer-level 压缩：每个 RecordBatch 的各 buffer 会自动做 LZ4_FRAME 压缩
                writer =
                        new ArrowStreamWriter(
                                root,
                                null,
                                Channels.newChannel(baOs),
                                new IpcOption(),
                                compressionFactory,
                                compressionCodecType);
            } else {
                writer =
                        new ArrowStreamWriter(
                                root, null, Channels.newChannel(baOs), new IpcOption());
            }
            writer.start();
            writerInitialized = true;
        }
    }

    /**
     * 将单个<RECORD>记录添加到批次中. 当攒批批次达到阈值时, 将整批数据转为Arrow格式.
     *
     * @param record 单个记录
     */
    public void put(RECORD record) throws IOException {
        batchRecords.add(record);
        if (batchRecords.size() >= maxBatchSize) {
            writeBatch();
        }
    }

    /**
     * 将当前已经攒批的数据, 转为Arrow格式并通过writeBatch写入.
     *
     * @throws IOException 如果写入失败
     */
    private void writeBatch() throws IOException {
        initializeWriter();
        fillVectorSchemaRoot(root, batchRecords);
        writer.writeBatch();
        // 清空批次
        batchRecords.clear();
    }

    /**
     * 将一批<RECORD>数据转换为Arrow格式, 填充到VectorSchemaRoot中.
     *
     * @param root VectorSchemaRoot
     * @param recordList 记录列表
     * @throws IOException 如果写入失败
     */
    public abstract void fillVectorSchemaRoot(VectorSchemaRoot root, List<RECORD> recordList)
            throws IOException;

    /**
     * 获取当前Arrow格式数据的大小.
     *
     * @return Arrow格式数据的大小
     */
    public int getArrowDataSize() {
        return baOs.size();
    }

    /**
     * 结束并关闭写入器.
     *
     * @throws IOException 如果关闭失败
     */
    public byte[] endAndGetBytes() throws IOException {
        // 写入剩余的记录
        if (!batchRecords.isEmpty()) {
            writeBatch();
        }

        if (writerInitialized) {
            writer.end();
            writer.close();
            writerInitialized = false;
        }
        if (root != null) {
            root.close();
        }

        // 获取数据并清空baOs
        byte[] data = baOs.toByteArray();
        baOs.reset();
        return data;
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("ArrowWriter close begin");
        try {
            if (writerInitialized) {
                // 使用者应该调用endAndReset方法, 确保正确的处理了writer中的batch
                LOGGER.error(
                        "ArrowWriter is not closed properly: caller should use endAndReset method, make sure the batch data in writer is processed.");
                try {
                    writer.end();
                    writer.close();
                } catch (Exception e) {
                    LOGGER.error("ArrowWriter writer close error", e);
                }
                writerInitialized = false;
            }
            if (root != null) {
                root.close();
                root = null;
            }
            allocator.close();
        } catch (Exception e) {
            LOGGER.error("ArrowWriter close error", e);
        }
        LOGGER.info("ArrowWriter close end");
    }
}
