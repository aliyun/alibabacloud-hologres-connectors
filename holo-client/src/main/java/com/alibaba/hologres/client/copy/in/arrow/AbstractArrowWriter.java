package com.alibaba.hologres.client.copy.in.arrow;

import com.alibaba.hologres.client.model.TableSchema;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
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

    // 攒批相关属性
    private final List<RECORD> batchRecords = new ArrayList<>();
    private final ByteArrayOutputStream baOs = new ByteArrayOutputStream();

    public AbstractArrowWriter(TableSchema schema, List<String> columns, int maxBatchSize) {
        this.schema = schema;
        this.columns = columns;
        this.allocator = new RootAllocator();
        this.arrowSchema = ArrowVectorCreatorUtil.createArrowSchema(schema, columns);
        this.maxBatchSize = maxBatchSize;
    }

    /**
     * 初始化ArrowStreamWriter.
     *
     * @throws IOException 如果初始化失败
     */
    private void initializeWriter() throws IOException {
        if (!writerInitialized) {
            root = VectorSchemaRoot.create(arrowSchema, allocator);

            // 创建新的writer
            writer = new ArrowStreamWriter(root, null, Channels.newChannel(baOs), new IpcOption());
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
