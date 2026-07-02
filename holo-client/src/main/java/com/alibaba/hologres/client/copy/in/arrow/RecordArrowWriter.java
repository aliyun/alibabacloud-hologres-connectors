package com.alibaba.hologres.client.copy.in.arrow;

import com.alibaba.hologres.client.copy.in.arrow.creator.AbstractArrowVectorCreator;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;

import java.io.IOException;
import java.util.List;

/** Arrow格式写入器，用于将HoloClient的Record攒批写为Arrow格式. */
public class RecordArrowWriter extends AbstractArrowWriter<Record> {

    private static final int DEFAULT_ESTIMATED_VARCHAR_BYTES_PER_ROW = 64;
    private static final int DEFAULT_ESTIMATED_VARBINARY_BYTES_PER_ROW = 64;
    private static final int DEFAULT_ESTIMATED_LIST_BYTES_PER_ROW = 128;

    private final int[] columnIndexes;
    private final Column[] selectedColumns;

    /** 当前 root 绑定的 creators；如果 root 变化，需要重新初始化 */
    private transient VectorSchemaRoot boundRoot;

    private transient AbstractArrowVectorCreator[] creators;

    public RecordArrowWriter(TableSchema schema, List<String> columns, int maxBatchSize) {
        this(schema, columns, maxBatchSize, false);
    }

    /**
     * 创建 RecordArrowWriter，支持可选的 Arrow IPC LZ4 压缩.
     *
     * @param schema 表 schema
     * @param columns 列名列表
     * @param maxBatchSize 每批最大行数
     * @param enableCompression 是否启用 Arrow IPC V5 buffer-level LZ4 压缩
     */
    public RecordArrowWriter(
            TableSchema schema, List<String> columns, int maxBatchSize, boolean enableCompression) {
        super(schema, columns, maxBatchSize, enableCompression);

        int columnCount = columns.size();
        this.columnIndexes = new int[columnCount];
        this.selectedColumns = new Column[columnCount];

        for (int i = 0; i < columnCount; i++) {
            String columnName = columns.get(i);
            int columnIndex = schema.getColumnIndex(columnName);
            if (columnIndex < 0) {
                throw new IllegalArgumentException("Column not found in schema: " + columnName);
            }
            this.columnIndexes[i] = columnIndex;
            this.selectedColumns[i] = schema.getColumn(columnIndex);
        }
    }

    @Override
    public void fillVectorSchemaRoot(VectorSchemaRoot root, List<Record> recordList)
            throws IOException {
        if (recordList == null || recordList.isEmpty()) {
            resetRoot(root);
            return;
        }

        ensureCreators(root);

        final int rowCount = recordList.size();
        final int columnCount = columnIndexes.length;

        // 批前复位 + 预分配，尽量复用已有 vector
        prepareRootForBatch(root, rowCount);

        for (int rowId = 0; rowId < rowCount; rowId++) {
            Record record = recordList.get(rowId);
            for (int col = 0; col < columnCount; col++) {
                Object value = record.getObject(columnIndexes[col]);
                creators[col].set(rowId, value);
            }
        }

        // 统一设置 valueCount
        finalizeRoot(root, rowCount);
    }

    private void ensureCreators(VectorSchemaRoot root) {
        if (root == boundRoot && creators != null) {
            return;
        }

        List<FieldVector> fieldVectors = root.getFieldVectors();
        if (fieldVectors.size() != selectedColumns.length) {
            throw new IllegalArgumentException(
                    "Column count mismatch, vectors="
                            + fieldVectors.size()
                            + ", columns="
                            + selectedColumns.length);
        }

        this.creators = new AbstractArrowVectorCreator[selectedColumns.length];
        for (int i = 0; i < selectedColumns.length; i++) {
            this.creators[i] =
                    ArrowVectorCreatorUtil.createColumnVectorCreator(
                            fieldVectors.get(i), selectedColumns[i]);
        }
        this.boundRoot = root;
    }

    /** 轻量 reset root，不 clear 掉整个已分配结构。 */
    private void resetRoot(VectorSchemaRoot root) {
        List<FieldVector> vectors = root.getFieldVectors();
        for (FieldVector vector : vectors) {
            resetVector(vector);
            vector.setValueCount(0);
        }
        root.setRowCount(0);
    }

    /** 为当前 batch 做复位和容量准备，尽量避免每次 clear + allocateNew。 */
    private void prepareRootForBatch(VectorSchemaRoot root, int rowCount) {
        List<FieldVector> vectors = root.getFieldVectors();

        if (vectors.size() != selectedColumns.length) {
            throw new IllegalArgumentException(
                    "Column count mismatch, vectors="
                            + vectors.size()
                            + ", columns="
                            + selectedColumns.length);
        }

        for (int i = 0; i < vectors.size(); i++) {
            FieldVector vector = vectors.get(i);
            Column column = selectedColumns[i];

            resetVector(vector);
            ensureVectorCapacity(vector, column, rowCount);
            vector.setValueCount(0);
        }

        root.setRowCount(0);
    }

    /** 批量写完后统一收口。 */
    private void finalizeRoot(VectorSchemaRoot root, int rowCount) {
        List<FieldVector> vectors = root.getFieldVectors();
        for (FieldVector vector : vectors) {
            vector.setValueCount(rowCount);
        }
        root.setRowCount(rowCount);
    }

    /** 尽量复用 vector，重置其内容状态而不是彻底 clear + 重新 allocate。 */
    private void resetVector(FieldVector vector) {
        // reset() 通常比 clear()+allocateNew() 更适合复用场景
        // 如果某些特定 vector 子类行为有差异，可再针对性处理
        vector.reset();
    }

    /** 按类型为 vector 做容量准备，减少 setSafe 时的扩容次数。 */
    private void ensureVectorCapacity(FieldVector vector, Column column, int rowCount) {
        if (vector instanceof BaseFixedWidthVector) {
            BaseFixedWidthVector fixed = (BaseFixedWidthVector) vector;
            fixed.setInitialCapacity(rowCount);
            fixed.allocateNew();
            return;
        }

        if (vector instanceof BaseVariableWidthVector) {
            BaseVariableWidthVector variable = (BaseVariableWidthVector) vector;
            int estimatedBytes = estimateVariableWidthBytes(column, rowCount);
            variable.allocateNew(estimatedBytes, rowCount);
            return;
        }

        if (vector instanceof ListVector) {
            ListVector listVector = (ListVector) vector;
            listVector.setInitialCapacity(rowCount);
            listVector.allocateNew();
            return;
        }

        // 兜底：对于其他 vector，至少保证有可写空间
        vector.setInitialCapacity(rowCount);
        vector.allocateNew();
    }

    /** 粗略估算变长列每批所需字节，避免 VarChar/VarBinary 在 setSafe 中频繁扩容。 */
    private int estimateVariableWidthBytes(Column column, int rowCount) {
        switch (column.getType()) {
            case java.sql.Types.CHAR:
                if (column.getPrecision() > 0) {
                    // 粗略按 UTF-8 平均 2 bytes/char 估，保守点可乘 3
                    return Math.max(rowCount * column.getPrecision() * 2, rowCount * 8);
                }
                return rowCount * DEFAULT_ESTIMATED_VARCHAR_BYTES_PER_ROW;

            case java.sql.Types.VARCHAR:
            case java.sql.Types.OTHER:
                if ("roaringbitmap".equals(column.getTypeName())) {
                    return rowCount * DEFAULT_ESTIMATED_VARBINARY_BYTES_PER_ROW;
                }
                if (column.getPrecision() > 0) {
                    return Math.max(rowCount * column.getPrecision() * 2, rowCount * 8);
                }
                return rowCount * DEFAULT_ESTIMATED_VARCHAR_BYTES_PER_ROW;

            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
                return rowCount * DEFAULT_ESTIMATED_VARBINARY_BYTES_PER_ROW;

            default:
                return rowCount * DEFAULT_ESTIMATED_LIST_BYTES_PER_ROW;
        }
    }
}
