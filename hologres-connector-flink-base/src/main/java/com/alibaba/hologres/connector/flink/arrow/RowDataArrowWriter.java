package com.alibaba.hologres.connector.flink.arrow;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.alibaba.hologres.client.copy.in.arrow.AbstractArrowWriter;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.TableSchema;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;

import java.io.IOException;
import java.util.List;

/**
 * 直接将 Flink RowData 转换为 Arrow 向量的 Writer. 跳过 RowData → Put/Record 的中间转换，直接写入 Arrow.
 *
 * <p>使用 {@link RowDataColumnWriter} 接口，每列一个写入器实例， 直接调用 Arrow 向量的原生 set 方法，避免 Object 装箱/拆箱.
 *
 * <p>优化点：
 *
 * <ul>
 *   <li>消除 Put/Record 对象创建开销
 *   <li>基础类型（int, long, float, double, short, boolean）零装箱
 *   <li>VARCHAR/CHAR 类型直接 StringData.toBytes() → VarCharVector.setSafe()
 *   <li>不常见类型（Decimal, Array 等）降级使用 holo-client 标准 Creator
 * </ul>
 */
public class RowDataArrowWriter extends AbstractArrowWriter<RowData> {

    /** 对应 columns 中每列在 TableSchema 中的 Column 元信息. */
    private final Column[] selectedColumns;

    /** 对应 columns 中每列的 Flink LogicalType. */
    private final LogicalType[] logicalTypes;

    // ---- 运行时状态 ----
    private transient VectorSchemaRoot boundRoot;
    private transient RowDataColumnWriter[] columnWriters;

    public RowDataArrowWriter(
            TableSchema schema, List<String> columns, int maxBatchSize, RowType rowType) {
        this(schema, columns, maxBatchSize, rowType, false);
    }

    public RowDataArrowWriter(
            TableSchema schema,
            List<String> columns,
            int maxBatchSize,
            RowType rowType,
            boolean enableCompression) {
        super(schema, columns, maxBatchSize, enableCompression);

        int columnCount = columns.size();
        this.selectedColumns = new Column[columnCount];
        this.logicalTypes = new LogicalType[columnCount];

        List<RowType.RowField> fields = rowType.getFields();
        for (int i = 0; i < columnCount; i++) {
            String columnName = columns.get(i);
            int columnIndex = schema.getColumnIndex(columnName);
            if (columnIndex < 0) {
                throw new IllegalArgumentException("Column not found in schema: " + columnName);
            }
            this.selectedColumns[i] = schema.getColumn(columnIndex);
            this.logicalTypes[i] = fields.get(i).getType();
        }
    }

    @Override
    public void fillVectorSchemaRoot(VectorSchemaRoot root, List<RowData> rowDataList)
            throws IOException {
        if (rowDataList == null || rowDataList.isEmpty()) {
            resetRoot(root);
            return;
        }

        ensureColumnWriters(root);
        int rowCount = rowDataList.size();
        int columnCount = columnWriters.length;
        prepareRootForBatch(root, rowCount);

        for (int rowId = 0; rowId < rowCount; rowId++) {
            RowData rowData = rowDataList.get(rowId);

            for (int col = 0; col < columnCount; col++) {
                if (rowData.isNullAt(col)) {
                    columnWriters[col].setNull(rowId);
                } else {
                    columnWriters[col].write(rowId, rowData, col);
                }
            }
        }

        finalizeRoot(root, rowCount);
    }

    /** 根据 VectorSchemaRoot 创建/重建所有列写入器. */
    private void ensureColumnWriters(VectorSchemaRoot root) {
        if (root != this.boundRoot || this.columnWriters == null) {
            List<FieldVector> fieldVectors = root.getFieldVectors();
            if (fieldVectors.size() != selectedColumns.length) {
                throw new IllegalArgumentException(
                        "Column count mismatch, vectors="
                                + fieldVectors.size()
                                + ", columns="
                                + selectedColumns.length);
            }

            this.columnWriters = new RowDataColumnWriter[selectedColumns.length];
            for (int i = 0; i < selectedColumns.length; i++) {
                this.columnWriters[i] =
                        RowDataColumnWriterFactory.create(
                                fieldVectors.get(i), selectedColumns[i], logicalTypes[i]);
            }

            this.boundRoot = root;
        }
    }

    // ====================== Arrow 批处理辅助方法 ======================

    private void resetRoot(VectorSchemaRoot root) {
        for (FieldVector vector : root.getFieldVectors()) {
            vector.reset();
            vector.setValueCount(0);
        }
        root.setRowCount(0);
    }

    private void prepareRootForBatch(VectorSchemaRoot root, int rowCount) {
        List<FieldVector> vectors = root.getFieldVectors();
        for (int i = 0; i < vectors.size(); i++) {
            FieldVector vector = vectors.get(i);
            vector.reset();
            ensureVectorCapacity(vector, selectedColumns[i], rowCount);
            vector.setValueCount(0);
        }
        root.setRowCount(0);
    }

    private void finalizeRoot(VectorSchemaRoot root, int rowCount) {
        for (FieldVector vector : root.getFieldVectors()) {
            vector.setValueCount(rowCount);
        }
        root.setRowCount(rowCount);
    }

    private void ensureVectorCapacity(FieldVector vector, Column column, int rowCount) {
        if (vector instanceof BaseFixedWidthVector) {
            BaseFixedWidthVector fixed = (BaseFixedWidthVector) vector;
            fixed.setInitialCapacity(rowCount);
            fixed.allocateNew();
        } else if (vector instanceof BaseVariableWidthVector) {
            BaseVariableWidthVector variable = (BaseVariableWidthVector) vector;
            int estimatedBytes = estimateVariableWidthBytes(column, rowCount);
            variable.allocateNew((long) estimatedBytes, rowCount);
        } else if (vector instanceof ListVector) {
            ListVector listVector = (ListVector) vector;
            listVector.setInitialCapacity(rowCount);
            listVector.allocateNew();
        } else {
            vector.setInitialCapacity(rowCount);
            vector.allocateNew();
        }
    }

    private int estimateVariableWidthBytes(Column column, int rowCount) {
        switch (column.getType()) {
            case -3: // VARBINARY
            case -2: // BINARY
                return rowCount * 64;
            case 1: // CHAR
                if (column.getPrecision() > 0) {
                    return Math.max(rowCount * column.getPrecision() * 2, rowCount * 8);
                }
                return rowCount * 64;
            case 12: // VARCHAR
            case 1111: // OTHER (json, jsonb, etc.)
                if ("roaringbitmap".equals(column.getTypeName())) {
                    return rowCount * 64;
                }
                if (column.getPrecision() > 0) {
                    return Math.max(rowCount * column.getPrecision() * 2, rowCount * 8);
                }
                return rowCount * 64;
            default:
                return rowCount * 128;
        }
    }
}
