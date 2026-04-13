package com.alibaba.hologres.client.copy.out.arrow;

import com.alibaba.hologres.client.copy.out.arrow.accessor.AbstractArrowVectorAccessor;
import com.alibaba.hologres.client.copy.out.arrow.accessor.BaseArrowArrayAccessor;
import com.alibaba.hologres.client.copy.out.arrow.accessor.BaseArrowBigIntAccessor;
import com.alibaba.hologres.client.copy.out.arrow.accessor.BaseArrowDateDayAccessor;
import com.alibaba.hologres.client.copy.out.arrow.accessor.BaseArrowDateMilliAccessor;
import com.alibaba.hologres.client.copy.out.arrow.accessor.BaseArrowDecimalAccessor;
import com.alibaba.hologres.client.copy.out.arrow.accessor.BaseArrowFixedSizeBinaryAccessor;
import com.alibaba.hologres.client.copy.out.arrow.accessor.BaseArrowFloat4Accessor;
import com.alibaba.hologres.client.copy.out.arrow.accessor.BaseArrowFloat8Accessor;
import com.alibaba.hologres.client.copy.out.arrow.accessor.BaseArrowIntAccessor;
import com.alibaba.hologres.client.copy.out.arrow.accessor.BaseArrowSmallIntAccessor;
import com.alibaba.hologres.client.copy.out.arrow.accessor.BaseArrowTimeMicroAccessor;
import com.alibaba.hologres.client.copy.out.arrow.accessor.BaseArrowTimeStampMicroAccessor;
import com.alibaba.hologres.client.copy.out.arrow.accessor.BaseArrowUInt1Accessor;
import com.alibaba.hologres.client.copy.out.arrow.accessor.BaseArrowVarBinaryAccessor;
import com.alibaba.hologres.client.copy.out.arrow.accessor.BaseArrowVarCharAccessor;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
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
import org.apache.arrow.vector.types.pojo.Field;

import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/** HoloArrowVectorAccessorUtil. */
public class ArrowVectorAccessorUtil {

    /**
     * Convert VectorSchemaRoot to Records. 对jsonb类型,我们读取时进行了格式转换,如jsonb_column_1::text,
     * 因此arrow结果root.getSchema()中的字段名不是原本的jsonb_column_1,而是jsonb_out
     *
     * <p>根据传入的列, 创建相应的accessor. 列的数量和arrow结果中需要一致
     */
    public static List<Record> convertVectorSchemaRootToRecords(
            VectorSchemaRoot root, TableSchema schema, List<String> columns) throws IOException {
        if (root == null) {
            throw new IOException("Please call nextBatch() first.");
        }
        List<Record> currentRecords = new ArrayList<>();
        int rowCount = root.getRowCount();
        for (int i = 0; i < rowCount; ++i) {
            Record record = new Record(schema);
            // columns 与 copy out sql 中的列顺序和数量要完全一致
            if (columns.size() != root.getSchema().getFields().size()) {
                throw new IOException(
                        "The number of columns is not equal to the number of vectors.");
            }
            for (int j = 0; j < columns.size(); ++j) {
                FieldVector vector = root.getFieldVectors().get(j);
                int index = schema.getColumnIndex(columns.get(j));
                Column column = schema.getColumn(index);

                AbstractArrowVectorAccessor columnVectorAccessor =
                        ArrowVectorAccessorUtil.createColumnVectorAccessor(vector, column);
                record.setObject(index, columnVectorAccessor.get(i));
            }
            currentRecords.add(record);
        }
        return currentRecords;
    }

    // 根据列名和类型, 创建相应的accessor
    public static List<Record> convertVectorSchemaRootToRecords(
            VectorSchemaRoot root, TableSchema schema) throws IOException {
        if (root == null) {
            throw new IOException("Please call nextBatch() first.");
        }
        List<Record> currentRecords = new ArrayList<>();
        int rowCount = root.getRowCount();
        for (int i = 0; i < rowCount; ++i) {
            Record record = new Record(schema);
            for (Field field : root.getSchema().getFields()) {
                FieldVector vector = root.getVector(field.getName());
                Column column = schema.getColumn(schema.getColumnIndex(field.getName()));
                AbstractArrowVectorAccessor columnVectorAccessor =
                        ArrowVectorAccessorUtil.createColumnVectorAccessor(vector, column);

                record.setObject(
                        schema.getColumnIndex(field.getName()), columnVectorAccessor.get(i));
            }
            currentRecords.add(record);
        }
        return currentRecords;
    }

    public static AbstractArrowVectorAccessor createColumnVectorAccessor(
            FieldVector vector, Column column) {
        switch (column.getType()) {
            case Types.INTEGER:
                return new BaseArrowIntAccessor((IntVector) vector);
            case Types.BIGINT:
                return new BaseArrowBigIntAccessor((BigIntVector) vector);
            case Types.SMALLINT:
                return new BaseArrowSmallIntAccessor((SmallIntVector) vector);
            case Types.FLOAT:
            case Types.REAL:
                return new BaseArrowFloat4Accessor((Float4Vector) vector);
            case Types.DOUBLE:
                return new BaseArrowFloat8Accessor((Float8Vector) vector);
            case Types.DECIMAL:
            case Types.NUMERIC:
                return new BaseArrowDecimalAccessor((DecimalVector) vector, column.getScale());
            case Types.BOOLEAN:
            case Types.BIT:
                return new BaseArrowUInt1Accessor((UInt1Vector) vector);
            case Types.CHAR:
                return new BaseArrowVarCharAccessor((VarCharVector) vector, column.getPrecision());
            case Types.VARCHAR:
                return new BaseArrowVarCharAccessor((VarCharVector) vector);
            case Types.BINARY:
            case Types.VARBINARY:
                return new BaseArrowVarBinaryAccessor((VarBinaryVector) vector);
            case Types.TIMESTAMP:
            case Types.TIME_WITH_TIMEZONE:
                if (column.getTypeName().equals("timestamptz")) {
                    return new BaseArrowDateMilliAccessor((DateMilliVector) vector);
                } else {
                    return new BaseArrowTimeStampMicroAccessor((TimeStampMicroVector) vector);
                }
            case Types.TIME:
                if (column.getTypeName().equals("timetz")) {
                    return new BaseArrowFixedSizeBinaryAccessor((FixedSizeBinaryVector) vector);
                } else {
                    return new BaseArrowTimeMicroAccessor((TimeMicroVector) vector);
                }
            case Types.DATE:
                return new BaseArrowDateDayAccessor((DateDayVector) vector);
            case Types.ARRAY:
                return new BaseArrowArrayAccessor(
                        (ListVector) vector, column.getArrayElementType());
            case Types.OTHER:
                if (column.getTypeName().equals("roaringbitmap")) {
                    return new BaseArrowVarBinaryAccessor((VarBinaryVector) vector);
                } else {
                    return new BaseArrowVarCharAccessor((VarCharVector) vector);
                }
            default:
                throw new IllegalArgumentException(
                        "Unsupported column type: " + column.getTypeName());
        }
    }
}
