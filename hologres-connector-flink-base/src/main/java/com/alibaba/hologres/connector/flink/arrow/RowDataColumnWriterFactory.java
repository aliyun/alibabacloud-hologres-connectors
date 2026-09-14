package com.alibaba.hologres.connector.flink.arrow;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;

import com.alibaba.hologres.client.copy.in.arrow.ArrowVectorCreatorUtil;
import com.alibaba.hologres.client.copy.in.arrow.creator.AbstractArrowVectorCreator;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.org.postgresql.jdbc.TimestampUtil;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.holders.BitHolder;

import java.sql.Timestamp;

/**
 * RowDataColumnWriter 工厂，根据 Flink LogicalType 和 Hologres Column 类型 创建对应的列写入器.
 *
 * <p>每个写入器直接调用 Arrow 向量的原生 set 方法（如 setSafe(int, int)）， 避免 Object 装箱/拆箱.对于无法直接优化的类型（如
 * Decimal、Array）， 降级使用 holo-client 的标准 AbstractArrowVectorCreator.
 */
public final class RowDataColumnWriterFactory {

    private RowDataColumnWriterFactory() {}

    /** 根据 Arrow FieldVector、Hologres Column 和 Flink LogicalType 创建列写入器. */
    public static RowDataColumnWriter create(
            FieldVector vector, Column column, LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                if (vector instanceof BitVector) {
                    return new BooleanWriter((BitVector) vector);
                }
                break;

            case TINYINT:
                // Hologres 中 TINYINT 映射为 SmallInt
                if (vector instanceof SmallIntVector) {
                    return new TinyIntToSmallIntWriter((SmallIntVector) vector);
                } else if (vector instanceof IntVector) {
                    return new TinyIntToIntWriter((IntVector) vector);
                }
                break;

            case SMALLINT:
                if (vector instanceof SmallIntVector) {
                    return new SmallIntWriter((SmallIntVector) vector);
                }
                break;

            case INTEGER:
                if (vector instanceof IntVector) {
                    return new IntWriter((IntVector) vector);
                }
                break;

            case BIGINT:
                if (vector instanceof BigIntVector) {
                    return new BigIntWriter((BigIntVector) vector);
                }
                break;

            case FLOAT:
                if (vector instanceof Float4Vector) {
                    return new FloatWriter((Float4Vector) vector);
                }
                break;

            case DOUBLE:
                if (vector instanceof Float8Vector) {
                    return new DoubleWriter((Float8Vector) vector);
                }
                break;

            case CHAR:
            case VARCHAR:
                if (vector instanceof VarCharVector) {
                    return new VarCharWriter((VarCharVector) vector);
                }
                break;

            case DATE:
                if (vector instanceof DateDayVector) {
                    return new DateWriter((DateDayVector) vector);
                }
                break;

            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (vector instanceof TimeStampMicroVector) {
                    return new TimestampWriter((TimeStampMicroVector) vector, column, logicalType);
                }
                break;

            default:
                break;
        }

        // 降级：使用 holo-client 标准 Creator（通过 Object 传值）
        return new GenericWriter(
                ArrowVectorCreatorUtil.createColumnVectorCreator(vector, column),
                column,
                logicalType);
    }

    // ========================= 各类型写入器实现 =========================

    /** BOOLEAN → BitVector，避免 Boolean 装箱. */
    static final class BooleanWriter implements RowDataColumnWriter {
        private final BitVector vector;
        private final BitHolder holder = new BitHolder();

        BooleanWriter(BitVector vector) {
            this.vector = vector;
        }

        @Override
        public void write(int rowId, RowData rowData, int pos) {
            holder.value = rowData.getBoolean(pos) ? 1 : 0;
            vector.setSafe(rowId, holder);
        }

        @Override
        public void setNull(int rowId) {
            vector.setNull(rowId);
        }
    }

    /** TINYINT → SmallIntVector，byte 直接转 short. */
    static final class TinyIntToSmallIntWriter implements RowDataColumnWriter {
        private final SmallIntVector vector;

        TinyIntToSmallIntWriter(SmallIntVector vector) {
            this.vector = vector;
        }

        @Override
        public void write(int rowId, RowData rowData, int pos) {
            vector.setSafe(rowId, rowData.getByte(pos));
        }

        @Override
        public void setNull(int rowId) {
            vector.setNull(rowId);
        }
    }

    /** TINYINT → IntVector（当 Hologres 列是 INT4 时）. */
    static final class TinyIntToIntWriter implements RowDataColumnWriter {
        private final IntVector vector;

        TinyIntToIntWriter(IntVector vector) {
            this.vector = vector;
        }

        @Override
        public void write(int rowId, RowData rowData, int pos) {
            vector.setSafe(rowId, rowData.getByte(pos));
        }

        @Override
        public void setNull(int rowId) {
            vector.setNull(rowId);
        }
    }

    /** SMALLINT → SmallIntVector，直接 short 无装箱. */
    static final class SmallIntWriter implements RowDataColumnWriter {
        private final SmallIntVector vector;

        SmallIntWriter(SmallIntVector vector) {
            this.vector = vector;
        }

        @Override
        public void write(int rowId, RowData rowData, int pos) {
            vector.setSafe(rowId, rowData.getShort(pos));
        }

        @Override
        public void setNull(int rowId) {
            vector.setNull(rowId);
        }
    }

    /** INTEGER → IntVector，直接 int 无装箱. */
    static final class IntWriter implements RowDataColumnWriter {
        private final IntVector vector;

        IntWriter(IntVector vector) {
            this.vector = vector;
        }

        @Override
        public void write(int rowId, RowData rowData, int pos) {
            vector.setSafe(rowId, rowData.getInt(pos));
        }

        @Override
        public void setNull(int rowId) {
            vector.setNull(rowId);
        }
    }

    /** BIGINT → BigIntVector，直接 long 无装箱. */
    static final class BigIntWriter implements RowDataColumnWriter {
        private final BigIntVector vector;

        BigIntWriter(BigIntVector vector) {
            this.vector = vector;
        }

        @Override
        public void write(int rowId, RowData rowData, int pos) {
            vector.setSafe(rowId, rowData.getLong(pos));
        }

        @Override
        public void setNull(int rowId) {
            vector.setNull(rowId);
        }
    }

    /** FLOAT → Float4Vector，直接 float 无装箱. */
    static final class FloatWriter implements RowDataColumnWriter {
        private final Float4Vector vector;

        FloatWriter(Float4Vector vector) {
            this.vector = vector;
        }

        @Override
        public void write(int rowId, RowData rowData, int pos) {
            vector.setSafe(rowId, rowData.getFloat(pos));
        }

        @Override
        public void setNull(int rowId) {
            vector.setNull(rowId);
        }
    }

    /** DOUBLE → Float8Vector，直接 double 无装箱. */
    static final class DoubleWriter implements RowDataColumnWriter {
        private final Float8Vector vector;

        DoubleWriter(Float8Vector vector) {
            this.vector = vector;
        }

        @Override
        public void write(int rowId, RowData rowData, int pos) {
            vector.setSafe(rowId, rowData.getDouble(pos));
        }

        @Override
        public void setNull(int rowId) {
            vector.setNull(rowId);
        }
    }

    /** VARCHAR/CHAR → VarCharVector，StringData.toBytes() 直接写入，避免 toString() + getBytes(). */
    static final class VarCharWriter implements RowDataColumnWriter {
        private final VarCharVector vector;

        VarCharWriter(VarCharVector vector) {
            this.vector = vector;
        }

        @Override
        public void write(int rowId, RowData rowData, int pos) {
            StringData sd = rowData.getString(pos);
            vector.setSafe(rowId, sd.toBytes());
        }

        @Override
        public void setNull(int rowId) {
            vector.setNull(rowId);
        }
    }

    /** DATE → DateDayVector，直接 int（epoch days）无装箱. */
    static final class DateWriter implements RowDataColumnWriter {
        private final DateDayVector vector;

        DateWriter(DateDayVector vector) {
            this.vector = vector;
        }

        @Override
        public void write(int rowId, RowData rowData, int pos) {
            vector.setSafe(rowId, rowData.getInt(pos));
        }

        @Override
        public void setNull(int rowId) {
            vector.setNull(rowId);
        }
    }

    /** TIMESTAMP → TimeStampMicroVector，TimestampData 直接转微秒. */
    static final class TimestampWriter implements RowDataColumnWriter {
        private final TimeStampMicroVector vector;
        private final Column column;
        private final LogicalType logicalType;

        TimestampWriter(TimeStampMicroVector vector, Column column, LogicalType logicalType) {
            this.vector = vector;
            this.column = column;
            this.logicalType = logicalType;
        }

        @Override
        public void write(int rowId, RowData rowData, int pos) {
            TimestampData ts = rowData.getTimestamp(pos, getPrecision());
            Timestamp value;
            String holoType;
            boolean timestamptz;
            if (logicalType instanceof LocalZonedTimestampType) {
                validateTimestampType("timestamptz");
                value = Timestamp.from(ts.toInstant());
                holoType = "timestamptz";
                timestamptz = true;
            } else {
                validateTimestampType("timestamp");
                value = ts.toTimestamp();
                holoType = "timestamp";
                timestamptz = false;
            }
            // 转为 java.sql.Timestamp 后使用 holo-client 的工具方法转微秒
            long microSeconds = TimestampUtil.timestampToMicroSecond(value, holoType, timestamptz);
            vector.setSafe(rowId, microSeconds);
        }

        private void validateTimestampType(String expectedType) {
            if (!expectedType.equalsIgnoreCase(column.getTypeName())) {
                throw new IllegalArgumentException(
                        String.format(
                                "Hologres sink does not support writing Flink %s to Hologres %s"
                                        + " column %s; expected Hologres %s",
                                logicalType, column.getTypeName(), column.getName(), expectedType));
            }
        }

        private int getPrecision() {
            if (logicalType instanceof TimestampType) {
                return ((TimestampType) logicalType).getPrecision();
            }
            if (logicalType instanceof LocalZonedTimestampType) {
                return ((LocalZonedTimestampType) logicalType).getPrecision();
            }
            return 6;
        }

        @Override
        public void setNull(int rowId) {
            vector.setNull(rowId);
        }
    }

    /**
     * 降级写入器：对于不常见的类型（Decimal、Array、Binary 等）， 使用 holo-client 标准 AbstractArrowVectorCreator，通过
     * Object 传值.
     */
    static final class GenericWriter implements RowDataColumnWriter {
        private final AbstractArrowVectorCreator creator;
        private final Column column;
        private final LogicalType logicalType;

        GenericWriter(AbstractArrowVectorCreator creator, Column column, LogicalType logicalType) {
            this.creator = creator;
            this.column = column;
            this.logicalType = logicalType;
        }

        @Override
        public void write(int rowId, RowData rowData, int pos) {
            Object value = extractValue(rowData, pos);
            creator.set(rowId, value);
        }

        @Override
        public void setNull(int rowId) {
            creator.set(rowId, null);
        }

        private Object convertFlinkArrayToJavaArray(ArrayData arrayData) {
            if (arrayData == null) {
                return null;
            }
            LogicalType elementType = ((ArrayType) logicalType).getElementType();
            int size = arrayData.size();
            switch (elementType.getTypeRoot()) {
                case INTEGER:
                    int[] intArr = new int[size];
                    for (int i = 0; i < size; i++) {
                        intArr[i] = arrayData.getInt(i);
                    }
                    return intArr;
                case BIGINT:
                    long[] longArr = new long[size];
                    for (int i = 0; i < size; i++) {
                        longArr[i] = arrayData.getLong(i);
                    }
                    return longArr;
                case FLOAT:
                    float[] floatArr = new float[size];
                    for (int i = 0; i < size; i++) {
                        floatArr[i] = arrayData.getFloat(i);
                    }
                    return floatArr;
                case DOUBLE:
                    double[] doubleArr = new double[size];
                    for (int i = 0; i < size; i++) {
                        doubleArr[i] = arrayData.getDouble(i);
                    }
                    return doubleArr;
                case BOOLEAN:
                    boolean[] boolArr = new boolean[size];
                    for (int i = 0; i < size; i++) {
                        boolArr[i] = arrayData.getBoolean(i);
                    }
                    return boolArr;
                case VARCHAR:
                case CHAR:
                    String[] strArr = new String[size];
                    for (int i = 0; i < size; i++) {
                        StringData sd = arrayData.getString(i);
                        strArr[i] = sd != null ? sd.toString() : null;
                    }
                    return strArr;
                default:
                    Object[] objArr = new Object[size];
                    for (int i = 0; i < size; i++) {
                        objArr[i] = arrayData.getString(i).toString();
                    }
                    return objArr;
            }
        }

        private Object extractValue(RowData rowData, int pos) {
            switch (logicalType.getTypeRoot()) {
                case DECIMAL:
                    DecimalType dt = (DecimalType) logicalType;
                    return rowData.getDecimal(pos, dt.getPrecision(), dt.getScale()).toBigDecimal();
                case VARBINARY:
                case BINARY:
                    return rowData.getBinary(pos);
                case ARRAY:
                    return convertFlinkArrayToJavaArray(rowData.getArray(pos));
                case VARCHAR:
                case CHAR:
                    StringData sd = rowData.getString(pos);
                    return sd != null ? sd.toString() : null;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    validateTimestampType("timestamp");
                    return rowData.getTimestamp(pos, ((TimestampType) logicalType).getPrecision())
                            .toTimestamp();
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    validateTimestampType("timestamptz");
                    return Timestamp.from(
                            rowData.getTimestamp(
                                            pos,
                                            ((LocalZonedTimestampType) logicalType).getPrecision())
                                    .toInstant());
                default:
                    throw new IllegalArgumentException(
                            "Hologres sink does not support data type " + logicalType);
            }
        }

        private void validateTimestampType(String expectedType) {
            if (!expectedType.equalsIgnoreCase(column.getTypeName())) {
                throw new IllegalArgumentException(
                        String.format(
                                "Hologres sink does not support writing Flink %s to Hologres %s"
                                        + " column %s; expected Hologres %s",
                                logicalType, column.getTypeName(), column.getName(), expectedType));
            }
        }
    }
}
