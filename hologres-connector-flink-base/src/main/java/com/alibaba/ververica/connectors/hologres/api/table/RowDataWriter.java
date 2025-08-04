package com.alibaba.ververica.connectors.hologres.api.table;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import java.io.Serializable;
import java.sql.Types;
import java.util.Objects;

/** An interface to transform from RowData to other data type T. */
public interface RowDataWriter<T> extends Serializable {
    static <T> FieldWriter createFieldWriter(
            LogicalType fieldType,
            int hologresType,
            String hologresTypeName,
            RowDataWriter<T> rowDataWriter,
            int columnIndexInHologresTable,
            boolean isJdbcEnableRemoveU0000InText) {
        FieldWriter fieldWriter;
        switch (hologresType) {
            case Types.CHAR:
            case Types.VARCHAR:
                fieldWriter =
                        (obj) -> {
                            if (isJdbcEnableRemoveU0000InText) {
                                rowDataWriter.writeStringRemoveU0000(
                                        (StringData) obj, columnIndexInHologresTable);
                            } else {
                                rowDataWriter.writeString(
                                        (StringData) obj, columnIndexInHologresTable);
                            }
                        };
                break;
            case Types.BIT:
            case Types.BOOLEAN:
                fieldWriter =
                        (obj) -> {
                            rowDataWriter.writeBoolean((Boolean) obj, columnIndexInHologresTable);
                        };
                break;
            case Types.BINARY:
            case Types.VARBINARY:
                fieldWriter =
                        (obj) -> {
                            rowDataWriter.writeBinary((byte[]) obj, columnIndexInHologresTable);
                        };
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalPrecision = LogicalTypeChecks.getPrecision(fieldType);
                int decimalScale = LogicalTypeChecks.getScale(fieldType);
                fieldWriter =
                        (obj) -> {
                            rowDataWriter.writeDecimal(
                                    (DecimalData) obj,
                                    columnIndexInHologresTable,
                                    decimalPrecision,
                                    decimalScale);
                        };
                break;
            case Types.TINYINT:
                fieldWriter =
                        (obj) -> {
                            rowDataWriter.writeByte((Byte) obj, columnIndexInHologresTable);
                        };
                break;
            case Types.SMALLINT:
                fieldWriter =
                        (obj) -> {
                            // SMALLINT should compatible with TINYINT
                            if (obj instanceof Byte) {
                                rowDataWriter.writeShort(
                                        ((Byte) obj).shortValue(), columnIndexInHologresTable);
                            } else {
                                rowDataWriter.writeShort((short) obj, columnIndexInHologresTable);
                            }
                        };
                break;
            case Types.INTEGER:
                fieldWriter =
                        (obj) -> {
                            rowDataWriter.writeInt((Integer) obj, columnIndexInHologresTable);
                        };
                break;
            case Types.DATE:
                fieldWriter =
                        (obj) -> {
                            rowDataWriter.writeDate((Integer) obj, columnIndexInHologresTable);
                        };
                break;
            case Types.TIME:
                fieldWriter =
                        obj -> rowDataWriter.writeTime((Integer) obj, columnIndexInHologresTable);
                break;
            case Types.BIGINT:
                fieldWriter =
                        (obj) -> {
                            rowDataWriter.writeLong((Long) obj, columnIndexInHologresTable);
                        };
                break;
            case Types.REAL:
            case Types.FLOAT:
                fieldWriter =
                        (obj) -> {
                            rowDataWriter.writeFloat((Float) obj, columnIndexInHologresTable);
                        };
                break;
            case Types.DOUBLE:
                fieldWriter =
                        (obj) -> {
                            rowDataWriter.writeDouble((Double) obj, columnIndexInHologresTable);
                        };
                break;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                boolean isFlinkLTZ =
                        fieldType
                                .getTypeRoot()
                                .equals(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
                if (hologresTypeName.equals("timestamptz")) {
                    if (isFlinkLTZ) {
                        // flink TIMESTAMP_LTZ -> holo TIMESTAMPTZ
                        fieldWriter =
                                (obj) -> {
                                    rowDataWriter.writeLTZAsTimestampTz(
                                            (TimestampData) obj, columnIndexInHologresTable);
                                };
                    } else {
                        // flink TIMESTAMP -> holo TIMESTAMPTZ
                        fieldWriter =
                                (obj) -> {
                                    rowDataWriter.writeTimestampTz(
                                            (TimestampData) obj, columnIndexInHologresTable);
                                };
                    }
                } else {
                    if (isFlinkLTZ) {
                        // flink TIMESTAMP_LTZ -> holo TIMESTAMP
                        throw new UnsupportedOperationException(
                                "The hologres connector does not support writing flink timestamp_ltz type to hologres timestamp type, please use hologres timestamp with timezone instead.");
                    } else {
                        // flink TIMESTAMP -> holo TIMESTAMP
                        fieldWriter =
                                (obj) -> {
                                    rowDataWriter.writeTimestamp(
                                            (TimestampData) obj, columnIndexInHologresTable);
                                };
                    }
                }
                break;
            case Types.ARRAY:
                fieldWriter =
                        createArrayFieldWriter(
                                fieldType,
                                hologresTypeName,
                                rowDataWriter,
                                columnIndexInHologresTable);

                break;
            case Types.OTHER:
                // The type check of jdbc copy writer is relatively strict, only supports
                // java.lang.String but not BinaryStringData.
                if (hologresTypeName.equals("json") || hologresTypeName.equals("jsonb")) {
                    fieldWriter =
                            (obj) -> {
                                if (isJdbcEnableRemoveU0000InText) {
                                    rowDataWriter.writeStringRemoveU0000(
                                            (StringData) obj, columnIndexInHologresTable);
                                } else {
                                    rowDataWriter.writeString(
                                            (StringData) obj, columnIndexInHologresTable);
                                }
                            };
                } else {
                    fieldWriter =
                            (obj) -> rowDataWriter.writeObject(obj, columnIndexInHologresTable);
                }
                break;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Hologres sink does not support data type %s for now", fieldType));
        }

        return (obj) -> {
            if (obj == null) {
                rowDataWriter.writeNull(columnIndexInHologresTable);
            } else {
                fieldWriter.writeValue(obj);
            }
        };
    }

    static FieldWriter createArrayFieldWriter(
            LogicalType fieldType,
            String hologresTypeName,
            RowDataWriter rowDataWriter,
            int columnIndexInHologresTable) {
        switch (hologresTypeName) {
            case "_bool":
                return (obj) -> {
                    rowDataWriter.writeBooleanArray(
                            ((ArrayData) obj).toBooleanArray(), columnIndexInHologresTable);
                };
            case "_int4":
                return (obj) -> {
                    rowDataWriter.writeIntArray(
                            ((ArrayData) obj).toIntArray(), columnIndexInHologresTable);
                };
            case "_int8":
                return (obj) -> {
                    rowDataWriter.writeLongArray(
                            ((ArrayData) obj).toLongArray(), columnIndexInHologresTable);
                };
            case "_float8":
                return (obj) -> {
                    rowDataWriter.writeDoubleArray(
                            ((ArrayData) obj).toDoubleArray(), columnIndexInHologresTable);
                };
            case "_float4":
                return (obj) -> {
                    rowDataWriter.writeFloatArray(
                            ((ArrayData) obj).toFloatArray(), columnIndexInHologresTable);
                };
            case "_varchar":
            case "_text":
                return (obj) -> {
                    ArrayData arrayData = (ArrayData) obj;
                    String[] strings = new String[arrayData.size()];
                    for (int i = 0; i < arrayData.size(); i++) {
                        strings[i] = Objects.toString(arrayData.getString(i));
                    }
                    rowDataWriter.writeStringArray(strings, columnIndexInHologresTable);
                };
            default:
                throw new UnsupportedOperationException(
                        "Hologres does not support array type " + hologresTypeName);
        }
    }

    void checkHologresTypeSupported(int hologresType, String typeName);

    void newRecord();

    void writeNull(int index);

    void writeBoolean(Boolean value, int columnIndexInHologresTable);

    void writeByte(Byte value, int columnIndexInHologresTable);

    void writeShort(Short value, int columnIndexInHologresTable);

    void writeInt(Integer value, int columnIndexInHologresTable);

    void writeLong(Long value, int columnIndexInHologresTable);

    void writeFloat(Float value, int columnIndexInHologresTable);

    void writeDouble(Double value, int columnIndexInHologresTable);

    void writeString(StringData value, int columnIndexInHologresTable);

    void writeStringRemoveU0000(StringData value, int columnIndexInHologresTable);

    void writeDate(Integer value, int columnIndexInHologresTable);

    void writeTime(Integer value, int columnIndexInHologresTable);

    void writeLTZAsTimestampTz(TimestampData value, int columnIndexInHologresTable);

    void writeTimestampTz(TimestampData value, int columnIndexInHologresTable);

    void writeTimestamp(TimestampData value, int columnIndexInHologresTable);

    void writeBinary(byte[] value, int columnIndexInHologresTable);

    void writeObject(Object value, int columnIndexInHologresTable);

    void writeDecimal(
            DecimalData value,
            int columnIndexInHologresTable,
            int decimalPrecision,
            int decimalScale);

    void writeIntArray(int[] value, int columnIndexInHologresTable);

    void writeLongArray(long[] value, int columnIndexInHologresTable);

    void writeFloatArray(float[] value, int columnIndexInHologresTable);

    void writeDoubleArray(double[] value, int columnIndexInHologresTable);

    void writeBooleanArray(boolean[] value, int columnIndexInHologresTable);

    void writeStringArray(String[] value, int columnIndexInHologresTable);

    T complete();

    RowDataWriter<T> copy();

    /** FieldWriter. */
    interface FieldWriter extends Serializable {
        void writeValue(Object value);
    }
}
