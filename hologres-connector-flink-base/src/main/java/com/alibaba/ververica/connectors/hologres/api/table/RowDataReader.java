package com.alibaba.ververica.connectors.hologres.api.table;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import com.alibaba.ververica.connectors.hologres.utils.PostgresTypeUtil;

import java.io.Serializable;
import java.sql.Types;

/** An interface to transform other data type T to RowData. */
public interface RowDataReader<T> extends Serializable {
    static <T> RowDataReader.FieldReader<T> createFieldReader(
            LogicalType fieldType,
            int hologresType,
            String hologresTypeName,
            RowDataReader<T> rowDataReader,
            int index) {
        switch (hologresType) {
            case Types.CHAR:
            case Types.VARCHAR:
                return (obj) -> rowDataReader.readString(obj, index);
            case Types.BIT:
            case Types.BOOLEAN:
                return (obj) -> rowDataReader.readBoolean(obj, index);
            case Types.BINARY:
            case Types.VARBINARY:
                return (obj) -> rowDataReader.readBinary(obj, index);
            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalPrecision = LogicalTypeChecks.getPrecision(fieldType);
                int decimalScale = LogicalTypeChecks.getScale(fieldType);
                return (obj) ->
                        rowDataReader.readDecimal(obj, index, decimalPrecision, decimalScale);
            case Types.TINYINT:
                return (obj) -> rowDataReader.readByte(obj, index);
            case Types.SMALLINT:
                return (obj) -> rowDataReader.readShort(obj, index);
            case Types.INTEGER:
                return (obj) -> rowDataReader.readInt(obj, index);
            case Types.DATE:
                return (obj) -> rowDataReader.readDate(obj, index);
            case Types.BIGINT:
                return (obj) -> rowDataReader.readLong(obj, index);
            case Types.REAL:
            case Types.FLOAT:
                return (obj) -> rowDataReader.readFloat(obj, index);
            case Types.DOUBLE:
                return (obj) -> rowDataReader.readDouble(obj, index);
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                boolean isFlinkLTZ =
                        fieldType
                                .getTypeRoot()
                                .equals(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
                if (hologresTypeName.equals(PostgresTypeUtil.PG_TIMESTAMPTZ)) {
                    if (isFlinkLTZ) {
                        // hologres timestamptz -> flink timestamp_ltz
                        return (obj) -> rowDataReader.readTimestamptzAsLTZ(obj, index);
                    } else {
                        // hologres timestamptz -> flink timestamp
                        return (obj) -> rowDataReader.readTimestamptz(obj, index);
                    }
                } else {
                    if (isFlinkLTZ) {
                        // hologres timestamp -> flink timestamp_ltz
                        throw new UnsupportedOperationException(
                                "The hologres connector doesn't support reading the hologres timestamp type as the flink timestamp_ltz type, please use hologres timestamp with timezone instead.");
                    } else {
                        // hologres timestamp -> flink timestamp
                        return (obj) -> rowDataReader.readTimestamp(obj, index);
                    }
                }
            case Types.ARRAY:
                return createArrayFieldReader(fieldType, hologresTypeName, rowDataReader, index);
            case Types.OTHER:
                return (obj) -> rowDataReader.readObject(obj, index);
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Hologres sink does not support data type %s for now", fieldType));
        }
    }

    static <T> RowDataReader.FieldReader<T> createArrayFieldReader(
            LogicalType fieldType,
            String hologresTypeName,
            RowDataReader<T> rowDataReader,
            int index) {
        switch (hologresTypeName) {
            case "_bool":
                return (obj) -> rowDataReader.readBooleanArray(obj, index);
            case "_int4":
                return (obj) -> rowDataReader.readIntArray(obj, index);
            case "_int8":
                return (obj) -> rowDataReader.readLongArray(obj, index);
            case "_float8":
                return (obj) -> rowDataReader.readDoubleArray(obj, index);
            case "_float4":
                return (obj) -> rowDataReader.readFloatArray(obj, index);
            case "_varchar":
            case "_text":
                return (obj) -> rowDataReader.readStringArray(obj, index);
            default:
                throw new UnsupportedOperationException(
                        "Hologres does not support array type " + hologresTypeName);
        }
    }

    void checkHologresTypeSupported(int hologresType, String typeName);

    Boolean readBoolean(T record, int index);

    Byte readByte(T record, int index);

    Short readShort(T record, int index);

    Integer readInt(T record, int index);

    Float readFloat(T record, int index);

    Double readDouble(T record, int index);

    StringData readString(T record, int index);

    Integer readDate(T record, int index);

    Long readLong(T record, int index);

    TimestampData readTimestamptzAsLTZ(T record, int index);

    TimestampData readTimestamptz(T record, int index);

    TimestampData readTimestamp(T record, int index);

    Object readObject(T record, int index);

    byte[] readBinary(T record, int index);

    DecimalData readDecimal(T record, int index, int decimalPrecision, int decimalScale);

    GenericArrayData readIntArray(T record, int index);

    GenericArrayData readLongArray(T record, int index);

    GenericArrayData readFloatArray(T record, int index);

    GenericArrayData readDoubleArray(T record, int index);

    GenericArrayData readBooleanArray(T record, int index);

    GenericArrayData readStringArray(T record, int index);

    /** FieldReader. */
    interface FieldReader<T> extends Serializable {
        Object readValue(T value);
    }
}
