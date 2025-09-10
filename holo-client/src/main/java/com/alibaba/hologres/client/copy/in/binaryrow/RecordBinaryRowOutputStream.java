package com.alibaba.hologres.client.copy.in.binaryrow;

import com.alibaba.blink.dataformat.BinaryArray;
import com.alibaba.blink.dataformat.BinaryArrayWriter;
import com.alibaba.blink.dataformat.BinaryRow;
import com.alibaba.blink.dataformat.BinaryRowWriter;
import com.alibaba.hologres.client.copy.in.RecordBinaryOutputStream;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.CommonUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.TimestampUtil;
import org.postgresql.jdbc.TimestampUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalTime;
import java.util.TimeZone;

/** Record转binaryrow. */
public class RecordBinaryRowOutputStream extends RecordBinaryOutputStream {

    public RecordBinaryRowOutputStream(
            OutputStream os, TableSchema schema, BaseConnection conn, int maxCellBufferSize) {
        super(os, schema, conn, maxCellBufferSize);
    }

    @Override
    protected void fillByteBuffer(Record record) throws IOException {
        if (!fillHeader) {
            fillHeader = true;
            fillHeader();
        }

        writeShort((short) record.getBitSet().cardinality());
        byte[] bytes = convertRecordToBinaryRow(record);
        writeInt(bytes.length);
        write(bytes);
    }

    private byte[] convertRecordToBinaryRow(Record record) throws IOException {
        BinaryRow row = new BinaryRow(record.getBitSet().cardinality());
        BinaryRowWriter w = new BinaryRowWriter(row);
        int srcIndex = 0;
        int dstIndex = 0;
        for (Column column : record.getSchema().getColumnSchema()) {
            try {
                if (record.isSet(srcIndex)) {
                    convertToBinaryRow(
                            w, dstIndex, record.getObject(srcIndex), column, timestampUtils);
                    ++dstIndex;
                }
            } catch (Exception e) {
                throw new IOException(
                        "fail to convert column "
                                + column.getName()
                                + " type "
                                + column.getTypeName()
                                + " value "
                                + record.getObject(srcIndex)
                                + " to binary",
                        e);
            }
            ++srcIndex;
        }
        w.complete();

        byte[] data = row.serializeToBytes();
        return data;
    }

    public static void convertToBinaryRow(
            BinaryRowWriter writer,
            int dstIndex,
            Object obj,
            Column column,
            TimestampUtils timestampUtils)
            throws IOException, SQLException {
        if (obj == null) {
            writer.setNullAt(dstIndex);
            return;
        }
        int type = column.getType();
        String typeName = column.getTypeName();
        String columnName = column.getName();
        switch (type) {
            case Types.SMALLINT:
                if (obj instanceof Number) {
                    writer.writeShort(dstIndex, ((Number) obj).shortValue());
                    break;
                } else {
                    throw new IOException(
                            "unsupported class for int2 : " + obj.getClass().getName());
                }
            case Types.INTEGER:
                if (obj instanceof Number) {
                    writer.writeInt(dstIndex, ((Number) obj).intValue());
                    break;
                } else {
                    throw new IOException(
                            "unsupported class for int4 : " + obj.getClass().getName());
                }
            case Types.BIGINT:
                if (obj instanceof Number) {
                    writer.writeLong(dstIndex, ((Number) obj).longValue());
                    break;
                } else {
                    throw new IOException(
                            "unsupported class for int8 : " + obj.getClass().getName());
                }
            case Types.VARCHAR:
            case Types.CHAR:
                {
                    String str = obj.toString();
                    CommonUtil.pgVerifyMbstrLen(str);
                    int maxLen = column.getPrecision();
                    if (typeName != "text"
                            && maxLen > 0
                            && maxLen != Integer.MAX_VALUE) { // 未指定精度时，maxLen = Integer.MAX_VALUE
                        if (type == Types.VARCHAR) {
                            str = CommonUtil.varcharInput(str, maxLen);
                        }
                        if (type == Types.CHAR) {
                            str = CommonUtil.bpcharInput(str, maxLen);
                        }
                    }
                    writer.writeString(dstIndex, str);
                    break;
                }
            case Types.BIT:
                if ("bool".equals(typeName)) {
                    if (obj instanceof Boolean) {
                        writer.writeBoolean(dstIndex, (Boolean) obj);
                    } else if (obj instanceof Number) {
                        writer.writeBoolean(dstIndex, ((Number) obj).intValue() > 0 ? true : false);
                    } else {
                        throw new IOException(
                                "unsupported class for bool : " + obj.getClass().getName());
                    }
                } else {
                    throw new IOException("unsupported type:" + typeName);
                }
                break;

            case Types.REAL:
                if ("float4".equals(typeName)) {
                    if (obj instanceof Float) {
                        writer.writeFloat(dstIndex, (Float) obj);
                    } else if (obj instanceof Number) {
                        writer.writeFloat(dstIndex, ((Number) obj).floatValue());
                    } else {
                        throw new IOException(
                                "unsupported class for bool : " + obj.getClass().getName());
                    }
                } else {
                    throw new IOException("unsupported type:" + typeName);
                }
                break;
            case Types.DOUBLE:
                if ("float8".equals(typeName)) {
                    if (obj instanceof Double) {
                        writer.writeDouble(dstIndex, (Double) obj);
                    } else if (obj instanceof Number) {
                        writer.writeDouble(dstIndex, ((Number) obj).doubleValue());
                    } else {
                        throw new IOException(
                                "unsupported class for bool : " + obj.getClass().getName());
                    }
                } else {
                    throw new IOException("unsupported type:" + typeName);
                }
                break;
            case Types.DATE:
                {
                    byte[] val = new byte[4];
                    try {
                        if (obj instanceof java.sql.Date) {
                            timestampUtils.toBinDate(null, val, (Date) obj);
                        } else if (obj instanceof java.util.Date) {
                            Date tmpd = new java.sql.Date(((java.util.Date) obj).getTime());
                            timestampUtils.toBinDate(null, val, tmpd);
                        } else if (obj instanceof String) {
                            timestampUtils.toBinDate(
                                    null, val, timestampUtils.toDate(null, (String) obj));
                        } else {
                            throw new IOException(
                                    "unsupported class for date : " + obj.getClass().getName());
                        }
                        int integer =
                                ((int) (val[0] & 0xFF) << 24)
                                        | ((int) (val[1] & 0xFF) << 16)
                                        | ((int) (val[2] & 0xFF) << 8)
                                        | (int) (val[3] & 0xFF);
                        //// 10957 is from 1970-01-01 to 2000-01-01,
                        // pg的epoch起始时间是2000-01-01零点,java的起始时间是1970-01-01零点
                        integer += 10957;
                        writer.writeInt(dstIndex, integer);
                    } catch (SQLException e) {
                        throw new IOException(e);
                    }
                    break;
                }
            case Types.TIMESTAMP:
                if (column.getTypeName().equals("timestamptz")) {
                    writer.writeLong(dstIndex, TimestampUtil.timestampToMillisecond(obj, typeName));
                } else if (column.getTypeName().equals("timestamp")) {
                    writer.writeLong(
                            dstIndex, TimestampUtil.timestampToMicroSecond(obj, typeName, false));
                } else {
                    throw new IOException("unsupported type:" + typeName);
                }
                break;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                {
                    long tVal;
                    int timezone;
                    if (obj instanceof java.sql.Time) {
                        java.sql.Time timeObj = (java.sql.Time) obj;
                        LocalTime localTime = timeObj.toLocalTime();
                        long nanos = (timeObj.getTime() % 1000) * 1000000L;
                        localTime = localTime.plusNanos(nanos);
                        tVal = localTime.toNanoOfDay() / 1000;
                        timezone = timeObj.getTimezoneOffset() * 60;
                    } else if (obj instanceof java.time.LocalTime) {
                        java.time.LocalTime localTime = (java.time.LocalTime) obj;
                        tVal = localTime.toNanoOfDay() / 1000;
                        timezone = TimeZone.getDefault().getRawOffset() / -1000;
                    } else {
                        throw new IOException(
                                "unsupported class for time : " + obj.getClass().getName());
                    }
                    if (column.getTypeName().equals("time")) {
                        writer.writeLong(dstIndex, tVal);
                    } else {
                        byte[] val = new byte[16];
                        // 将tval和timezone转为小端序
                        for (int i = 0; i < 8; i++) {
                            val[7 - i] = (byte) ((tVal >> (56 - i * 8)) & 0xFF);
                        }
                        for (int i = 8; i < 12; i++) {
                            val[11 - i + 8] = (byte) ((timezone >> (24 - (i - 8) * 8)) & 0xFF);
                        }
                        // timetz 是个成员变量加起来长度为12字节的c struct结构， 在c中会8字节对齐到16字节，因此在这里进行zero padding.
                        for (int i = 12; i < 16; i++) {
                            val[i] = 0x00;
                        }
                        writer.writeByteArray(dstIndex, val);
                    }
                }
                break;
            case Types.BINARY:
                if (obj instanceof byte[]) {
                    byte[] binary = (byte[]) obj;
                    writer.writeByteArray(dstIndex, binary);
                }
                break;
            case Types.NUMERIC:
                {
                    BigDecimal decimal;
                    if (obj instanceof String) {
                        decimal = new BigDecimal((String) obj);
                    } else if (obj instanceof BigDecimal) {
                        decimal = (BigDecimal) obj;
                    } else if (obj instanceof Integer) {
                        decimal = new BigDecimal((Integer) obj);
                    } else if (obj instanceof Long) {
                        decimal = new BigDecimal((Long) obj);
                    } else {
                        throw new RuntimeException(
                                "unsupported type for numeric " + obj.getClass().getName());
                    }
                    BigInteger rescaled =
                            decimal.setScale(column.getScale(), RoundingMode.HALF_UP)
                                    .unscaledValue();
                    byte[] tmp = rescaled.toByteArray();
                    byte[] results = new byte[16];
                    int len = tmp.length;
                    if (len == 0) {
                        writer.setNullAt(dstIndex);
                        return;
                    }
                    if (len > 16) {
                        throw new NumberFormatException(decimal + " is Too Large to Store!");
                    }
                    byte sign = (tmp[0] < 0) ? (byte) 0xFF : (byte) 0x00;
                    for (int i = 0; i < len; i++) {
                        results[i] = tmp[len - i - 1];
                    }
                    for (int i = len; i < 16; i++) {
                        results[i] = sign;
                    }
                    writer.writeByteArray(dstIndex, results);
                }
                break;

            case Types.OTHER:
                if ("jsonb".equals(typeName)) {
                    throw new IOException("unsupported type:" + typeName + "(" + type + ")");
                } else if ("json".equals(typeName)) {
                    byte[] jsonBytes = String.valueOf(obj).getBytes(UTF8);
                    writer.writeByteArray(dstIndex, jsonBytes);
                    break;
                } else if ("roaringbitmap".equals(typeName)) {
                    if (obj instanceof byte[]) {
                        byte[] rbBytes = (byte[]) obj;
                        writer.writeByteArray(dstIndex, rbBytes);
                    } else {
                        throw new RuntimeException(
                                "unsupported type for roaringbitmap " + obj.getClass().getName());
                    }
                    break;
                } else {
                    throw new IOException("unsupported type:" + typeName + "(" + type + ")");
                }
            case Types.ARRAY:
                switch (typeName) {
                    case "_bool":
                        {
                            Boolean[] value = null;
                            if (obj instanceof boolean[]) {
                                value = ArrayUtils.toObject((boolean[]) obj);
                            } else if (obj instanceof Boolean[]) {
                                value = (Boolean[]) obj;
                            } else {
                                throw new IOException(
                                        "unsupported class for _bool : "
                                                + obj.getClass().getName());
                            }
                            BinaryArray binaryArray = new BinaryArray();
                            BinaryArrayWriter binaryArrayWriter =
                                    new BinaryArrayWriter(binaryArray, value.length, 1);
                            for (int i = 0; i < value.length; i++) {
                                if (value[i] == null) {
                                    binaryArrayWriter.setNullBoolean(i);
                                } else {
                                    binaryArrayWriter.writeBoolean(i, value[i]);
                                }
                            }
                            binaryArrayWriter.complete();
                            writer.writeBinaryArray(dstIndex, binaryArray);
                        }
                        break;
                    case "_int4":
                        {
                            Integer[] value = null;
                            if (obj instanceof int[]) {
                                value = ArrayUtils.toObject((int[]) obj);
                            } else if (obj instanceof Integer[]) {
                                value = (Integer[]) obj;
                            } else {
                                throw new IOException(
                                        "unsupported class for _int4 : "
                                                + obj.getClass().getName());
                            }
                            BinaryArray binaryArray = new BinaryArray();
                            BinaryArrayWriter binaryArrayWriter =
                                    new BinaryArrayWriter(binaryArray, value.length, 4);
                            for (int i = 0; i < value.length; i++) {
                                if (value[i] == null) {
                                    binaryArrayWriter.setNullInt(i);
                                } else {
                                    binaryArrayWriter.writeInt(i, value[i]);
                                }
                            }
                            binaryArrayWriter.complete();
                            writer.writeBinaryArray(dstIndex, binaryArray);
                        }
                        break;
                    case "_int8":
                        {
                            Long[] value = null;
                            if (obj instanceof long[]) {
                                value = ArrayUtils.toObject((long[]) obj);
                            } else if (obj instanceof Long[]) {
                                value = (Long[]) obj;
                            } else {
                                throw new IOException(
                                        "unsupported class for _int8 : "
                                                + obj.getClass().getName());
                            }
                            BinaryArray binaryArray = new BinaryArray();
                            BinaryArrayWriter binaryArrayWriter =
                                    new BinaryArrayWriter(binaryArray, value.length, 8);
                            for (int i = 0; i < value.length; i++) {
                                if (value[i] == null) {
                                    binaryArrayWriter.setNullLong(i);
                                } else {
                                    binaryArrayWriter.writeLong(i, value[i]);
                                }
                            }
                            binaryArrayWriter.complete();
                            writer.writeBinaryArray(dstIndex, binaryArray);
                        }
                        break;
                    case "_float4":
                        {
                            Float[] value = null;
                            if (obj instanceof float[]) {
                                value = ArrayUtils.toObject((float[]) obj);
                            } else if (obj instanceof Float[]) {
                                value = (Float[]) obj;
                            } else {
                                throw new IOException(
                                        "unsupported class for _float4 : "
                                                + obj.getClass().getName());
                            }
                            BinaryArray binaryArray = new BinaryArray();
                            BinaryArrayWriter binaryArrayWriter =
                                    new BinaryArrayWriter(binaryArray, value.length, 4);
                            for (int i = 0; i < value.length; i++) {
                                if (value[i] == null) {
                                    binaryArrayWriter.setNullFloat(i);
                                } else {
                                    binaryArrayWriter.writeFloat(i, value[i]);
                                }
                            }
                            binaryArrayWriter.complete();
                            writer.writeBinaryArray(dstIndex, binaryArray);
                            break;
                        }
                    case "_float8":
                        {
                            Double[] value = null;
                            if (obj instanceof double[]) {
                                value = ArrayUtils.toObject((double[]) obj);
                            } else if (obj instanceof Double[]) {
                                value = (Double[]) obj;
                            } else {
                                throw new IOException(
                                        "unsupported class for _float8 : "
                                                + obj.getClass().getName());
                            }
                            BinaryArray binaryArray = new BinaryArray();
                            BinaryArrayWriter binaryArrayWriter =
                                    new BinaryArrayWriter(binaryArray, value.length, 8);
                            for (int i = 0; i < value.length; i++) {
                                if (value[i] == null) {
                                    binaryArrayWriter.setNullDouble(i);
                                } else {
                                    binaryArrayWriter.writeDouble(i, value[i]);
                                }
                            }
                            binaryArrayWriter.complete();
                            writer.writeBinaryArray(dstIndex, binaryArray);
                            break;
                        }
                    case "_text":
                    case "_varchar":
                        {
                            String[] value = null;
                            if (obj instanceof String[]) {
                                value = (String[]) obj;
                            } else {
                                throw new IOException(
                                        "unsupported class for _text : "
                                                + obj.getClass().getName());
                            }
                            BinaryArray binaryArray = new BinaryArray();
                            BinaryArrayWriter binaryArrayWriter =
                                    new BinaryArrayWriter(binaryArray, value.length, 8);
                            for (int i = 0; i < value.length; i++) {
                                if (value[i] == null) {
                                    binaryArrayWriter.setNull(i);
                                } else {
                                    binaryArrayWriter.writeString(i, value[i]);
                                }
                            }
                            binaryArrayWriter.complete();
                            writer.writeBinaryArray(dstIndex, binaryArray);
                            break;
                        }
                    default:
                        throw new IOException(
                                "unsupported array type:" + typeName + "(" + type + ")");
                }
                break;
            default:
                throw new IOException("unsupported type:" + typeName + "(" + type + ")");
        }
    }
}
