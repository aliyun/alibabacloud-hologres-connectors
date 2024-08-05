package com.alibaba.hologres.kafka.sink;

import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.kafka.conf.HoloSinkConfigManager;
import com.alibaba.hologres.kafka.exception.KafkaHoloException;
import com.alibaba.hologres.kafka.model.DirtyDataStrategy;
import com.alibaba.hologres.kafka.model.InputFormat;
import com.alibaba.hologres.kafka.sink.writer.AbstractHoloWriter;
import com.alibaba.hologres.kafka.sink.writer.HoloCopyWriter;
import com.alibaba.hologres.kafka.sink.writer.HoloSqlWriter;
import com.alibaba.hologres.kafka.utils.DirtyDataUtils;
import com.alibaba.hologres.kafka.utils.MessageInfo;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.security.InvalidParameterException;
import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** HoloSinkWriter. convert and write record to holo. */
public class HoloSinkWriter {
    private static final Logger logger = LoggerFactory.getLogger(HoloSinkWriter.class);
    private final AbstractHoloWriter holoWriter;
    private final boolean schemaForceCheck;
    private final MessageInfo messageInfo;
    private final InputFormat inputFormat;
    private final long initialTimestamp;
    private final DirtyDataUtils dirtyDataUtils;
    private final TableSchema schema;

    public HoloSinkWriter(
            HoloSinkConfigManager configManager, TableSchema tableSchema, boolean copyWriteMode)
            throws HoloClientException {
        if (copyWriteMode) {
            holoWriter = new HoloCopyWriter(configManager);
        } else {
            holoWriter = new HoloSqlWriter(configManager);
        }
        this.schemaForceCheck = configManager.isSchemaForceCheck();
        this.messageInfo = configManager.getMessageInfo();
        this.inputFormat = configManager.getInputFormat();
        this.initialTimestamp = configManager.getInitialTimestamp();
        this.dirtyDataUtils = configManager.getDirtyDataUtils();
        this.schema = tableSchema;
    }

    public void write(Collection<SinkRecord> records)
            throws HoloClientException, KafkaHoloException {
        for (SinkRecord record : records) {
            if (record.timestamp() < initialTimestamp) {
                continue;
            }
            try {
                if (inputFormat == InputFormat.STRUCT_JSON) {
                    writeStructJson(record);
                } else if (inputFormat == InputFormat.STRING) {
                    writeString(record);
                } else {
                    writeJson(record);
                }
            } catch (InvalidParameterException | NullPointerException e) {
                if (dirtyDataUtils.getDirtyDataStrategy().equals(DirtyDataStrategy.SKIP)) {
                    logger.warn("Skip Dirty Data: " + record);
                } else if (dirtyDataUtils
                        .getDirtyDataStrategy()
                        .equals(DirtyDataStrategy.SKIP_ONCE)) {
                    List<String> dirtyDataToSkipOnce = dirtyDataUtils.getDirtyDataToSkipOnce();
                    if (!(Objects.equals(dirtyDataToSkipOnce.get(0), record.topic())
                            && Objects.equals(
                                    dirtyDataToSkipOnce.get(1), record.kafkaPartition().toString())
                            && Objects.equals(
                                    dirtyDataToSkipOnce.get(2),
                                    Long.toString(record.kafkaOffset())))) {
                        throw new KafkaHoloException(makeSkipProperties(record), e);
                    }
                    logger.warn("Skip(once) Dirty Data: " + record);
                } else {
                    throw new KafkaHoloException(makeSkipProperties(record), e);
                }
            }
        }
    }

    public void flush() {
        holoWriter.flush();
    }

    public void close() {
        if (holoWriter != null) {
            holoWriter.flush();
            holoWriter.close();
        }
    }

    /** InputFormat is StructJSON. */
    private void writeStructJson(SinkRecord record) throws KafkaHoloException {
        Put put = new Put(schema);
        for (Field field : record.valueSchema().fields()) {
            String fieldName = field.name();
            Schema.Type fieldType = field.schema().type();
            Object fieldValue;
            if (field.schema().name() != null) {
                switch (field.schema().name()) {
                        // 前三个case使用kafka.connect.data类型，对输入格式要求较为严格
                    case Decimal.LOGICAL_NAME:
                        fieldValue = (BigDecimal) ((Struct) record.value()).get(fieldName);
                        break;
                    case Date.LOGICAL_NAME:
                        java.util.Date dateValue =
                                (java.util.Date) ((Struct) record.value()).get(fieldName);
                        fieldValue = new java.sql.Date(dateValue.getTime());
                        break;
                    case Timestamp.LOGICAL_NAME:
                        java.util.Date timestampValue =
                                (java.util.Date) ((Struct) record.value()).get(fieldName);
                        fieldValue = new java.sql.Timestamp(timestampValue.getTime());
                        break;
                        // 以下三个case使用string类型读入并写入holo，可读性高
                    case "Decimal":
                        fieldValue = new BigDecimal(((Struct) record.value()).getString(fieldName));
                        break;
                    case "Date":
                        fieldValue =
                                java.sql.Date.valueOf(
                                        ((Struct) record.value()).getString(fieldName));
                        break;
                    case "Timestamp":
                        fieldValue =
                                java.sql.Timestamp.valueOf(
                                        ((Struct) record.value()).getString(fieldName));
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "not support type name " + field.schema().name());
                }
            } else {
                switch (fieldType) {
                    case INT8:
                        fieldValue = ((Struct) record.value()).getInt8(fieldName);
                        break;
                    case INT16:
                        fieldValue = ((Struct) record.value()).getInt16(fieldName);
                        break;
                    case INT32:
                        fieldValue = ((Struct) record.value()).getInt32(fieldName);
                        break;
                    case INT64:
                        fieldValue = ((Struct) record.value()).getInt64(fieldName);
                        break;
                    case FLOAT32:
                        fieldValue = ((Struct) record.value()).getFloat32(fieldName);
                        break;
                    case FLOAT64:
                        fieldValue = ((Struct) record.value()).getFloat64(fieldName);
                        break;
                    case BOOLEAN:
                        fieldValue = ((Struct) record.value()).getBoolean(fieldName);
                        break;
                    case STRING:
                        fieldValue = ((Struct) record.value()).getString(fieldName);
                        break;
                    case BYTES:
                        fieldValue = ((Struct) record.value()).getBytes(fieldName);
                        break;
                    case ARRAY:
                        fieldValue = ((Struct) record.value()).getArray(fieldName);
                        break;
                    default:
                        throw new IllegalArgumentException("not support type " + fieldType.name());
                }
            }
            try {
                put.setObject(fieldName, fieldValue);
            } catch (InvalidParameterException e) {
                if (e.getMessage().contains("can not found column") && !schemaForceCheck) {
                    logger.warn(
                            "field {} not exists in holo table {} but we ignore it because schemaForceCheck is false",
                            fieldName,
                            schema.getTableName());
                } else {
                    throw new KafkaHoloException(
                            String.format(
                                    "hologres table %s not have column named %s",
                                    schema.getTableName(), fieldName));
                }
            }
            putMessageInfo(record, put);
        }
        holoWriter.write(put);
    }

    /** InputFormat is JSON. */
    private void writeJson(SinkRecord record) throws KafkaHoloException {
        Put put = new Put(schema);
        HashMap<String, Object> jsonMap = (HashMap<String, Object>) record.value();
        for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
            Object value;

            Column holoColumn;
            try {
                holoColumn = schema.getColumn(schema.getColumnIndex(entry.getKey()));
            } catch (NullPointerException e) {
                if (!schemaForceCheck) {
                    logger.warn(
                            "field {} not exists in holo table {} but we ignore it because schemaForceCheck is false",
                            entry.getKey(),
                            schema.getTableName());
                    continue;
                } else {
                    throw new KafkaHoloException(
                            String.format(
                                    "hologres table %s not have column named %s",
                                    schema.getTableName(), entry.getKey()));
                }
            }
            switch (holoColumn.getType()) {
                case Types.CHAR:
                case Types.VARCHAR:
                    value = entry.getValue().toString();
                    break;
                case Types.BIT:
                case Types.BOOLEAN:
                    value = Boolean.valueOf(entry.getValue().toString());
                    break;
                case Types.NUMERIC:
                case Types.DECIMAL:
                    value = new BigDecimal(entry.getValue().toString());
                    break;
                case Types.SMALLINT:
                    value = (short) entry.getValue();
                    break;
                case Types.INTEGER:
                    value = Integer.valueOf(entry.getValue().toString());
                    break;
                case Types.BIGINT:
                    value = (long) entry.getValue();
                    break;
                case Types.REAL:
                case Types.FLOAT:
                    value = (float) entry.getValue();
                    break;
                case Types.DOUBLE:
                    value = (double) entry.getValue();
                    break;
                case Types.DATE:
                    if (entry.getValue() instanceof Long) {
                        value = new java.sql.Date((Long) entry.getValue());
                    } else {
                        value = java.sql.Date.valueOf(entry.getValue().toString());
                    }
                    break;
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    if (entry.getValue() instanceof Long) {
                        value = new java.sql.Timestamp((Long) entry.getValue());
                    } else {
                        value = java.sql.Timestamp.valueOf(entry.getValue().toString());
                    }
                    break;
                default:
                    throw new IllegalArgumentException(
                            "not support hologres type " + holoColumn.getTypeName());
            }
            put.setObject(entry.getKey(), value);

            putMessageInfo(record, put);
        }
        holoWriter.write(put);
    }

    /** InputFormat is STRING. */
    private void writeString(SinkRecord record) throws KafkaHoloException {
        Put put = new Put(schema);
        String key = record.key().toString();
        String value = record.value().toString();

        put.setObject("key", key);
        put.setObject("value", value);
        putMessageInfo(record, put);

        holoWriter.write(put);
    }

    private void putMessageInfo(SinkRecord record, Put put) {
        if (messageInfo.wholeMessageInfo) {
            put.setObject(messageInfo.messageTopic, record.topic());
            put.setObject(messageInfo.messagePartition, record.kafkaPartition());
            put.setObject(messageInfo.messageOffset, record.kafkaOffset());
            put.setObject(messageInfo.messageTimestamp, new java.sql.Timestamp(record.timestamp()));
        }
    }

    private String makeSkipProperties(SinkRecord record) {
        return String.format(
                "\nIf you want to skip this dirty data, please add "
                        + "< dirty_data_strategy=SKIP_ONCE > and < dirty_data_to_skip_once=%s,%s,%s > in holo-sink.properties; "
                        + "or add < dirty_data_strategy=SKIP > to skip all dirty data(not recommended).",
                record.topic(), record.kafkaPartition(), record.kafkaOffset());
    }
}
