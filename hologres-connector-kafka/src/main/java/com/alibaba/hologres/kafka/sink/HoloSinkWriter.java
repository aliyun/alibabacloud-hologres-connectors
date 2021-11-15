package com.alibaba.hologres.kafka.sink;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.kafka.exception.KafkaHoloException;
import com.alibaba.hologres.kafka.model.DirtyDataStrategy;
import com.alibaba.hologres.kafka.model.InputFormat;
import com.alibaba.hologres.kafka.utils.DirtyDataUtils;
import com.alibaba.hologres.kafka.utils.MessageInfo;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.postgresql.model.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.security.InvalidParameterException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** HoloSinkWriter. convert and write record to holo. */
public class HoloSinkWriter {
    private static final Logger logger = LoggerFactory.getLogger(HoloSinkWriter.class);
    private final HoloClient client;
    private final MessageInfo messageInfo;
    private final InputFormat inputFormat;
    private final long initialTimestamp;
    private final DirtyDataUtils dirtyDataUtils;
    private final TableSchema schema;

    public HoloSinkWriter(
            HoloClient client,
            String tableName,
            MessageInfo messageInfo,
            InputFormat inputFormat,
            long initialTimestamp,
            DirtyDataUtils dirtyDataUtils)
            throws HoloClientException {
        this.client = client;
        this.messageInfo = messageInfo;
        this.inputFormat = inputFormat;
        this.initialTimestamp = initialTimestamp;
        this.dirtyDataUtils = dirtyDataUtils;
        this.schema = client.getTableSchema(tableName);
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

    /** InputFormat is StructJSON. */
    public void writeStructJson(SinkRecord record) throws KafkaHoloException {
        Put put = new Put(schema);
        try {
            for (Field field : record.valueSchema().fields()) {
                String fieldName = field.name();
                Schema.Type fieldType = field.schema().type();
                Object fieldValue = new Object();
                if (field.schema().name() != null) {
                    switch (field.schema().name()) {
                            // 前三个case使用kafka.connect.data类型，对输入格式要求较为严格
                        case Decimal.LOGICAL_NAME:
                            fieldValue = ((Struct) record.value()).get(fieldName);
                            break;
                        case Date.LOGICAL_NAME:
                            fieldValue = ((Struct) record.value()).get(fieldName);
                            break;
                        case Timestamp.LOGICAL_NAME:
                            fieldValue = ((Struct) record.value()).get(fieldName);
                            break;
                            // 以下三个case使用string类型读入并写入holo，可读性高
                        case "Decimal":
                            fieldValue =
                                    new BigDecimal(((Struct) record.value()).getString(fieldName));
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
                            // fall through to normal types
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
                    }
                }
                put.setObject(fieldName, fieldValue);
                putMessageInfo(record, put);
            }
            client.put(put);
        } catch (HoloClientException e) {
            throw new KafkaHoloException(e);
        }
    }

    /** InputFormat is JSON. */
    public void writeJson(SinkRecord record) throws KafkaHoloException {
        Put put = new Put(schema);
        HashMap<String, Object> jsonMap = (HashMap<String, Object>) record.value();
        try {
            for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
                put.setObject(entry.getKey(), entry.getValue());
                putMessageInfo(record, put);
            }
            client.put(put);
        } catch (HoloClientException e) {
            throw new KafkaHoloException(e);
        }
    }

    /** InputFormat is STRING. */
    public void writeString(SinkRecord record) throws KafkaHoloException {
        Put put = new Put(schema);
        try {
            String key = record.key().toString();
            String value = record.value().toString();

            put.setObject("key", key);
            put.setObject("value", value);
            putMessageInfo(record, put);

            client.put(put);
        } catch (HoloClientException e) {
            throw new KafkaHoloException(e);
        }
    }

    public void putMessageInfo(SinkRecord record, Put put) {
        if (messageInfo.wholeMessageInfo) {
            put.setObject(messageInfo.messageTopic, record.topic());
            put.setObject(messageInfo.messagePartition, record.kafkaPartition());
            put.setObject(messageInfo.messageOffset, record.kafkaOffset());
            put.setObject(messageInfo.messageTimestamp, record.timestamp());
        }
    }

    public String makeSkipProperties(SinkRecord record) {
        return String.format(
                "\nIf you want to skip this dirty data, please add "
                        + "< dirty_date_strategy=SKIP_ONCE > and < dirty_date_to_skip_once=%s,%s,%s > in holo-sink.properties; "
                        + "or add < dirty_date_strategy=SKIP > to skip all dirty data(not recommended).",
                record.topic(), record.kafkaPartition(), record.kafkaOffset());
    }
}
