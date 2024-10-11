package com.alibaba.ververica.connectors.hologres.jdbc;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import com.alibaba.hologres.client.CheckAndPut;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.Put.MutationType;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.RecordKey;
import com.alibaba.hologres.client.model.checkandput.CheckAndPutRecord;
import com.alibaba.ververica.connectors.hologres.api.HologresRecordConverter;
import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.api.HologresWriter;
import com.alibaba.ververica.connectors.hologres.api.table.HologresRowDataConverter;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.utils.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** An IO writer implementation for JDBC. */
public class HologresJDBCWriter<T> extends HologresWriter<T> {
    private static final transient Logger LOG = LoggerFactory.getLogger(HologresJDBCWriter.class);

    private transient HologresJDBCClientProvider clientProvider;
    private final HologresRecordConverter<T, Record> recordConverter;
    private final Map<RecordKey, Record> holdOnUpdateBeforeRecords;

    @Deprecated
    public HologresJDBCWriter(
            HologresConnectionParam param,
            TableSchema tableSchema,
            HologresRecordConverter<T, Record> converter) {
        super(param, tableSchema.getFieldNames(), SchemaUtil.getLogicalTypes(tableSchema));
        this.recordConverter = converter;
        this.holdOnUpdateBeforeRecords = new HashMap<>();
    }

    @Deprecated
    public static HologresJDBCWriter<RowData> createTableWriter(
            HologresConnectionParam param,
            TableSchema tableSchema,
            HologresTableSchema hologresTableSchema) {
        return new HologresJDBCWriter<RowData>(
                param,
                tableSchema,
                new HologresRowDataConverter<Record>(
                        tableSchema.getFieldNames(),
                        SchemaUtil.getLogicalTypes(tableSchema),
                        param,
                        new HologresJDBCRecordWriter(param),
                        new HologresJDBCRecordReader(
                                tableSchema.getFieldNames(), hologresTableSchema),
                        hologresTableSchema));
    }

    public HologresJDBCWriter(
            HologresConnectionParam param,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            HologresRecordConverter<T, Record> converter) {
        super(param, fieldNames, fieldTypes);
        this.recordConverter = converter;
        this.holdOnUpdateBeforeRecords = new HashMap<>();
    }

    public static HologresJDBCWriter<RowData> createTableWriter(
            HologresConnectionParam param,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            HologresTableSchema hologresTableSchema) {
        return new HologresJDBCWriter<RowData>(
                param,
                fieldNames,
                fieldTypes,
                new HologresRowDataConverter<Record>(
                        fieldNames,
                        fieldTypes,
                        param,
                        new HologresJDBCRecordWriter(param),
                        new HologresJDBCRecordReader(fieldNames, hologresTableSchema),
                        hologresTableSchema));
    }

    @Override
    public void open(Integer subtaskIdx, Integer numParallelSubtasks) throws IOException {
        LOG.info(
                "Initiating connection to database [{}] / table[{}], whole connection params: {}",
                param.getJdbcOptions().getDatabase(),
                param.getTable(),
                param);
        this.clientProvider = new HologresJDBCClientProvider(param);
        LOG.info(
                "Successfully initiated connection to database [{}] / table[{}]",
                param.getJdbcOptions().getDatabase(),
                param.getTable());
    }

    @Override
    public long writeAddRecord(T record) throws IOException {
        Record jdbcRecord = recordConverter.convertFrom(record);
        try {
            if (param.isEnableHoldOnUpdateBefore()) {
                RecordKey key = new RecordKey(jdbcRecord);
                if (holdOnUpdateBeforeRecords.containsKey(key)) {
                    clientProvider.getClient().flush();
                    Record ubRecord = holdOnUpdateBeforeRecords.remove(key);
                    putRecord(ubRecord);
                    putRecord(jdbcRecord);
                    clientProvider.getClient().flush();
                    // 将update before和update after同时写入
                    LOG.debug(
                            "Write update before record {} and update after record {} at same batch",
                            ubRecord,
                            jdbcRecord);
                    return ubRecord.getByteSize() + jdbcRecord.getByteSize();
                }
            }
            putRecord(jdbcRecord);
            return jdbcRecord.getByteSize();
        } catch (HoloClientException e) {
            throw new IOException(e);
        }
    }

    @Override
    public long writeDeleteRecord(T record) throws IOException {
        Record jdbcRecord = recordConverter.convertFrom(record);
        jdbcRecord.setType(MutationType.DELETE);
        if (param.isEnableHoldOnUpdateBefore()) {
            RecordKey key = new RecordKey(jdbcRecord);
            holdOnUpdateBeforeRecords.put(key, jdbcRecord);
            LOG.debug("Hold on update before record: {}", jdbcRecord);
            return 0;
        }
        try {
            putRecord(jdbcRecord);
        } catch (HoloClientException e) {
            throw new IOException(e);
        }
        return jdbcRecord.getByteSize();
    }

    private void putRecord(Record jdbcRecord) throws HoloClientException {
        if (param.getCheckAndPutCondition() != null) {
            CheckAndPutRecord checkAndPutRecord = (CheckAndPutRecord) jdbcRecord;
            clientProvider.getClient().checkAndPut(new CheckAndPut(checkAndPutRecord));
        } else {
            clientProvider.getClient().put(new Put(jdbcRecord));
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            if (!holdOnUpdateBeforeRecords.isEmpty()) {
                LOG.info("Flushing {} hold on delete records", holdOnUpdateBeforeRecords.size());
                for (Record record : holdOnUpdateBeforeRecords.values()) {
                    putRecord(record);
                }
            }
            holdOnUpdateBeforeRecords.clear();
            clientProvider.getClient().flush();
        } catch (HoloClientException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            flush();
        } finally {
            if (null != clientProvider.getClient()) {
                clientProvider.getClient().close();
            }
        }
    }
}
