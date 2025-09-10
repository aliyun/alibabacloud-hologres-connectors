package com.alibaba.hologres.connector.flink.jdbc;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import com.alibaba.hologres.client.CheckAndPut;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.Put.MutationType;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.RecordKey;
import com.alibaba.hologres.client.model.checkandput.CheckAndPutRecord;
import com.alibaba.hologres.client.model.expression.RecordWithExpression;
import com.alibaba.hologres.client.utils.RecordChecker;
import com.alibaba.hologres.connector.flink.api.HologresRecordConverter;
import com.alibaba.hologres.connector.flink.api.HologresTableSchema;
import com.alibaba.hologres.connector.flink.api.HologresWriter;
import com.alibaba.hologres.connector.flink.api.table.HologresRowDataConverter;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.sink.v1.multi.InternalHologresMapRecordConverter;
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
    private boolean checkDirtyData;

    public HologresJDBCWriter(
            HologresConnectionParam param,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            HologresRecordConverter<T, Record> converter) {
        super(param, fieldNames, fieldTypes);
        this.recordConverter = converter;
        this.holdOnUpdateBeforeRecords = new HashMap<>();
        this.checkDirtyData = param.isDirtyDataCheck();
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

    public static HologresJDBCWriter<Map<String, Object>> createMapRecordWriter(
            HologresConnectionParam param, HologresTableSchema hologresTableSchema) {
        return new HologresJDBCWriter<>(
                param,
                new String[0],
                new LogicalType[0],
                new InternalHologresMapRecordConverter(
                        hologresTableSchema.get(),
                        param.isDataTypeTolerant(),
                        param.isExtraColumnTolerant()));
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
    public long writeAddRecord(T record) throws HoloClientException, IOException {
        Record jdbcRecord;
        if (param.getInsertExpression().hasExpr()) {
            jdbcRecord =
                    new RecordWithExpression.Builder(recordConverter.convertFrom(record))
                            .setConflictUpdateSet(param.getInsertExpression().conflictUpdateSet)
                            .setConflictWhere(param.getInsertExpression().conflictWhere)
                            .build();
        } else {
            jdbcRecord = recordConverter.convertFrom(record);
        }
        if (checkDirtyData) {
            try {
                RecordChecker.check(jdbcRecord);
            } catch (HoloClientException e) {
                throw new IOException(
                        String.format(
                                "failed to copy because dirty data, the error record is %s.",
                                jdbcRecord),
                        e);
            }
        }
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
    }

    @Override
    public long writeDeleteRecord(T record) throws HoloClientException, IOException {
        Record jdbcRecord = recordConverter.convertFrom(record);
        jdbcRecord.setType(MutationType.DELETE);
        if (param.isEnableHoldOnUpdateBefore()) {
            RecordKey key = new RecordKey(jdbcRecord);
            holdOnUpdateBeforeRecords.put(key, jdbcRecord);
            LOG.debug("Hold on update before record: {}", jdbcRecord);
            return 0;
        }
        putRecord(jdbcRecord);
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
    public void flush() throws HoloClientException, IOException {
        if (!holdOnUpdateBeforeRecords.isEmpty()) {
            LOG.info("Flushing {} hold on delete records", holdOnUpdateBeforeRecords.size());
            for (Record record : holdOnUpdateBeforeRecords.values()) {
                putRecord(record);
            }
        }
        holdOnUpdateBeforeRecords.clear();
        clientProvider.getClient().flush();
        checkDirtyData = false;
    }

    @Override
    public void close() throws IOException {
        if (null != clientProvider) {
            clientProvider.closeClient();
        }
    }
}
