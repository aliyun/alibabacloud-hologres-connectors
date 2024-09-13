package com.alibaba.ververica.connectors.hologres.jdbc;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;

import com.alibaba.hologres.client.CheckAndPut;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.Put.MutationType;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.checkandput.CheckAndPutRecord;
import com.alibaba.ververica.connectors.hologres.api.HologresRecordConverter;
import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.api.HologresWriter;
import com.alibaba.ververica.connectors.hologres.api.table.HologresRowDataConverter;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** An IO writer implementation for JDBC. */
public class HologresJDBCWriter<T> extends HologresWriter<T> {
    private static final transient Logger LOG = LoggerFactory.getLogger(HologresJDBCWriter.class);

    private transient HologresJDBCClientProvider clientProvider;
    private HologresRecordConverter<T, Record> recordConverter;

    public HologresJDBCWriter(
            HologresConnectionParam param,
            TableSchema tableSchema,
            HologresRecordConverter<T, Record> converter) {
        super(param, tableSchema);
        this.recordConverter = converter;
    }

    public static HologresJDBCWriter<RowData> createTableWriter(
            HologresConnectionParam param,
            TableSchema tableSchema,
            HologresTableSchema hologresTableSchema) {
        return new HologresJDBCWriter<RowData>(
                param,
                tableSchema,
                new HologresRowDataConverter<Record>(
                        tableSchema,
                        param,
                        new HologresJDBCRecordWriter(param),
                        new HologresJDBCRecordReader(
                                tableSchema.getFieldNames(), hologresTableSchema),
                        hologresTableSchema));
    }

    @Override
    public void open(RuntimeContext runtimeContext) throws IOException {
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
            if (param.getCheckAndPutCondition() != null) {
                CheckAndPutRecord checkAndPutRecord = (CheckAndPutRecord) jdbcRecord;
                clientProvider.getClient().checkAndPut(new CheckAndPut(checkAndPutRecord));
            } else {
                clientProvider.getClient().put(new Put(jdbcRecord));
            }
        } catch (HoloClientException e) {
            throw new IOException(e);
        }
        return jdbcRecord.getByteSize();
    }

    @Override
    public long writeDeleteRecord(T record) throws IOException {
        Record jdbcRecord = recordConverter.convertFrom(record);
        jdbcRecord.setType(MutationType.DELETE);
        try {
            clientProvider.getClient().put(new Put(jdbcRecord));
        } catch (HoloClientException e) {
            throw new IOException(e);
        }
        return jdbcRecord.getByteSize();
    }

    @Override
    public void flush() throws HoloClientException {
        clientProvider.getClient().flush();
    }

    @Override
    public void close() throws HoloClientException {
        try {
            flush();
        } finally {
            if (null != clientProvider.getClient()) {
                clientProvider.getClient().close();
            }
        }
    }
}
