package com.alibaba.ververica.connectors.hologres.jdbc;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;

import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.ververica.connectors.hologres.api.HologresRecordConverter;
import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.api.HologresWriter;
import com.alibaba.ververica.connectors.hologres.api.table.HologresRowDataConverter;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import org.postgresql.core.SqlCommandType;

import java.io.IOException;

/** An IO writer implementation for JDBC. */
public class HologresJDBCWriter<T> extends HologresWriter<T> {
    private transient HologresJDBCClientProvider clientProvider;
    private transient HologresTableSchema schema;
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
        try {
            this.clientProvider = new HologresJDBCClientProvider(param);
            schema =
                    new HologresTableSchema(
                            clientProvider.getClient().getTableSchema(param.getTable()));
        } catch (HoloClientException e) {
            throw new IOException(e);
        }
    }

    @Override
    public long writeAddRecord(T record) throws IOException {
        Record jdbcRecord = (Record) recordConverter.convertFrom(record);
        jdbcRecord.setType(SqlCommandType.INSERT);
        try {
            clientProvider.getClient().put(new Put(jdbcRecord));
        } catch (HoloClientException e) {
            throw new IOException(e);
        }
        return jdbcRecord.getByteSize();
    }

    @Override
    public long writeDeleteRecord(T record) throws IOException {
        Record jdbcRecord = recordConverter.convertFrom(record);
        jdbcRecord.setType(SqlCommandType.DELETE);
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
