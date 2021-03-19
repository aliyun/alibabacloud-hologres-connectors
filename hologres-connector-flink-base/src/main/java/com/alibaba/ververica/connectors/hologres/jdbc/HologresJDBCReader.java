package com.alibaba.ververica.connectors.hologres.jdbc;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;

import com.alibaba.hologres.client.Get;
import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.Scan;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.ververica.connectors.hologres.api.HologresReader;
import com.alibaba.ververica.connectors.hologres.api.HologresRecordConverter;
import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.api.table.HologresRowDataConverter;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.config.JDBCOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/** An IO reader implementation for JDBC. */
public class HologresJDBCReader<T> extends HologresReader<T> {
    private static final transient Logger LOG = LoggerFactory.getLogger(HologresJDBCReader.class);
    private final HologresRecordConverter<T, Record> recordConverter;
    protected transient HoloClient holoClient;
    protected final boolean insertIfNotExists;
    private transient HologresTableSchema hologresTableSchema;

    public HologresJDBCReader(
            String[] primaryKeys,
            HologresConnectionParam param,
            TableSchema tableSchema,
            HologresRecordConverter<T, Record> recordConverter) {
        super(param, tableSchema, primaryKeys);
        this.recordConverter = recordConverter;
        this.insertIfNotExists = param.isInsertIfNotExists();
    }

    public static HologresJDBCReader<RowData> createTableReader(
            HologresConnectionParam param,
            TableSchema tableSchema,
            String[] index,
            HologresTableSchema hologresTableSchema) {
        return new HologresJDBCReader(
                index,
                param,
                tableSchema,
                new HologresRowDataConverter<Record>(
                        index,
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
                "Initiating connection to database [{}] / table[{}]",
                param.getJdbcOptions().getDatabase(),
                param.getTable());
        if (insertIfNotExists) {
            LOG.info("Hologres dim table will insert new record if primary key does not exist.");
        }
        HoloConfig holoConfig = new HoloConfig();
        JDBCOptions jdbcOptions = param.getJdbcOptions();
        holoConfig.setJdbcUrl(jdbcOptions.getDbUrl());
        holoConfig.setUsername(jdbcOptions.getUsername());
        holoConfig.setPassword(jdbcOptions.getPassword());
        holoConfig.setWriteMode(WriteMode.INSERT_OR_IGNORE);
        holoConfig.setReadThreadSize(param.getConnectionPoolSize());
        try {
            holoClient = new HoloClient(holoConfig);
            hologresTableSchema =
                    new HologresTableSchema(holoClient.getTableSchema(jdbcOptions.getTable()));
        } catch (HoloClientException e) {
            throw new RuntimeException(e);
        }
        LOG.info(
                "Successfully initiated connection to database [{}] / table[{}]",
                param.getJdbcOptions().getDatabase(),
                param.getTable());
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing HologresLookUpFunction");
        if (holoClient != null) {
            this.holoClient.close();
        }
    }

    @Override
    public CompletableFuture<T> asyncGet(T in) throws IOException {
        CompletableFuture<T> result = new CompletableFuture<>();
        Record record = recordConverter.convertToPrimaryKey(in);
        try {
            holoClient
                    .get(new Get(record.clone()))
                    .handleAsync(
                            (rowData, throwable) -> {
                                try {
                                    // caught an error
                                    if (throwable != null) {
                                        result.completeExceptionally(throwable);
                                    } else {
                                        if (rowData == null) {
                                            if (insertIfNotExists) {
                                                try {
                                                    T newRow = insertNewPrimaryKey(record);
                                                    result.complete(newRow);
                                                } catch (IOException exception) {
                                                    result.completeExceptionally(exception);
                                                }
                                            } else {
                                                result.complete(null);
                                            }
                                        } else {
                                            T resultRow = recordConverter.convertTo(rowData);
                                            result.complete(resultRow);
                                        }
                                    }
                                } catch (Throwable e) {
                                    result.completeExceptionally(e);
                                }
                                return null;
                            });
        } catch (HoloClientException e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    @Override
    public T get(T record) throws IOException {
        try {
            return this.asyncGet(record).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Override
    public CompletableFuture<List<T>> asyncGetMany(T in) throws IOException {
        CompletableFuture<List<T>> scanResult = new CompletableFuture<>();
        Record record = recordConverter.convertToPrimaryKey(in);
        Scan.Builder scanBuilder =
                new Scan.Builder(record.getSchema()).withSelectedColumns(fieldNames);
        for (String primaryKeys : primaryKeys) {
            scanBuilder.addEqualFilter(primaryKeys, record.getObject(primaryKeys));
        }
        try {
            holoClient
                    .asyncScan(scanBuilder.build())
                    .handleAsync(
                            (scanner, throwable) -> {
                                List<T> result = new ArrayList<>();
                                while (true) {
                                    try {
                                        if (!scanner.next()) {
                                            break;
                                        }
                                        Record resultRecord = scanner.getRecord();
                                        result.add(recordConverter.convertTo(resultRecord));
                                    } catch (HoloClientException e) {
                                        scanResult.completeExceptionally(e);
                                        break;
                                    }
                                }
                                scanResult.complete(result);
                                return null;
                            });
        } catch (HoloClientException e) {
            throw new IOException(e);
        }
        return scanResult;
    }

    @Override
    public List<T> getMany(T record) throws IOException {
        try {
            return asyncGetMany(record).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }

    protected T insertNewPrimaryKey(Record record) throws IOException {
        Put put = new Put(record);
        try {
            holoClient.put(put);
            holoClient.flush();
            Record rowData = holoClient.get(new Get(record)).get();
            if (rowData == null) {
                throw new IOException("Could not get value for " + record);
            }
            return recordConverter.convertTo(rowData);
        } catch (HoloClientException | InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }
}
