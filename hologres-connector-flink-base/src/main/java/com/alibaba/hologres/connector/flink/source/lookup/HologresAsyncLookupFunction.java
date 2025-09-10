package com.alibaba.hologres.connector.flink.source.lookup;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import com.alibaba.hologres.connector.flink.api.HologresReader;
import com.alibaba.hologres.connector.flink.api.HologresTableSchema;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.jdbc.HologresJDBCReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/** Hologres row fetcher based on LRU Cache with async mode. */
public class HologresAsyncLookupFunction extends AsyncLookupFunction {
    private static final Logger LOG = LoggerFactory.getLogger(HologresAsyncLookupFunction.class);
    private final String sqlTableName;

    private final String[] fieldNames;
    private final LogicalType[] fieldTypes;
    private final String[] lookupKeys;
    private final boolean hasPrimaryKey;
    private final HologresConnectionParam connectionParam;
    private final HologresTableSchema hologresTableSchema;

    private HologresReader<RowData> hologresReader;

    public HologresAsyncLookupFunction(
            String sqlTableName,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            String[] lookupKeys,
            HologresConnectionParam connectionParam,
            HologresTableSchema hologresTableSchema,
            boolean hasPrimaryKey) {
        this.sqlTableName = sqlTableName;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.lookupKeys = lookupKeys;
        this.hasPrimaryKey = hasPrimaryKey;
        this.hologresTableSchema = hologresTableSchema;
        this.connectionParam = connectionParam;
    }

    @Override
    public void open(FunctionContext context) throws IOException {
        LOG.info("start open lookup function of table {} ...", sqlTableName);
        this.hologresReader =
                HologresJDBCReader.createTableReader(
                        connectionParam, fieldNames, fieldTypes, lookupKeys, hologresTableSchema);
        this.hologresReader.open(null, null);
        LOG.info("end open lookup function of table {}.", sqlTableName);
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData rowData) {
        if (hasPrimaryKey) {
            try {
                return hologresReader
                        .asyncGet(rowData)
                        .thenApply(
                                r -> r == null ? Collections.emptyList() : Lists.newArrayList(r));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            try {
                return hologresReader
                        .asyncGetMany(rowData)
                        .thenApply(
                                r -> r == null ? Collections.emptyList() : Lists.newArrayList(r));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        LOG.info("start close ...");
        if (hologresReader != null) {
            hologresReader.close();
        }
        LOG.info("end close.");
    }
}
