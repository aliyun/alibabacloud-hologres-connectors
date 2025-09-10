package com.alibaba.hologres.connector.flink.source.lookup;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.logical.LogicalType;

import com.alibaba.hologres.connector.flink.api.HologresReader;
import com.alibaba.hologres.connector.flink.api.HologresTableSchema;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.jdbc.HologresJDBCReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** Hologres row fetcher based on LRU Cache with sync mode. */
public class HologresLookupFunction extends LookupFunction {
    private static final long serialVersionUID = 5111593589582551115L;
    private static final Logger LOG = LoggerFactory.getLogger(HologresLookupFunction.class);

    private final String sqlTableName;
    private final String[] fieldNames;
    private final LogicalType[] fieldTypes;
    private final String[] lookupKeys;
    private final boolean hasPrimaryKey;
    private final HologresConnectionParam connectionParam;
    private final HologresTableSchema hologresTableSchema;

    private HologresReader<RowData> hologresReader;

    public HologresLookupFunction(
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
    public Collection<RowData> lookup(RowData rowData) throws IOException {
        if (hasPrimaryKey) {
            RowData result = hologresReader.get(rowData);
            if (result != null) {
                return Collections.singletonList(result);
            } else {
                return Collections.emptyList();
            }
        } else {
            List<RowData> result = hologresReader.getMany(rowData);
            if (result != null && !result.isEmpty()) {
                return result;
            } else {
                return Collections.emptyList();
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
