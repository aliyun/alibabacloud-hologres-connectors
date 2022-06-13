package com.alibaba.ververica.connectors.hologres.source;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.ScanTableSource.ScanContext;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.util.Preconditions;

import com.alibaba.ververica.connectors.common.dim.AsyncLookupFunctionWrapper;
import com.alibaba.ververica.connectors.common.dim.DimOptions;
import com.alibaba.ververica.connectors.common.dim.LookupFunctionWrapper;
import com.alibaba.ververica.connectors.common.dim.cache.CacheConfig;
import com.alibaba.ververica.connectors.hologres.api.HologresReader;
import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.config.JDBCOptions;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCReader;
import com.alibaba.ververica.connectors.hologres.source.bulkread.HologresBulkreadInputFormat;

import java.util.Arrays;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/** Table Source. */
public class HologresTableSource implements DynamicTableSource, LookupTableSource, ScanTableSource {
    private String tableName;
    private TableSchema tableSchema;
    private CacheConfig cacheConfig;
    private HologresConnectionParam connectionParam;
    private ReadableConfig config;
    private JDBCOptions jdbcOptions;

    public HologresTableSource(
            String tableName,
            TableSchema tableSchema,
            CacheConfig cacheConfig,
            HologresConnectionParam connectionParam,
            JDBCOptions jdbcOptions,
            ReadableConfig config) {
        this.tableName = tableName;
        this.tableSchema = tableSchema;
        this.cacheConfig = cacheConfig;
        this.connectionParam = connectionParam;
        this.jdbcOptions = jdbcOptions;
        this.config = config;
    }

    @Override
    public DynamicTableSource copy() {
        return new HologresTableSource(
                tableName, tableSchema, cacheConfig, connectionParam, jdbcOptions, config);
    }

    @Override
    public String asSummaryString() {
        return "Hologres-" + tableName;
    }

    private boolean primaryKeyFullProvided(
            String[] lookupKeys, HologresTableSchema hologresTableSchema) {
        String[] hologresPrimaryKeys = hologresTableSchema.get().getPrimaryKeys();
        return Arrays.stream(lookupKeys)
                .collect(Collectors.toSet())
                .equals(Arrays.stream(hologresPrimaryKeys).collect(Collectors.toSet()));
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String[] lookupKeys = new String[context.getKeys().length];
        for (int i = 0; i < lookupKeys.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "Do not support nested lookup keys");
            lookupKeys[i] = tableSchema.getFieldNames()[innerKeyArr[0]];
        }

        HologresTableSchema hologresTableSchema = HologresTableSchema.get(jdbcOptions);
        boolean hasPrimaryKey = primaryKeyFullProvided(lookupKeys, hologresTableSchema);

        HologresReader<RowData> hologresReader;
        hologresReader =
                HologresJDBCReader.createTableReader(
                        connectionParam, tableSchema, lookupKeys, hologresTableSchema);

        ServiceLoader<HologresLookUpFunctionFactory> loader =
                ServiceLoader.load(HologresLookUpFunctionFactory.class);
        HologresLookUpFunctionFactory hologresLookUpFunctionFactory = loader.iterator().next();

        if (config.get(DimOptions.OPTIONAL_ASYNC)) {
            AsyncTableFunction<RowData> lookupFunc =
                    new AsyncLookupFunctionWrapper(
                            hologresLookUpFunctionFactory.createAsyncFunction(
                                    tableName,
                                    tableSchema,
                                    lookupKeys,
                                    cacheConfig.getCacheStrategy(),
                                    hologresReader,
                                    hasPrimaryKey));
            return AsyncTableFunctionProvider.of(lookupFunc);
        } else {
            TableFunction<RowData> lookupFunc =
                    new LookupFunctionWrapper(
                            hologresLookUpFunctionFactory.createFunction(
                                    tableName,
                                    tableSchema,
                                    lookupKeys,
                                    cacheConfig.getCacheStrategy(),
                                    hologresReader,
                                    hasPrimaryKey));
            return TableFunctionProvider.of(lookupFunc);
        }
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        return InputFormatProvider.of(new HologresBulkreadInputFormat(jdbcOptions, tableSchema));
    }
}
