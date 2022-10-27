package com.alibaba.ververica.connectors.hologres.source;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
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
import com.alibaba.ververica.connectors.hologres.filter.FilterVisitor;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCReader;
import com.alibaba.ververica.connectors.hologres.source.bulkread.HologresBulkreadInputFormat;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/** Table Source. */
public class HologresTableSource
        implements DynamicTableSource, LookupTableSource, ScanTableSource, SupportsFilterPushDown {
    private String tableName;
    private TableSchema tableSchema;
    private CacheConfig cacheConfig;
    private HologresConnectionParam connectionParam;
    private ReadableConfig config;
    private JDBCOptions jdbcOptions;
    private FilterVisitor filterVisitor = new FilterVisitor((name) -> name);
    private List<String> filters;

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

    public HologresTableSource(HologresTableSource copy) {
        this.tableName = copy.tableName;
        this.tableSchema = copy.tableSchema;
        this.cacheConfig = copy.cacheConfig;
        this.connectionParam = copy.connectionParam;
        this.jdbcOptions = copy.jdbcOptions;
        this.config = copy.config;
        this.filters = copy.filters;
    }

    @Override
    public DynamicTableSource copy() {
        return new HologresTableSource(this);
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
        StringBuilder filterBuilder = new StringBuilder();
        String filter = jdbcOptions.getFilter();
        if (StringUtils.isNotEmpty(filter)) {
            filterBuilder.append("(");
            filterBuilder.append(filter);
            filterBuilder.append(")");
        }
        if (!CollectionUtils.isEmpty(this.filters)) {
            if (filterBuilder.length() > 0) filterBuilder.append(" AND ");
            filterBuilder.append(String.join(" AND ", filters));
        }

        JDBCOptions newJdbcOptions =
                new JDBCOptions(
                        this.jdbcOptions.getDatabase(),
                        this.jdbcOptions.getTable(),
                        this.jdbcOptions.getUsername(),
                        this.jdbcOptions.getPassword(),
                        this.jdbcOptions.getEndpoint(),
                        this.jdbcOptions.getDelimiter(),
                        filterBuilder.toString());

        return InputFormatProvider.of(new HologresBulkreadInputFormat(newJdbcOptions, tableSchema));
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> flinkFilters) {
        List<String> sqlFilters = new ArrayList<>();
        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
        FilterVisitor filterVisitor = this.filterVisitor;

        for (ResolvedExpression flinkFilter : flinkFilters) {
            Optional<String> optional = flinkFilter.accept(filterVisitor);
            if (optional.isPresent()) {
                sqlFilters.add(optional.get());
                acceptedFilters.add(flinkFilter);
            }
        }

        this.filters = sqlFilters;
        return SupportsFilterPushDown.Result.of(acceptedFilters, flinkFilters);
    }
}
