package com.alibaba.hologres.connector.flink.source;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import com.alibaba.hologres.connector.flink.api.HologresTableSchema;
import com.alibaba.hologres.connector.flink.config.HologresConfigs;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.config.JDBCOptions;
import com.alibaba.hologres.connector.flink.source.bulkread.HologresBulkreadInputFormat;
import com.alibaba.hologres.connector.flink.source.lookup.HologresAsyncLookupFunction;
import com.alibaba.hologres.connector.flink.source.lookup.HologresLookupFunction;
import com.alibaba.hologres.connector.flink.utils.HologresUtils;
import com.alibaba.hologres.connector.flink.utils.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Table Source. */
public class HologresTableSource
        implements DynamicTableSource,
                LookupTableSource,
                ScanTableSource,
                SupportsLimitPushDown,
                SupportsFilterPushDown {
    private static final Logger LOG = LoggerFactory.getLogger(HologresTableSource.class);

    private String tableName;
    private TableSchema tableSchema;
    private HologresConnectionParam connectionParam;
    private ReadableConfig config;
    private JDBCOptions jdbcOptions;
    private String[] fieldNames;
    private LogicalType[] fieldTypes;
    private final LookupCache cache;
    private final boolean enableFilterPushDown;
    private ArrayList<String> resolvedPredicates = new ArrayList<>();
    private long limit = -1;

    public HologresTableSource(
            String tableName, TableSchema schema, ReadableConfig config, LookupCache cache) {
        this.tableName = tableName;
        this.tableSchema = checkNotNull(schema);
        this.config = checkNotNull(config);
        this.fieldNames = tableSchema.getFieldNames();
        this.fieldTypes = SchemaUtil.getLogicalTypes(tableSchema);
        this.connectionParam = new HologresConnectionParam(config);
        this.jdbcOptions = connectionParam.getJdbcOptions();
        this.config = config;
        this.cache = cache;
        this.enableFilterPushDown = connectionParam.isEnableFilterPushDown();
    }

    @Override
    public DynamicTableSource copy() {
        HologresTableSource source = new HologresTableSource(tableName, tableSchema, config, cache);
        source.resolvedPredicates = new ArrayList<>(this.resolvedPredicates);
        source.limit = limit;
        return source;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
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

        LookupRuntimeProvider lookupProvider;
        if (config.get(HologresConfigs.LOOKUP_ASYNC)) {
            HologresAsyncLookupFunction asyncLookupFunction =
                    new HologresAsyncLookupFunction(
                            tableName,
                            fieldNames,
                            fieldTypes,
                            lookupKeys,
                            connectionParam,
                            hologresTableSchema,
                            hasPrimaryKey);
            if (cache != null) {
                lookupProvider = PartialCachingAsyncLookupProvider.of(asyncLookupFunction, cache);
            } else {
                lookupProvider = AsyncLookupFunctionProvider.of(asyncLookupFunction);
            }
        } else {
            HologresLookupFunction lookupFunction =
                    new HologresLookupFunction(
                            tableName,
                            fieldNames,
                            fieldTypes,
                            lookupKeys,
                            connectionParam,
                            hologresTableSchema,
                            hasPrimaryKey);
            if (cache != null) {
                lookupProvider = PartialCachingLookupProvider.of(lookupFunction, cache);
            } else {
                lookupProvider = LookupFunctionProvider.of(lookupFunction);
            }
        }

        return lookupProvider;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        String filterPredicate = String.join(" and ", resolvedPredicates);
        HologresTableSchema hologresTableSchema =
                HologresTableSchema.get(connectionParam.getJdbcOptions());
        verifyBulkReadJob(tableSchema.getFieldNames(), hologresTableSchema);
        LOG.info("Filter predicate of bulk read is: " + filterPredicate);
        return InputFormatProvider.of(
                new HologresBulkreadInputFormat(
                        connectionParam, jdbcOptions, tableSchema, filterPredicate, limit));
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        if (!enableFilterPushDown) {
            return Result.of(Collections.emptyList(), filters);
        }
        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
        List<ResolvedExpression> remainingFilters = new ArrayList<>();
        HologresUtils.ExpressionExtractor extractor = new HologresUtils.ExpressionExtractor();
        for (ResolvedExpression filter : filters) {
            Optional<String> simplePredicate = filter.accept(extractor);
            if (simplePredicate.isPresent()) {
                resolvedPredicates.add(simplePredicate.get());
                LOG.info(
                        "convert filter expression [{}] to sql [{}].",
                        filter.asSummaryString(),
                        simplePredicate.get());
                acceptedFilters.add(filter);
            } else {
                LOG.info(
                        "filter [{}] not support push down, add it to remainingFilters.",
                        filter.asSummaryString());
                remainingFilters.add(filter);
            }
        }

        // return Result.of(acceptedFilters, remainingFilters);
        return Result.of(acceptedFilters, filters);
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    private void verifyBulkReadJob(String[] fieldNames, HologresTableSchema hologresTableSchema) {
        for (String fieldName : fieldNames) {
            Integer hologresColumnIndex = hologresTableSchema.get().getColumnIndex(fieldName);
            if (hologresColumnIndex == null || hologresColumnIndex < 0) {
                throw new IllegalArgumentException(
                        "Hologres table "
                                + hologresTableSchema.get().getTableName()
                                + " does not have column "
                                + fieldName);
            }
        }
    }
}
