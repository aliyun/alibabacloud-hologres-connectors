package com.alibaba.ververica.connectors.hologres.jdbc.source;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.util.Preconditions;

import com.alibaba.ververica.connectors.common.dim.AsyncLookupFunctionWrapper;
import com.alibaba.ververica.connectors.common.dim.DimOptions;
import com.alibaba.ververica.connectors.common.dim.LookupFunctionWrapper;
import com.alibaba.ververica.connectors.common.dim.cache.CacheConfig;
import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresParams;
import com.alibaba.ververica.connectors.hologres.jdbc.config.JDBCOptions;
import com.alibaba.ververica.connectors.hologres.jdbc.source.lookup.HologresAsyncLookupFunction;
import com.alibaba.ververica.connectors.hologres.jdbc.source.lookup.HologresLookupFunction;

/**
 * Table Source.
 */
public class HologresTableSource implements LookupTableSource {
	private String tableName;
	private TableSchema tableSchema;
	private CacheConfig cacheConfig;
	private HologresParams connectionParam;
	private ReadableConfig config;
	private JDBCOptions jdbcOptions;

	public HologresTableSource(
			String tableName, TableSchema tableSchema, CacheConfig cacheConfig,
			HologresParams param,
			JDBCOptions jdbcOptions,
			ReadableConfig config) {
		this.tableName = tableName;
		this.tableSchema = tableSchema;
		this.cacheConfig = cacheConfig;
		this.connectionParam = param;
		this.jdbcOptions = jdbcOptions;
		this.config = config;
	}

	@Override
	public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
		String[] lookupKeys = new String[lookupContext.getKeys().length];
		for (int i = 0; i < lookupKeys.length; i++) {
			int[] innerKeyArr = lookupContext.getKeys()[i];
			Preconditions.checkArgument(innerKeyArr.length == 1,
					"Do not support nested lookup keys");
			lookupKeys[i] = tableSchema.getFieldNames()[innerKeyArr[0]];
		}

		if (config.get(DimOptions.OPTIONAL_ASYNC)) {
			AsyncTableFunction<RowData> lookupFunc =
					new AsyncLookupFunctionWrapper(new HologresAsyncLookupFunction(
							tableName,
							tableSchema,
							lookupKeys,
							connectionParam,
							cacheConfig.getCacheStrategy()
					));
			return AsyncTableFunctionProvider.of(lookupFunc);
		} else {
			TableFunction<RowData> lookupFunc =
					new LookupFunctionWrapper(new HologresLookupFunction(
							tableName,
							tableSchema,
							lookupKeys,
							connectionParam,
							cacheConfig.getCacheStrategy()));
			return TableFunctionProvider.of(lookupFunc);
		}
	}

	@Override
	public DynamicTableSource copy() {
		return new HologresTableSource(tableName, tableSchema, cacheConfig, connectionParam, jdbcOptions, config);
	}

	@Override
	public String asSummaryString() {
		return "Hologres-" + tableName;
	}
}
