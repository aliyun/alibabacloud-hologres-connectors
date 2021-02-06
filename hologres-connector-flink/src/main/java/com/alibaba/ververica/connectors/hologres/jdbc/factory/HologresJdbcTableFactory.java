package com.alibaba.ververica.connectors.hologres.jdbc.factory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import com.alibaba.ververica.connectors.common.dim.cache.CacheConfig;
import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresJdbcConfigs;
import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresParams;
import com.alibaba.ververica.connectors.hologres.jdbc.config.JDBCOptions;
import com.alibaba.ververica.connectors.hologres.jdbc.sink.HologresJdbcDynamicTableSink;
import com.alibaba.ververica.connectors.hologres.jdbc.source.HologresTableSource;
import com.alibaba.ververica.connectors.hologres.jdbc.util.JDBCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static com.alibaba.ververica.connectors.common.util.ContextUtil.transformContext;
import static com.alibaba.ververica.connectors.hologres.jdbc.config.HologresJdbcConfigs.DATABASE;
import static com.alibaba.ververica.connectors.hologres.jdbc.config.HologresJdbcConfigs.ENDPOINT;
import static com.alibaba.ververica.connectors.hologres.jdbc.config.HologresJdbcConfigs.PASSWORD;
import static com.alibaba.ververica.connectors.hologres.jdbc.config.HologresJdbcConfigs.TABLE;
import static com.alibaba.ververica.connectors.hologres.jdbc.config.HologresJdbcConfigs.USERNAME;
import static com.alibaba.ververica.connectors.hologres.jdbc.config.HologresJdbcConfigs.getAllOption;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 * HologresJdbcTableFactory.
 */
public class HologresJdbcTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {
	private static final transient Logger LOG = LoggerFactory.getLogger(HologresJdbcTableFactory.class);

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		transformContext(this, context);
		FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
		helper.validate();
		final ReadableConfig config = helper.getOptions();
		TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		HologresParams params = new HologresParams(config);
		return new HologresJdbcDynamicTableSink(tableSchema, params);
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		transformContext(this, context);
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		helper.validate();
		final ReadableConfig config = helper.getOptions();
		String tableName = context.getObjectIdentifier().getObjectName();
		TableSchema schema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

		CacheConfig cacheConfig = CacheConfig.createCacheConfig(config, "LRU");
		JDBCOptions jdbcOptions = JDBCUtils.getJDBCOptions(config);
		HologresParams params = new HologresParams(config);
		return new HologresTableSource(tableName, schema, cacheConfig, params, jdbcOptions, config);
	}

	@Override
	public String factoryIdentifier() {
		return HologresJdbcConfigs.HOLOGRES_TABLE_TYPE;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		set.add(USERNAME);
		set.add(PASSWORD);
		set.add(DATABASE);
		set.add(ENDPOINT);
		set.add(TABLE);
		return set;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		return getAllOption();
	}
}
