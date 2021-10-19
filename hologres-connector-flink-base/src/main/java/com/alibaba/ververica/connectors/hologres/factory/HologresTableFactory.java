package com.alibaba.ververica.connectors.hologres.factory;

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
import com.alibaba.ververica.connectors.hologres.config.HologresConfigs;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.config.JDBCOptions;
import com.alibaba.ververica.connectors.hologres.sink.HologresDynamicTableSink;
import com.alibaba.ververica.connectors.hologres.source.HologresTableSource;
import com.alibaba.ververica.connectors.hologres.utils.JDBCUtils;

import java.util.HashSet;
import java.util.Set;

import static com.alibaba.ververica.connectors.hologres.config.HologresConfigs.DATABASE;
import static com.alibaba.ververica.connectors.hologres.config.HologresConfigs.ENDPOINT;
import static com.alibaba.ververica.connectors.hologres.config.HologresConfigs.PASSWORD;
import static com.alibaba.ververica.connectors.hologres.config.HologresConfigs.TABLE;
import static com.alibaba.ververica.connectors.hologres.config.HologresConfigs.USERNAME;
import static com.alibaba.ververica.connectors.hologres.utils.FlinkUtil.transformContext;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/** Factory for sink. */
public class HologresTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        transformContext(this, context);
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();
        final ReadableConfig config = helper.getOptions();
        TableSchema tableSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        return new HologresDynamicTableSink(tableSchema, config);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        transformContext(this, context);
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        final ReadableConfig config = helper.getOptions();
        String tableName = context.getObjectIdentifier().getObjectName();
        TableSchema schema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        CacheConfig cacheConfig = CacheConfig.createCacheConfig(config, "LRU");
        JDBCOptions jdbcOptions = JDBCUtils.getJDBCOptions(config);
        return new HologresTableSource(
                tableName,
                schema,
                cacheConfig,
                new HologresConnectionParam(config),
                jdbcOptions,
                config);
    }

    @Override
    public String factoryIdentifier() {
        return HologresConfigs.HOLOGRES_TABLE_TYPE;
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
        return HologresConfigs.getAllOption();
    }
}
