package com.alibaba.hologres.connector.flink.factory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import com.alibaba.hologres.connector.flink.config.HologresConfigs;
import com.alibaba.hologres.connector.flink.sink.HologresDynamicTableSink;
import com.alibaba.hologres.connector.flink.source.HologresTableSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/** Factory for sink. */
public class HologresTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        Context normalizedContext = normalizeContext(this, context);
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, normalizedContext);
        helper.validate();
        final ReadableConfig config = helper.getOptions();
        String tableName = context.getObjectIdentifier().getObjectName();
        TableSchema tableSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        return new HologresDynamicTableSink(tableName, tableSchema, config);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        Context normalizedContext = normalizeContext(this, context);
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, normalizedContext);
        helper.validate();
        final ReadableConfig config = helper.getOptions();
        String tableName = context.getObjectIdentifier().getObjectName();
        TableSchema schema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        LookupCache cache = null;

        if (config.get(LookupOptions.CACHE_TYPE).equals(LookupOptions.LookupCacheType.PARTIAL)) {
            cache = DefaultLookupCache.fromConfig(config);
        }

        return new HologresTableSource(tableName, schema, config, cache);
    }

    @Override
    public String factoryIdentifier() {
        return HologresConfigs.HOLOGRES_TABLE_TYPE;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(HologresConfigs.USERNAME);
        set.add(HologresConfigs.PASSWORD);
        set.add(HologresConfigs.DATABASE);
        set.add(HologresConfigs.ENDPOINT);
        set.add(HologresConfigs.TABLE);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return HologresConfigs.getAllOption();
    }

    public static DynamicTableFactory.Context normalizeContext(
            DynamicTableFactory factory, DynamicTableFactory.Context context) {
        // Options with normalized letter case
        Map<String, String> convertedOptions;

        // Table options in catalog, provided in WITH clause of CREATE TABLE statement.
        // Key: option key e.g. startupMode
        // Value: option value  e.g. EARLIEST
        Map<String, String> catalogOptions = context.getCatalogTable().getOptions();

        convertedOptions = normalizeOptionCaseAsFactory(factory, catalogOptions);

        // Put the converted options into catalog
        return new FactoryUtil.DefaultDynamicTableContext(
                context.getObjectIdentifier(),
                context.getCatalogTable().copy(convertedOptions),
                context.getEnrichmentOptions(),
                context.getConfiguration(),
                context.getClassLoader(),
                context.isTemporary());
    }

    /**
     * Normalize letter case (upper/lower case) of map-style options according to options defined in
     * factory.
     *
     * <p>For example:
     *
     * <ul>
     *   <li>Option defined in factory: AbCdEfGh
     *   <li>Option passed in: 'abcdefgh' = 'foo'
     * </ul>
     *
     * <p>Then this option will be converted to 'AbCdEfGh' = 'foo' finally.
     *
     * <p>For options that not defined in factory, this method will just keep its original
     * expression.
     *
     * @param factory Factory with defined required and optional table options.
     * @param options Options need to be normalized
     */
    public static Map<String, String> normalizeOptionCaseAsFactory(
            Factory factory, Map<String, String> options) {
        // Options with normalized letter case
        Map<String, String> normalizedOptions = new HashMap<>();

        // Required options defined in table factory
        // Key: Lower-case option keys. e.g. startupmode
        // Value: Original option keys. e.g. startupMode
        Map<String, String> requiredOptionKeysLowerCaseToOriginal =
                factory.requiredOptions().stream()
                        .collect(
                                Collectors.toMap(
                                        option -> option.key().toLowerCase(), ConfigOption::key));

        // Optional options defined in table factory
        // Key: Lower-case option keys. e.g. startupmode
        // Value: Original option keys. e.g. startupMode
        Map<String, String> optionalOptionKeysLowerCaseToOriginal =
                factory.optionalOptions().stream()
                        .collect(
                                Collectors.toMap(
                                        option -> option.key().toLowerCase(), ConfigOption::key));

        // Normalize passed-in options according to option keys defined in the factory
        for (Map.Entry<String, String> entry : options.entrySet()) {
            final String catalogOptionKey = entry.getKey();
            final String catalogOptionValue = entry.getValue();
            normalizedOptions.put(
                    // Convert passed-in option key to lower case and search it in factory-defined
                    // required and optional options
                    requiredOptionKeysLowerCaseToOriginal.containsKey(
                                    catalogOptionKey.toLowerCase())
                            ?
                            // If we can find it in factory-defined required option keys, replace it
                            // with the factory version
                            requiredOptionKeysLowerCaseToOriginal.get(
                                    catalogOptionKey.toLowerCase())
                            :
                            // If we cannot find it in required, try to search it from
                            // factory-defined optional options and replace it
                            // If it is not in optional options either, just use its original
                            // expression
                            optionalOptionKeysLowerCaseToOriginal.getOrDefault(
                                    catalogOptionKey.toLowerCase(), catalogOptionKey),
                    catalogOptionValue);
        }
        return normalizedOptions;
    }
}
