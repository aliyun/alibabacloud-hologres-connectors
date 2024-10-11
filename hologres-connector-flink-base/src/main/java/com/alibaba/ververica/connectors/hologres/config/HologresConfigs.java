package com.alibaba.ververica.connectors.hologres.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import com.alibaba.ververica.connectors.common.dim.DimOptions;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCConfigs;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

/** Configs for hologres connector. */
public class HologresConfigs {
    public static final String HOLOGRES_TABLE_TYPE = "hologres";

    public static final String DEFAULT_FIELD_DELIMITER = "\u0002";

    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint").stringType().noDefaultValue();
    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("dbname").stringType().noDefaultValue();
    public static final ConfigOption<String> TABLE =
            ConfigOptions.key("tablename").stringType().noDefaultValue();
    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username").stringType().noDefaultValue();
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password").stringType().noDefaultValue();
    public static final ConfigOption<String> FIELD_DELIMITER =
            ConfigOptions.key("field_delimiter").stringType().defaultValue(DEFAULT_FIELD_DELIMITER);
    public static final ConfigOption<String> ARRAY_DELIMITER =
            ConfigOptions.key("arrayDelimiter".toLowerCase())
                    .stringType()
                    .defaultValue(DEFAULT_FIELD_DELIMITER);

    public static final ConfigOption<Boolean> INSERT_OR_UPDATE =
            key("insertOrUpdate".toLowerCase()).booleanType().defaultValue(false);

    public static final ConfigOption<Boolean> CREATE_MISSING_PARTITION_TABLE =
            key("createPartTable".toLowerCase()).booleanType().defaultValue(false);

    public static final ConfigOption<String> MUTATE_TYPE =
            key("mutateType".toLowerCase()).stringType().defaultValue("insertOrIgnore");

    public static final ConfigOption<Integer> OPTIONAL_SPLIT_DATA_SIZE =
            ConfigOptions.key("splitDataSize".toLowerCase()).intType().defaultValue(262144);
    public static final ConfigOption<Boolean> OPTIONAL_SINK_IGNORE_DELETE =
            ConfigOptions.key("ignoreDelete".toLowerCase()).booleanType().defaultValue(true);

    public static final ConfigOption<Boolean> IGNORE_NULL_WHEN_UPDATE =
            key("ignoreNullWhenUpdate".toLowerCase()).defaultValue(false);

    public static final ConfigOption<String> ACTION_ON_INSERT_ERROR =
            ConfigOptions.key("actionOnInsert".toLowerCase())
                    .stringType()
                    .defaultValue("Exception");

    public static Set<ConfigOption<?>> getAllOption() {
        Set<ConfigOption<?>> allOptions = new HashSet<>();
        HologresConfigs config = new HologresConfigs();
        Arrays.stream(FieldUtils.getAllFields(HologresConfigs.class))
                .filter(f -> ConfigOption.class.isAssignableFrom(f.getType()))
                .forEach(
                        f -> {
                            try {
                                allOptions.add((ConfigOption) f.get(config));
                            } catch (IllegalAccessException e) {
                            }
                        });

        HologresJDBCConfigs jdbcConfigs = new HologresJDBCConfigs();
        Arrays.stream(FieldUtils.getAllFields(HologresJDBCConfigs.class))
                .filter(f -> ConfigOption.class.isAssignableFrom(f.getType()))
                .forEach(
                        f -> {
                            try {
                                allOptions.add((ConfigOption) f.get(jdbcConfigs));
                            } catch (IllegalAccessException e) {
                            }
                        });

        DimOptions dimOptions = new DimOptions();
        Arrays.stream(FieldUtils.getAllFields(DimOptions.class))
                .filter(f -> ConfigOption.class.isAssignableFrom(f.getType()))
                .forEach(
                        f -> {
                            try {
                                allOptions.add((ConfigOption) f.get(dimOptions));
                            } catch (IllegalAccessException e) {
                            }
                        });

        allOptions.add(SINK_PARALLELISM);

        return allOptions;
    }
}
