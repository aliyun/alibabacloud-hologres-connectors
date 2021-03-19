package com.alibaba.ververica.connectors.hologres.jdbc;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import static org.apache.flink.configuration.ConfigOptions.key;

/** HologresJdbcConfigs. */
public class HologresJdbcConfigs {
    public static final ConfigOption<Integer> OPTIONAL_CLIENT_CONNECTION_POOL_SIZE =
            key("connectionSize".toLowerCase()).intType().defaultValue(3);
    // Sink options
    public static final ConfigOption<Integer> OPTIONAL_JDBC_WRITE_BATCH_SIZE =
            key("jdbcWriteBatchSize".toLowerCase()).intType().defaultValue(256);
    public static final ConfigOption<Integer> OPTIONAL_JDBC_WRITE_FLUSH_INTERVAL =
            key("jdbcWriteFlushInterval".toLowerCase()).intType().defaultValue(10000);

    // Dim options
    public static final ConfigOption<Boolean> INSERT_IF_NOT_EXISTS =
            ConfigOptions.key("insertIfNotExists".toLowerCase()).booleanType().defaultValue(false);
}
