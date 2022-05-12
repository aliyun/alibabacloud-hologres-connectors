package com.alibaba.ververica.connectors.hologres.jdbc;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import static org.apache.flink.configuration.ConfigOptions.key;

/** HologresJDBCConfigs. */
public class HologresJDBCConfigs {
    // connection options
    public static final ConfigOption<Integer> OPTIONAL_CLIENT_CONNECTION_POOL_SIZE =
            key("connectionSize".toLowerCase()).intType().defaultValue(3);
    public static final ConfigOption<Integer> OPTIONAL_JDBC_RETRY_COUNT =
            key("jdbcRetryCount".toLowerCase()).intType().defaultValue(10);
    public static final ConfigOption<Long> OPTIONAL_RETRY_SLEEP_INIT_MS =
            key("jdbcRetrySleepInitMs".toLowerCase()).longType().defaultValue(1000L);
    public static final ConfigOption<Long> OPTIONAL_RETRY_SLEEP_STEP_MS =
            key("jdbcRetrySleepStepMs".toLowerCase()).longType().defaultValue(5000L);
    public static final ConfigOption<Long> OPTIONAL_JDBC_CONNECTION_MAX_IDLE_MS =
            key("jdbcConnectionMaxIdleMs".toLowerCase()).longType().defaultValue(60000L);
    public static final ConfigOption<Long> OPTIONAL_JDBC_META_CACHE_TTL =
            key("jdbcMetaCacheTTL".toLowerCase()).longType().defaultValue(60000L);
    public static final ConfigOption<Integer> OPTIONAL_JDBC_META_AUTO_REFRESH_FACTOR =
            key("jdbcMetaAutoRefreshFactor".toLowerCase()).intType().defaultValue(-1);

    // source options
    public static final ConfigOption<Integer> OPTIONAL_JDBC_READ_BATCH_SIZE =
            key("jdbcReadBatchSize".toLowerCase()).intType().defaultValue(128);
    public static final ConfigOption<Integer> OPTIONAL_JDBC_READ_BATCH_QUEUE_SIZE =
            key("jdbcReadBatchQueueSize".toLowerCase()).intType().defaultValue(256);
    public static final ConfigOption<Integer> OPTIONAL_JDBC_SCAN_FETCH_SIZE =
            key("jdbcScanFetchSize".toLowerCase()).intType().defaultValue(256);
    public static final ConfigOption<Integer> OPTIONAL_JDBC_SCAN_TIMEOUT_SECONDS =
            key("jdbcScanTimeoutSeconds".toLowerCase()).intType().defaultValue(256);

    // Sink options
    public static final ConfigOption<Integer> OPTIONAL_JDBC_WRITE_BATCH_SIZE =
            key("jdbcWriteBatchSize".toLowerCase()).intType().defaultValue(256);
    public static final ConfigOption<Long> OPTIONAL_JDBC_WRITE_BATCH_BYTE_SIZE =
            key("jdbcWriteBatchByteSize".toLowerCase()).longType().defaultValue(2097152L);
    public static final ConfigOption<Long> OPTIONAL_JDBC_WRITE_BATCH_TOTAL_BYTE_SIZE =
            key("jdbcWriteBatchTotalByteSize".toLowerCase()).longType().defaultValue(20971520L);
    public static final ConfigOption<Long> OPTIONAL_JDBC_WRITE_FLUSH_INTERVAL =
            key("jdbcWriteFlushInterval".toLowerCase()).longType().defaultValue(10000L);
    public static final ConfigOption<Boolean> OPTIONAL_JDBC_ENABLE_DEFAULT_FOR_NOT_NULL_COLUMN =
            key("jdbcEnableDefaultForNotNullColumn".toLowerCase()).booleanType().defaultValue(true);

    // Dim options
    public static final ConfigOption<Boolean> INSERT_IF_NOT_EXISTS =
            ConfigOptions.key("insertIfNotExists".toLowerCase()).booleanType().defaultValue(false);
    public static final ConfigOption<Integer> ASYNC_CALLBACK_EXECUTION_POOL =
            ConfigOptions.key("asyncCallbackPool".toLowerCase()).intType().defaultValue(1);
}
