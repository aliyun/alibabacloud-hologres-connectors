package com.alibaba.ververica.connectors.hologres.jdbc;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import static org.apache.flink.configuration.ConfigOptions.key;

/** HologresJDBCConfigs. */
public class HologresJDBCConfigs {
    // connection options
    public static final ConfigOption<Integer> OPTIONAL_CLIENT_CONNECTION_POOL_SIZE =
            key("connectionSize".toLowerCase()).intType().defaultValue(3);
    public static final ConfigOption<String> OPTIONAL_JDBC_SHARED_CONNECTION_POOL_NAME =
            key("connectionPoolName".toLowerCase()).stringType().noDefaultValue();
    public static final ConfigOption<Boolean> OPTIONAL_JDBC_FIXED_CONNECTION_MODE =
            key("fixedConnectionMode".toLowerCase()).booleanType().defaultValue(false);
    public static final ConfigOption<String> OPTIONAL_CONNECTION_SSL_MODE =
            key("connection.ssl.mode".toLowerCase()).stringType().defaultValue("disable");
    public static final ConfigOption<String> OPTIONAL_CONNECTION_SSL_ROOT_CERT_LOCATION =
            key("connection.ssl.root-cert.location".toLowerCase())
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Users need to upload files to vvp in advance, and the path must be /flink/usrlib/${certificate file name}");
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
    public static final ConfigOption<Boolean> OPTIONAL_JDBC_USE_LEGACY_PUT_HANDLER =
            key("jdbcUseLegacyPutHandler".toLowerCase()).booleanType().defaultValue(false);
    public static final ConfigOption<Boolean> OPTIONAL_JDBC_ENABLE_DEFAULT_FOR_NOT_NULL_COLUMN =
            key("jdbcEnableDefaultForNotNullColumn".toLowerCase()).booleanType().defaultValue(true);
    public static final ConfigOption<Boolean> OPTIONAL_ENABLE_REMOVE_U0000_IN_TEXT =
            key("remove-u0000-in-text.enabled".toLowerCase()).booleanType().defaultValue(false);
    public static final ConfigOption<Boolean> OPTIONAL_ENABLE_DEDUPLICATION =
            key("deduplication.enabled".toLowerCase()).booleanType().defaultValue(true);

    // Dim options
    public static final ConfigOption<Boolean> INSERT_IF_NOT_EXISTS =
            ConfigOptions.key("insertIfNotExists".toLowerCase()).booleanType().defaultValue(false);

    // Copy options
    public static final ConfigOption<Boolean> COPY_WRITE_MODE =
            key("jdbcCopyWriteMode".toLowerCase()).booleanType().defaultValue(false);
    public static final ConfigOption<String> COPY_WRITE_FORMAT =
            key("jdbcCopyWriteFormat".toLowerCase())
                    .stringType()
                    .defaultValue("binary")
                    .withDescription(
                            "copy format will be binary or text, if value is not binary, we use text");
    public static final ConfigOption<Boolean> OPTIONAL_BULK_LOAD =
            key("bulkLoad".toLowerCase()).booleanType().defaultValue(false);

    public static final ConfigOption<Boolean> COPY_WRITE_DIRECT_CONNECT =
            key("jdbcCopyWriteDirectConnect".toLowerCase()).booleanType().defaultValue(true);
}
