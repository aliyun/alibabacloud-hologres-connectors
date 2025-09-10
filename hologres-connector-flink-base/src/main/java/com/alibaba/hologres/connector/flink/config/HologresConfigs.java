package com.alibaba.hologres.connector.flink.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.connector.source.lookup.LookupOptions;

import org.apache.commons.lang3.reflect.FieldUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

/** Configs for hologres connector. */
public class HologresConfigs {
    public static final String HOLOGRES_TABLE_TYPE = "hologres";

    // connection config
    public static final ConfigOption<String> ENDPOINT =
            key("endpoint").stringType().noDefaultValue();
    public static final ConfigOption<String> DATABASE = key("dbname").stringType().noDefaultValue();
    public static final ConfigOption<String> TABLE = key("tablename").stringType().noDefaultValue();
    public static final ConfigOption<String> USERNAME =
            key("username").stringType().noDefaultValue();
    public static final ConfigOption<String> PASSWORD =
            key("password").stringType().noDefaultValue();

    public static final ConfigOption<Integer> CONNECTION_POOL_SIZE =
            key("connection.pool.size".toLowerCase()).intType().defaultValue(3);
    public static final ConfigOption<String> CONNECTION_POOL_NAME =
            key("connection.pool.name".toLowerCase()).stringType().noDefaultValue();
    public static final ConfigOption<Boolean> CONNECTION_FIXED =
            key("connection.fixed".toLowerCase()).booleanType().defaultValue(false);
    public static final ConfigOption<Boolean> CONNECTION_DIRECT =
            key("connection.direct".toLowerCase()).booleanType().defaultValue(false);
    public static final ConfigOption<String> CONNECTION_SSL_MODE =
            key("connection.ssl.mode".toLowerCase()).stringType().defaultValue("disable");
    public static final ConfigOption<String> CONNECTION_SSL_ROOT_CERT_LOCATION =
            key("connection.ssl.root-cert.location".toLowerCase())
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Users need to upload files to vvp in advance, and the path must be /flink/usrlib/${certificate file name}");
    public static final ConfigOption<Boolean> ENABLE_AKV4 =
            key("connection.akv4.enabled".toLowerCase()).booleanType().defaultValue(false);
    public static final ConfigOption<String> AKV4_REGION =
            key("connection.akv4.region".toLowerCase()).stringType().noDefaultValue();
    public static final ConfigOption<Integer> STATEMENT_TIMEOUT_SECONDS =
            key("statement-timeout-seconds".toLowerCase()).intType().defaultValue(28800);
    public static final ConfigOption<Boolean> ENABLE_SERVERLESS_COMPUTING =
            key("serverless-computing.enabled".toLowerCase()).booleanType().defaultValue(false);
    public static final ConfigOption<Integer> SERVERLESS_COMPUTING_QUERY_PRIORITY =
            key("serverless-computing.query-priority".toLowerCase()).intType().defaultValue(3);

    public static final ConfigOption<Integer> JDBC_RETRY_COUNT =
            key("retry-count".toLowerCase()).intType().defaultValue(10);
    public static final ConfigOption<Long> RETRY_SLEEP_STEP_MS =
            key("retry-sleep-step-ms".toLowerCase()).longType().defaultValue(5000L);
    public static final ConfigOption<Long> JDBC_CONNECTION_MAX_IDLE_MS =
            key("connection.max-idle-ms".toLowerCase()).longType().defaultValue(60000L);
    public static final ConfigOption<Long> JDBC_META_CACHE_TTL =
            key("connection.meta-cache-ttl-ms".toLowerCase()).longType().defaultValue(60000L);

    // source options
    public static final ConfigOption<Integer> SCAN_FETCH_SIZE =
            key("source.scan.fetch-size".toLowerCase()).intType().defaultValue(256);
    public static final ConfigOption<Integer> SCAN_TIMEOUT_SECONDS =
            key("source.scan.timeout-seconds".toLowerCase()).intType().defaultValue(28800);
    public static final ConfigOption<Boolean> ENABLE_FILTER_PUSH_DOWN =
            key("source.filter-push-down.enabled".toLowerCase()).booleanType().defaultValue(false);

    // Sink options
    public static final ConfigOption<WriteMode> WRITE_MODE =
            key("sink.write-mode".toLowerCase())
                    .enumType(WriteMode.class)
                    .defaultValue(WriteMode.INSERT);

    public static final ConfigOption<Boolean> ENABLE_API_VERSION_V2 =
            key("sink.api-version-v2.enabled".toLowerCase()).booleanType().defaultValue(false);
    public static final ConfigOption<Boolean> CREATE_MISSING_PARTITION_TABLE =
            key("sink.create-missing-partition".toLowerCase()).booleanType().defaultValue(false);
    public static final ConfigOption<String> ON_CONFLICT_ACTION =
            key("sink.on-conflict-action".toLowerCase())
                    .stringType()
                    .defaultValue("insertOrUpdate");
    public static final ConfigOption<Boolean> SINK_IGNORE_DELETE =
            key("sink.ignore-delete".toLowerCase()).booleanType().defaultValue(true);
    public static final ConfigOption<Boolean> ENABLE_REMOVE_U0000_IN_TEXT =
            key("sink.remove-u0000-in-text.enabled".toLowerCase()).booleanType().defaultValue(true);
    public static final ConfigOption<Boolean> ENABLE_DEDUPLICATION =
            key("sink.deduplication.enabled".toLowerCase()).booleanType().defaultValue(true);
    public static final ConfigOption<Boolean> ENABLE_AGGRESSIVE =
            key("sink.aggressive.enabled".toLowerCase()).booleanType().defaultValue(false);
    public static final ConfigOption<Boolean> ENABLE_AFFECTED_ROWS =
            key("sink.affect-rows.enabled".toLowerCase()).booleanType().defaultValue(false);
    public static final ConfigOption<Integer> WRITE_BATCH_SIZE =
            key("sink.insert.batch-size".toLowerCase()).intType().defaultValue(256);
    public static final ConfigOption<Long> WRITE_BATCH_BYTE_SIZE =
            key("sink.insert.batch-byte-size".toLowerCase()).longType().defaultValue(2097152L);
    public static final ConfigOption<Long> WRITE_BATCH_TOTAL_BYTE_SIZE =
            key("sink.insert.batch-total-byte-size".toLowerCase())
                    .longType()
                    .defaultValue(20971520L);
    public static final ConfigOption<Long> WRITE_FLUSH_INTERVAL =
            key("sink.insert.flush-interval-ms".toLowerCase()).longType().defaultValue(10000L);
    public static final ConfigOption<Boolean> WRITE_USE_LEGACY_PUT_HANDLER =
            key("sink.insert.legacy-put-handler".toLowerCase()).booleanType().defaultValue(false);
    public static final ConfigOption<DirtyDataStrategy> INSERT_DIRTY_DATA_STRATEGY =
            ConfigOptions.key("sink.insert.dirty-data-strategy".toLowerCase())
                    .enumType(DirtyDataStrategy.class)
                    .defaultValue(DirtyDataStrategy.EXCEPTION);
    public static final ConfigOption<String> INSERT_CONFLICT_UPDATE_SET =
            key("sink.insert.conflict-update-set".toLowerCase()).stringType().noDefaultValue();
    public static final ConfigOption<String> INSERT_CONFLICT_WHERE =
            key("sink.insert.conflict-where".toLowerCase()).stringType().noDefaultValue();
    public static final ConfigOption<String> CHECK_AND_PUT_COLUMN =
            key("sink.check-and-put.column".toLowerCase()).stringType().noDefaultValue();
    public static final ConfigOption<String> CHECK_AND_PUT_OPERATOR =
            key("sink.check-and-put.operator".toLowerCase()).stringType().defaultValue("GREATER");
    public static final ConfigOption<String> CHECK_AND_PUT_NULL_AS =
            key("sink.check-and-put.null-as".toLowerCase()).stringType().noDefaultValue();
    public static final ConfigOption<Boolean> ENABLE_HOLD_ON_UPDATE_BEFORE =
            key("sink.hold-on-update-before.enabled".toLowerCase())
                    .booleanType()
                    .defaultValue(false);
    public static final ConfigOption<Boolean> IGNORE_NULL_WHEN_UPDATE =
            key("sink.ignore-null-when-update.enabled".toLowerCase())
                    .booleanType()
                    .defaultValue(false);
    public static final ConfigOption<Boolean> ENABLE_PARTIAL_INSERT =
            key("sink.partial-insert.enabled".toLowerCase()).booleanType().defaultValue(false);
    public static final ConfigOption<String> COPY_WRITE_FORMAT =
            key("sink.copy.format".toLowerCase())
                    .stringType()
                    .defaultValue("binary")
                    .withDescription(
                            "copy format will be binary or text, if value is not binary, we use text");
    public static final ConfigOption<Boolean> ENABLE_DIRTY_DATA_CHECK =
            key("sink.dirty-data-check.enabled".toLowerCase()).booleanType().defaultValue(true);
    public static final ConfigOption<Boolean> ENABLE_MULTI_TABLE_DATATYPE_TOLERANT =
            key("sink.multi-table.datatype-tolerant.enabled".toLowerCase())
                    .booleanType()
                    .defaultValue(false);
    public static final ConfigOption<Boolean> ENABLE_MULTI_TABLE_EXTRA_COLUMN_TOLERANT =
            key("sink.multi-table.extra-column-tolerant.enabled".toLowerCase())
                    .booleanType()
                    .defaultValue(false);

    // 仅copy模式生效，根据总并发数，当前并发id和写入表的shard数，计算当前task将会写入的target shard，需要上游数据已经基于distribution
    // key进行了Repartition，且使用取余的方式分布在了holo的shard上
    public static final ConfigOption<Boolean> ENABLE_RESHUFFLE_BY_HOLO =
            key("sink.reshuffle-by-holo-distribution-key.enabled".toLowerCase())
                    .booleanType()
                    .defaultValue(false);

    // Dim options, 其他的一些维表相关配置来自org.apache.flink.table.connector.source.lookup.LookupOptions
    public static final ConfigOption<Boolean> INSERT_IF_NOT_EXISTS =
            key("lookup.insert-if-not-exists".toLowerCase()).booleanType().defaultValue(false);
    public static final ConfigOption<Boolean> LOOKUP_ASYNC =
            key("lookup.async".toLowerCase()).booleanType().defaultValue(true);
    public static final ConfigOption<Integer> READ_BATCH_SIZE =
            key("lookup.read.batch-size".toLowerCase()).intType().defaultValue(128);
    public static final ConfigOption<Integer> READ_BATCH_QUEUE_SIZE =
            key("lookup.read.batch-queue-size".toLowerCase()).intType().defaultValue(256);
    public static final ConfigOption<Boolean> ENABLE_READ_COLUMN_TABLE =
            key("lookup.read.column-table.enabled".toLowerCase()).booleanType().defaultValue(false);

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

        LookupOptions dimOptions = new LookupOptions();
        Arrays.stream(FieldUtils.getAllFields(LookupOptions.class))
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
