package com.alibaba.hologres.kafka.conf;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/** HoloSinkConfig. */
public class HoloSinkConfig extends AbstractConfig {

    /** CONNECTION CONFIG. */
    private static final String CONFIG_CONNECTION_GROUP = "Connection";

    public static final String JDBC_URL = "connection.jdbcUrl";
    private static final String JDBC_URL_DOC =
            "Hologres的jdbcUrl，包含数据库名称\n "
                    + "jdbc:postgresql://hostname:port/database to connect to Hologres.";
    private static final String JDBC_URL_DISPLAY = "JDBC URL";

    public static final String TABLE = "table";
    private static final String TABLE_DOC = "要写入的Hologres表名.";
    private static final String TABLE_DISPLAY = "Table Name";

    public static final String USERNAME = "connection.username";
    private static final String USERNAME_DOC = "阿里云账号的 AccessKey ID.";
    private static final String USERNAME_DISPLAY = "Hologres User Name";

    public static final String PASSWORD = "connection.password";
    private static final String PASSWORD_DOC = "阿里云账号的 AccessKey SECRET.";
    private static final String PASSWORD_DISPLAY = "Hologres User Password";

    public static final String WRITE_MODE = "connection.writeMode";
    public static final String WRITE_MODE_DEFAULT = "insert_or_replace";
    private static final String WRITE_MODE_DOC =
            "写入模式，以下三种可选：\n"
                    + "INSERT_OR_IGNORE 当主键冲突时，不写入\n"
                    + "INSERT_OR_UPDATE 当主键冲突时，更新相应列\n"
                    + "INSERT_OR_REPLACE 当主键冲突时，更新所有列";
    private static final String WRITE_MODE_DISPLAY = "Write Mode";

    public static final String WRITE_THREAD_SIZE = "connection.writeThreadSize";
    public static final int WRITE_THREAD_SIZE_DEFAULT = 1;
    private static final String WRITE_THREAD_SIZE_DOC = "写入并发线程数（每个并发占用1个数据库连接）";
    private static final String WRITE_THREAD_SIZE_DISPLAY = "Write Thread Size";

    public static final String WRITE_BATCH_SIZE = "connection.writeBatchSize";
    public static final int WRITE_BATCH_SIZE_DEFAULT = 512;
    private static final String WRITE_BATCH_SIZE_DOC = "每个写入线程的最大批次大小（默认512）";
    private static final String WRITE_BATCH_SIZE_DISPLAY = "Write Batch Size";

    public static final String WRITE_BATCH_BYTE_SIZE = "connection.writeBatchByteSize";
    public static final long WRITE_BATCH_BYTE_SIZE_DEFAULT = 2L * 1024L * 1024L;
    private static final String WRITE_BATCH_BYTE_SIZE_DOC = "每个写入线程的最大批次bytes大小，（默认2MB）";
    private static final String WRITE_BATCH_BYTE_SIZE_DISPLAY = "Write Batch Byte Size";

    public static final String WRITE_MAX_INTERVAL_MS = "connection.writeMaxIntervalMs";
    public static final long WRITE_MAX_INTERVAL_MS_DEFAULT = 10000L;
    private static final String WRITE_MAX_INTERVAL_MS_DOC = "距离上次提交超过writeMaxIntervalMs会触发一次批量提交";
    private static final String WRITE_MAX_INTERVAL_MS_DISPLAY = "Write Max Interval Ms";

    public static final String WRITE_FAIL_STRATEGY = "connection.writeFailStrategy";
    public static final String WRITE_FAIL_STRATEGY_DEFAULT = "try_one_by_one";
    private static final String WRITE_FAIL_STRATEGY_DOC =
            "当某一批次提交失败时，会将批次内的记录逐条提交（保序），单条提交失败的记录将会跟随异常被抛出";
    private static final String WRITE_FAIL_STRATEGY_DISPLAY = "Write Fail Strategy";

    public static final String USE_LEGACY_PUT_HANDLER = "connection.useLegacyPutHandler";
    public static final Boolean USE_LEGACY_PUT_HANDLER_DEFAULT = false;
    private static final String USE_LEGACY_PUT_HANDLER_DOC = "不使用unnest，而是使用values()的方式生成攒批sql";
    private static final String USE_LEGACY_PUT_HANDLER_DISPLAY = "USE LEGACY PUT HANDLER";

    public static final String DYNAMIC_PARTITION = "connection.dynamicPartition";
    public static final Boolean DYNAMIC_PARTITION_DEFAULT = false;
    private static final String DYNAMIC_PARTITION_DOC = "写入分区表父表时是否支持自动创建不存在的分区";
    private static final String DYNAMIC_PARTITION_DISPLAY = "SUPPORT DYNAMIC PARTITION";

    public static final String RETRY_COUNT = "connection.retryCount";
    public static final int RETRY_COUNT_DEFAULT = 3;
    private static final String RETRY_COUNT_DOC = "当连接故障时，写入和查询的重试次数";
    private static final String RETRY_COUNT_DISPLAY = "Retry Count";

    public static final String RETRY_SLEEP_INIT_MS = "connection.retrySleepInitMs";
    public static final long RETRY_SLEEP_INIT_MS_DEFAULT = 1000L;
    private static final String RETRY_SLEEP_INIT_MS_DOC =
            "每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs";
    private static final String RETRY_SLEEP_INIT_MS_DISPLAY = "Retry Sleep Init Ms";

    public static final String RETRY_SLEEP_STEP_MS = "connection.retrySleepStepMs";
    public static final long RETRY_SLEEP_STEP_MS_DEFAULT = 10000L;
    private static final String RETRY_SLEEP_STEP_MS_DOC =
            "每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs";
    private static final String RETRY_SLEEP_STEP_MS_DISPLAY = "Retry Sleep Step Ms";

    public static final String CONNECTION_MAX_IDLE_MS = "connection.connectionMaxIdleMs";
    public static final long CONNECTION_MAX_IDLE_MS_DEFAULT = 60000L;
    private static final String CONNECTION_MAX_IDLE_MS_DOC = "写入线程和点查线程数据库连接的最大Idle时间，超过连接将被释放";
    private static final String CONNECTION_MAX_IDLE_MS_DISPLAY = "Connection Max Idle Ms";

    public static final String FIXED_CONNECTION_MODE = "connection.fixedConnectionMode";
    public static final boolean FIXED_CONNECTION_MODE_DEFAULT = false;
    private static final String FIXED_CONNECTION_MODE_DOC =
            "写入和点查不占用连接数（beta功能，需要connector版本>=1.2.0，hologres引擎版本>=1.3）";
    private static final String FIXED_CONNECTION_MODE_DISPLAY = "fixed Connection Mode";

    public static final String COPY_WRITE_MODE = "copyWriteMode";
    public static final boolean COPY_WRITE_MODE_DEFAULT = true;
    private static final String COPY_WRITE_MODE_DOC =
            "copy模式写入，可以使用更少的连接，写入延迟也更小，hologres引擎版本>=1.3.24默认使用次模式";
    private static final String COPY_WRITE_MODE_DISPLAY = "copy Write Mode";

    public static final String COPY_WRITE_DIRTY_DATA_CHECK = "copyWriteDirtyDataCheck";
    public static final boolean COPY_WRITE_DIRTY_DATA_CHECK_DEFAULT = false;
    private static final String COPY_WRITE_DIRTY_DATA_CHECK_DOC =
            "copy模式写入是否进行脏数据校验，打开之后如果有脏数据，可以定位到写入失败的具体行";
    private static final String COPY_WRITE_DIRTY_DATA_CHECK_DISPLAY = "copy Write Dirty Data Check";

    public static final String COPY_WRITE_FORMAT = "copyWriteFormat";
    public static final String COPY_WRITE_FORMAT_DEFAULT = "binary";
    private static final String COPY_WRITE_FORMAT_DOC = "底层是否走二进制协议，二进制会更快，否则为文本模式，默认binary即二进制";
    private static final String COPY_WRITE_FORMAT_DISPLAY = "copy Write Format";

    public static final String COPY_WRITE_DIRECT_CONNECT = "copyWriteDirectConnect";
    public static final boolean COPY_WRITE_DIRECT_CONNECT_DEFAULT = true;
    private static final String COPY_WRITE_DIRECT_CONNECT_DOC =
            "是否直连hologres fe，从而不受vip endpoint的IO吞吐限制，默认根据环境判断能直连则直连";
    private static final String COPY_WRITE_DIRECT_CONNECT_DISPLAY = "copy Write Direct Connect";

    /** CONVERT CONFIG. */
    private static final String CONFIG_CONVERT_GROUP = "Convert";

    public static final String INPUT_FORMAT = "input_format";
    public static final String INPUT_FORMAT_DEFAULT = "string";
    private static final String INPUT_FORMAT_DOC = "消费的数据类型，分为json、struct_json、string三种";
    private static final String INPUT_FORMAT_DISPLAY = "Input Format";

    /** MESSAGE CONFIG. */
    private static final String CONFIG_MESSAGE_GROUP = "Message";

    public static final String WHOLE_MESSAGE_INFO = "whole_message_info";
    public static final boolean WHOLE_MESSAGE_INFO_DEFAULT = true;
    private static final String WHOLE_MESSAGE_INFO_DOC = "是否需要保存完整的topics信息";
    private static final String WHOLE_MESSAGE_INFO_DISPLAY = "SAVE WHOLE TOPICS INFO";

    public static final String MESSAGE_TOPIC = "message_topic";
    public static final String MESSAGE_TOPIC_DEFAULT = "kafkatopic";
    private static final String MESSAGE_TOPIC_DOC = "保存消息Topic时的字段名";
    private static final String MESSAGE_TOPIC_DISPLAY = "Message Topic Fields Name";

    public static final String MESSAGE_PARTITION = "message_partition";
    public static final String MESSAGE_PARTITION_DEFAULT = "kafkapartition";
    private static final String MESSAGE_PARTITION_DOC = "保存消息Partition时的字段名";
    private static final String MESSAGE_PARTITION_DISPLAY = "Message Partition Fields Name";

    public static final String MESSAGE_OFFSET = "message_offset";
    public static final String MESSAGE_OFFSET_DEFAULT = "kafkaoffset";
    private static final String MESSAGE_OFFSET_DOC = "保存消息Offset时的字段名";
    private static final String MESSAGE_OFFSET_DISPLAY = "Message Offset Fields Name";

    public static final String MESSAGE_TIMESTAMP = "message_timestamp";
    public static final String MESSAGE_TIMESTAMP_DEFAULT = "kafkatimestamp";
    private static final String MESSAGE_TIMESTAMP_DOC = "保存消息Timestamp时的字段名";
    private static final String MESSAGE_TIMESTAMP_DISPLAY = "Message Timestamp Fields Name";

    /** INITIAL CONFIG. */
    private static final String INITIAL_GROUP = "Initial";

    public static final String INITIAL_TIMESTAMP = "initial_timestamp";
    public static final long INITIAL_TIMESTAMP_DEFAULT = -1;
    private static final String INITIAL_TIMESTAMP_DOC = "从哪个时间戳开始消费";
    private static final String INITIAL_TIMESTAMP_DISPLAY = "Initial Timestamp";

    /** DIRTY DATA CONFIG. */
    private static final String DIRTY_DATA_GROUP = "DirtyData";

    public static final String DIRTY_DATA_STRATEGY = "dirty_data_strategy";
    public static final String DIRTY_DATA_STRATEGY_DEFAULT = "EXCEPTION";
    private static final String DIRTY_DATA_STRATEGY_DOC = "脏数据处理策略";
    private static final String DIRTY_DATA_STRATEGY_DISPLAY = "Dirty data strategy";

    public static final String DIRTY_DATA_TO_SKIP_ONCE = "dirty_data_to_skip_once";
    public static final String DIRTY_DATA_TO_SKIP_ONCE_DEFAULT = "null,-1,-1";
    private static final String DIRTY_DATA_TO_SKIP_ONCE_DOC = "跳过特定的一条脏数据";
    private static final String DIRTY_DATA_TO_SKIP_ONCE_DISPLAY = "Dirty data skip once";

    public static final String SCHEMA_FORCE_CHECK = "schema_force_check";
    public static final boolean SCHEMA_FORCE_CHECK_DEFAULT = false;
    private static final String SCHEMA_FORCE_CHECK_DOC =
            "是否强制校验holo的schema，false表示出现不存在的字段时，不抛出异常直接忽略";
    private static final String SCHEMA_FORCE_CHECK_DISPLAY = "Dirty data skip once";

    /** Metrics CONFIG. */
    private static final String METRICS_GROUP = "Metrics";

    public static final String METRICS_REPORT_INTERVAL = "metrics_report_interval";
    public static final int METRICS_REPORT_INTERVAL_DEFAULT = 60;
    private static final String METRICS_REPORT_INTERVAL_DOC = "脏数据处理策略";
    private static final String METRICS_REPORT_INTERVAL_DISPLAY = "Dirty data strategy";

    public static ConfigDef config() {
        ConfigDef config = new ConfigDef();
        // CONFIG_CONNECTION_GROUP
        config.define(
                JDBC_URL,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH,
                JDBC_URL_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                JDBC_URL_DISPLAY);

        config.define(
                TABLE,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH,
                TABLE_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                TABLE_DISPLAY);

        config.define(
                USERNAME,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH,
                USERNAME_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                USERNAME_DISPLAY);

        config.define(
                PASSWORD,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH,
                PASSWORD_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                PASSWORD_DISPLAY);

        config.define(
                WRITE_MODE,
                ConfigDef.Type.STRING,
                WRITE_MODE_DEFAULT,
                ConfigDef.Importance.HIGH,
                WRITE_MODE_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                WRITE_MODE_DISPLAY);

        config.define(
                WRITE_BATCH_SIZE,
                ConfigDef.Type.INT,
                WRITE_BATCH_SIZE_DEFAULT,
                ConfigDef.Importance.HIGH,
                WRITE_BATCH_SIZE_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                WRITE_BATCH_SIZE_DISPLAY);

        config.define(
                WRITE_BATCH_BYTE_SIZE,
                ConfigDef.Type.LONG,
                WRITE_BATCH_BYTE_SIZE_DEFAULT,
                ConfigDef.Importance.HIGH,
                WRITE_BATCH_BYTE_SIZE_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                WRITE_BATCH_BYTE_SIZE_DISPLAY);

        config.define(
                WRITE_MAX_INTERVAL_MS,
                ConfigDef.Type.LONG,
                WRITE_MAX_INTERVAL_MS_DEFAULT,
                ConfigDef.Importance.HIGH,
                WRITE_MAX_INTERVAL_MS_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                WRITE_MAX_INTERVAL_MS_DISPLAY);

        config.define(
                WRITE_FAIL_STRATEGY,
                ConfigDef.Type.STRING,
                WRITE_FAIL_STRATEGY_DEFAULT,
                ConfigDef.Importance.HIGH,
                WRITE_FAIL_STRATEGY_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                WRITE_FAIL_STRATEGY_DISPLAY);

        config.define(
                USE_LEGACY_PUT_HANDLER,
                ConfigDef.Type.BOOLEAN,
                USE_LEGACY_PUT_HANDLER_DEFAULT,
                ConfigDef.Importance.HIGH,
                USE_LEGACY_PUT_HANDLER_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                USE_LEGACY_PUT_HANDLER_DISPLAY);

        config.define(
                DYNAMIC_PARTITION,
                ConfigDef.Type.BOOLEAN,
                DYNAMIC_PARTITION_DEFAULT,
                ConfigDef.Importance.HIGH,
                DYNAMIC_PARTITION_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                DYNAMIC_PARTITION_DISPLAY);

        config.define(
                WRITE_THREAD_SIZE,
                ConfigDef.Type.INT,
                WRITE_THREAD_SIZE_DEFAULT,
                ConfigDef.Importance.HIGH,
                WRITE_THREAD_SIZE_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                WRITE_THREAD_SIZE_DISPLAY);

        config.define(
                RETRY_COUNT,
                ConfigDef.Type.INT,
                RETRY_COUNT_DEFAULT,
                ConfigDef.Importance.HIGH,
                RETRY_COUNT_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                RETRY_COUNT_DISPLAY);

        config.define(
                RETRY_SLEEP_INIT_MS,
                ConfigDef.Type.LONG,
                RETRY_SLEEP_INIT_MS_DEFAULT,
                ConfigDef.Importance.HIGH,
                RETRY_SLEEP_INIT_MS_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                RETRY_SLEEP_INIT_MS_DISPLAY);

        config.define(
                RETRY_SLEEP_STEP_MS,
                ConfigDef.Type.LONG,
                RETRY_SLEEP_STEP_MS_DEFAULT,
                ConfigDef.Importance.HIGH,
                RETRY_SLEEP_STEP_MS_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                RETRY_SLEEP_STEP_MS_DISPLAY);

        config.define(
                CONNECTION_MAX_IDLE_MS,
                ConfigDef.Type.LONG,
                CONNECTION_MAX_IDLE_MS_DEFAULT,
                ConfigDef.Importance.HIGH,
                CONNECTION_MAX_IDLE_MS_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                CONNECTION_MAX_IDLE_MS_DISPLAY);

        config.define(
                FIXED_CONNECTION_MODE,
                ConfigDef.Type.BOOLEAN,
                FIXED_CONNECTION_MODE_DEFAULT,
                ConfigDef.Importance.HIGH,
                FIXED_CONNECTION_MODE_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                FIXED_CONNECTION_MODE_DISPLAY);

        config.define(
                COPY_WRITE_MODE,
                ConfigDef.Type.BOOLEAN,
                COPY_WRITE_MODE_DEFAULT,
                ConfigDef.Importance.HIGH,
                COPY_WRITE_MODE_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                COPY_WRITE_MODE_DISPLAY);

        config.define(
                COPY_WRITE_DIRTY_DATA_CHECK,
                ConfigDef.Type.BOOLEAN,
                COPY_WRITE_DIRTY_DATA_CHECK_DEFAULT,
                ConfigDef.Importance.HIGH,
                COPY_WRITE_DIRTY_DATA_CHECK_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                COPY_WRITE_DIRTY_DATA_CHECK_DISPLAY);

        config.define(
                COPY_WRITE_FORMAT,
                ConfigDef.Type.STRING,
                COPY_WRITE_FORMAT_DEFAULT,
                ConfigDef.Importance.HIGH,
                COPY_WRITE_FORMAT_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                COPY_WRITE_FORMAT_DISPLAY);

        config.define(
                COPY_WRITE_DIRECT_CONNECT,
                ConfigDef.Type.BOOLEAN,
                COPY_WRITE_DIRECT_CONNECT_DEFAULT,
                ConfigDef.Importance.HIGH,
                COPY_WRITE_DIRECT_CONNECT_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                COPY_WRITE_DIRECT_CONNECT_DISPLAY);

        // CONFIG_CONVERT_GROUP
        config.define(
                INPUT_FORMAT,
                ConfigDef.Type.STRING,
                INPUT_FORMAT_DEFAULT,
                ConfigDef.Importance.HIGH,
                INPUT_FORMAT_DOC,
                CONFIG_CONVERT_GROUP,
                1,
                ConfigDef.Width.LONG,
                INPUT_FORMAT_DISPLAY);

        // CONFIG_MESSAGE_GROUP
        config.define(
                WHOLE_MESSAGE_INFO,
                ConfigDef.Type.BOOLEAN,
                WHOLE_MESSAGE_INFO_DEFAULT,
                ConfigDef.Importance.HIGH,
                WHOLE_MESSAGE_INFO_DOC,
                CONFIG_MESSAGE_GROUP,
                1,
                ConfigDef.Width.LONG,
                WHOLE_MESSAGE_INFO_DISPLAY);

        config.define(
                MESSAGE_TOPIC,
                ConfigDef.Type.STRING,
                MESSAGE_TOPIC_DEFAULT,
                ConfigDef.Importance.HIGH,
                MESSAGE_TOPIC_DOC,
                CONFIG_MESSAGE_GROUP,
                1,
                ConfigDef.Width.LONG,
                MESSAGE_TOPIC_DISPLAY);

        config.define(
                MESSAGE_PARTITION,
                ConfigDef.Type.STRING,
                MESSAGE_PARTITION_DEFAULT,
                ConfigDef.Importance.HIGH,
                MESSAGE_PARTITION_DOC,
                CONFIG_MESSAGE_GROUP,
                1,
                ConfigDef.Width.LONG,
                MESSAGE_PARTITION_DISPLAY);

        config.define(
                MESSAGE_OFFSET,
                ConfigDef.Type.STRING,
                MESSAGE_OFFSET_DEFAULT,
                ConfigDef.Importance.HIGH,
                MESSAGE_OFFSET_DOC,
                CONFIG_MESSAGE_GROUP,
                1,
                ConfigDef.Width.LONG,
                MESSAGE_OFFSET_DISPLAY);

        config.define(
                MESSAGE_TIMESTAMP,
                ConfigDef.Type.STRING,
                MESSAGE_TIMESTAMP_DEFAULT,
                ConfigDef.Importance.HIGH,
                MESSAGE_TIMESTAMP_DOC,
                CONFIG_MESSAGE_GROUP,
                1,
                ConfigDef.Width.LONG,
                MESSAGE_TIMESTAMP_DISPLAY);

        // CONFIG_INITIAL_GROUP
        config.define(
                INITIAL_TIMESTAMP,
                ConfigDef.Type.LONG,
                INITIAL_TIMESTAMP_DEFAULT,
                ConfigDef.Importance.HIGH,
                INITIAL_TIMESTAMP_DOC,
                INITIAL_GROUP,
                1,
                ConfigDef.Width.LONG,
                INITIAL_TIMESTAMP_DISPLAY);

        // CONFIG_DIRTY_DATA_GROUP
        config.define(
                DIRTY_DATA_STRATEGY,
                ConfigDef.Type.STRING,
                DIRTY_DATA_STRATEGY_DEFAULT,
                ConfigDef.Importance.HIGH,
                DIRTY_DATA_STRATEGY_DOC,
                DIRTY_DATA_GROUP,
                1,
                ConfigDef.Width.LONG,
                DIRTY_DATA_STRATEGY_DISPLAY);

        config.define(
                DIRTY_DATA_TO_SKIP_ONCE,
                ConfigDef.Type.LIST,
                DIRTY_DATA_TO_SKIP_ONCE_DEFAULT,
                ConfigDef.Importance.LOW,
                DIRTY_DATA_TO_SKIP_ONCE_DOC,
                DIRTY_DATA_GROUP,
                1,
                ConfigDef.Width.LONG,
                DIRTY_DATA_TO_SKIP_ONCE_DISPLAY);

        config.define(
                SCHEMA_FORCE_CHECK,
                ConfigDef.Type.BOOLEAN,
                SCHEMA_FORCE_CHECK_DEFAULT,
                ConfigDef.Importance.HIGH,
                SCHEMA_FORCE_CHECK_DOC,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                SCHEMA_FORCE_CHECK_DISPLAY);

        // CONFIG_METRICS_GROUP
        config.define(
                METRICS_REPORT_INTERVAL,
                ConfigDef.Type.INT,
                METRICS_REPORT_INTERVAL_DEFAULT,
                ConfigDef.Importance.HIGH,
                METRICS_REPORT_INTERVAL_DOC,
                METRICS_GROUP,
                1,
                ConfigDef.Width.LONG,
                METRICS_REPORT_INTERVAL_DISPLAY);

        return config;
    }

    public HoloSinkConfig(final Map<?, ?> props) {
        super(config(), props);
    }
}
