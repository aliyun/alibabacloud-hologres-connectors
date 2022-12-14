package com.alibaba.hologres.hive.conf;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.model.WriteFailStrategy;
import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.hologres.hive.utils.JDBCUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

/** generate HoloClientParam from user config or default value. */
public class HoloClientParam {

    private String tableName;
    private String url;
    private String username;
    private String password;

    private final boolean copyWriteMode;
    private final String copyWriteFormat;
    private final boolean copyWriteDirtyDataCheck;
    private boolean copyWriteDirectConnect;

    private final int writeBatchSize;
    private final long writeBatchByteSize;
    private final WriteMode writeMode;
    private final WriteFailStrategy writeFailStrategy;
    private final long writeMaxIntervalMs;
    private final int writeThreadSize;
    private final boolean dynamicPartition;
    private final boolean useLegacyPutHandler;

    private final int readBatchSize;
    private final int readThreadSize;
    private final int readBatchQueueSize;
    private final int scanFetchSize;
    private final int scanTimeoutSeconds;

    private final boolean copyScanMode;

    private final int retryCount;
    private final long retrySleepInitMs;
    private final long retrySleepStepMs;
    private final long connectionMaxIdleMs;
    private final boolean fixedConnectionMode;

    /** when call in initialize, Configuration is none and should get params from Properties. */
    public HoloClientParam(Configuration conf, Properties props) {
        this(conf);
        url = props.getProperty(HoloStorageConfig.JDBC_URL.getPropertyName());
        // the copyWriter just supports jdbc:hologres
        if (url.startsWith("jdbc:postgresql:")) {
            url = "jdbc:hologres:" + url.substring("jdbc:postgresql:".length());
        }
        tableName = props.getProperty(HoloStorageConfig.TABLE.getPropertyName());
        username = props.getProperty(HoloStorageConfig.USERNAME.getPropertyName());
        password = props.getProperty(HoloStorageConfig.PASSWORD.getPropertyName());
    }

    public HoloClientParam(Configuration conf) {
        this.tableName = conf.get(HoloStorageConfig.TABLE.getPropertyName());
        this.url =
                JDBCUtils.formatUrlWithHologres(
                        conf.get(HoloStorageConfig.JDBC_URL.getPropertyName()));
        this.username = conf.get(HoloStorageConfig.USERNAME.getPropertyName());
        this.password = conf.get(HoloStorageConfig.PASSWORD.getPropertyName());

        // copy write options
        this.copyWriteMode =
                conf.getBoolean(HoloStorageConfig.COPY_WRITE_MODE.getPropertyName(), true);
        this.copyWriteFormat =
                conf.get(HoloStorageConfig.COPY_WRITE_FORMAT.getPropertyName(), "binary");
        this.copyWriteDirtyDataCheck =
                conf.getBoolean(
                        HoloStorageConfig.COPY_WRITE_DIRTY_DATA_CHECK.getPropertyName(), false);
        this.copyWriteDirectConnect =
                conf.getBoolean(
                        HoloStorageConfig.COPY_WRITE_DIRECT_CONNECT.getPropertyName(), true);

        // write options
        this.writeBatchSize =
                conf.getInt(HoloStorageConfig.WRITE_BATCH_SIZE.getPropertyName(), 512);
        this.writeBatchByteSize =
                conf.getLong(
                        HoloStorageConfig.WRITE_BATCH_BYTE_SIZE.getPropertyName(),
                        2L * 1024L * 1024L);
        this.writeMaxIntervalMs =
                conf.getLong(HoloStorageConfig.WRITE_MAX_INTERVAL_MS.getPropertyName(), 10000L);
        this.writeThreadSize =
                conf.getInt(HoloStorageConfig.WRITE_THREAD_SIZE.getPropertyName(), 1);
        this.dynamicPartition =
                conf.getBoolean(HoloStorageConfig.DYNAMIC_PARTITION.getPropertyName(), false);
        this.useLegacyPutHandler =
                conf.getBoolean(HoloStorageConfig.USE_LEGACY_PUT_HANDLER.getPropertyName(), false);

        String wMode =
                conf.get(HoloStorageConfig.WRITE_MODE.getPropertyName(), "INSERT_OR_REPLACE")
                        .toLowerCase();
        switch (wMode) {
            case "insert_or_replace":
                writeMode = WriteMode.INSERT_OR_REPLACE;
                break;
            case "insert_or_ignore":
                writeMode = WriteMode.INSERT_OR_IGNORE;
                break;
            case "insert_or_update":
                writeMode = WriteMode.INSERT_OR_UPDATE;
                break;
            default:
                throw new IllegalArgumentException("Could not recognize writeMode " + wMode);
        }

        String wFailStrategy =
                conf.get(HoloStorageConfig.WRITE_FAIL_STRATEGY.getPropertyName(), "TRY_ONE_BY_ONE")
                        .toLowerCase();
        switch (wFailStrategy) {
            case "try_one_by_one":
                writeFailStrategy = WriteFailStrategy.TRY_ONE_BY_ONE;
                break;
            case "none":
                writeFailStrategy = WriteFailStrategy.NONE;
                break;
            default:
                throw new IllegalArgumentException(
                        "Could not recognize writeFailStrategy " + wFailStrategy);
        }

        // read options
        this.readBatchSize = conf.getInt(HoloStorageConfig.READ_BATCH_SIZE.getPropertyName(), 128);
        this.readThreadSize = conf.getInt(HoloStorageConfig.READ_THREAD_SIZE.getPropertyName(), 1);
        this.readBatchQueueSize =
                conf.getInt(HoloStorageConfig.READ_BATCH_QUEUE_SIZE.getPropertyName(), 256);
        this.scanFetchSize = conf.getInt(HoloStorageConfig.SCAN_FETCH_SIZE.getPropertyName(), 2000);
        this.scanTimeoutSeconds =
                conf.getInt(HoloStorageConfig.SCAN_TIMEOUT_SECONDS.getPropertyName(), 60);
        this.copyScanMode =
                conf.getBoolean(HoloStorageConfig.COPY_SCAN_MODE.getPropertyName(), false)
                        || conf.getBoolean(HoloStorageConfig.COPY_MODE.getPropertyName(), false);

        // else options
        this.retryCount = conf.getInt(HoloStorageConfig.RETRY_COUNT.getPropertyName(), 3);
        this.retrySleepInitMs =
                conf.getLong(HoloStorageConfig.RETRY_SLEEP_INIT_MS.getPropertyName(), 1000L);
        this.retrySleepStepMs =
                conf.getLong(HoloStorageConfig.RETRY_SLEEP_STEP_MS.getPropertyName(), 10000L);
        this.connectionMaxIdleMs =
                conf.getLong(HoloStorageConfig.CONNECTION_MAX_IDLE_MS.getPropertyName(), 60000L);
        this.fixedConnectionMode =
                conf.getBoolean(HoloStorageConfig.FIXED_CONNECTION_MODE.getPropertyName(), false);
    }

    public HoloConfig generateHoloConfig() {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("Table name should be defined");
        }
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("Url should be defined");
        }
        if (username == null || username.isEmpty()) {
            throw new IllegalArgumentException("username should be defined");
        }
        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException("password should be defined");
        }
        HoloConfig holoConfig = new HoloConfig();

        holoConfig.setJdbcUrl(url);
        holoConfig.setUsername(username);
        holoConfig.setPassword(password);
        holoConfig.setWriteMode(writeMode);
        holoConfig.setWriteFailStrategy(writeFailStrategy);
        holoConfig.setWriteBatchSize(writeBatchSize);
        holoConfig.setWriteBatchByteSize(writeBatchByteSize);
        holoConfig.setWriteMaxIntervalMs(writeMaxIntervalMs);
        holoConfig.setWriteThreadSize(writeThreadSize);
        holoConfig.setDynamicPartition(dynamicPartition);
        holoConfig.setUseLegacyPutHandler(useLegacyPutHandler);
        holoConfig.setUseFixedFe(fixedConnectionMode);

        holoConfig.setRetryCount(retryCount);
        holoConfig.setRetrySleepInitMs(retrySleepInitMs);
        holoConfig.setRetrySleepStepMs(retrySleepStepMs);
        holoConfig.setConnectionMaxIdleMs(connectionMaxIdleMs);

        holoConfig.setReadBatchSize(readBatchSize);
        holoConfig.setReadThreadSize(readThreadSize);
        holoConfig.setReadBatchQueueSize(readBatchQueueSize);
        holoConfig.setScanFetchSize(scanFetchSize);
        holoConfig.setScanTimeoutSeconds(scanTimeoutSeconds);

        holoConfig.setAppName("hologres-connector-hive");

        return holoConfig;
    }

    public String getTableName() {
        return tableName;
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public WriteMode getWriteMode() {
        return writeMode;
    }

    public String getCopyWriteFormat() {
        return copyWriteFormat;
    }

    public boolean isCopyWriteMode() {
        return copyWriteMode;
    }

    public boolean isCopyWriteDirtyDataCheck() {
        return copyWriteDirtyDataCheck;
    }

    public boolean isCopyWriteDirectConnect() {
        return copyWriteDirectConnect;
    }

    public void setCopyWriteDirectConnect(boolean copyWriteDirectConnect) {
        this.copyWriteDirectConnect = copyWriteDirectConnect;
    }

    public boolean isCopyScanMode() {
        return copyScanMode;
    }
}
