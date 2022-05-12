package com.alibaba.hologres.hive;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteFailStrategy;
import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.hologres.hive.conf.HoloStorageConfig;
import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

/** HoloClient factory which supports create holo client. */
public class HoloClientProvider {

    private HoloClient client;
    private String tableName;
    private String url;
    private String username;
    private String password;

    private final int writeBatchSize;
    private final long writeBatchByteSize;
    private final WriteMode writeMode;
    private final WriteFailStrategy writeFailStrategy;
    private final long writeMaxIntervalMs;
    private final int writeThreadSize;
    private final boolean dynamicPartition;

    private final int readBatchSize;
    private final int readThreadSize;
    private final int readBatchQueueSize;
    private final int scanFetchSize;
    private final int scanTimeoutSeconds;
    private final boolean copyMode;

    private final int retryCount;
    private final long retrySleepInitMs;
    private final long retrySleepStepMs;
    private final long connectionMaxIdleMs;

    /** when call in initialize, Configuration is none and should get params from Properties. */
    public HoloClientProvider(Configuration conf, Properties props) {
        this(conf);
        url = props.getProperty(HoloStorageConfig.JDBC_URL.getPropertyName());
        tableName = props.getProperty(HoloStorageConfig.TABLE.getPropertyName());
        username = props.getProperty(HoloStorageConfig.USERNAME.getPropertyName());
        password = props.getProperty(HoloStorageConfig.PASSWORD.getPropertyName());
    }

    public HoloClientProvider(Configuration conf) {
        this.tableName = conf.get(HoloStorageConfig.TABLE.getPropertyName());
        this.url = conf.get(HoloStorageConfig.JDBC_URL.getPropertyName());
        this.username = conf.get(HoloStorageConfig.USERNAME.getPropertyName());
        this.password = conf.get(HoloStorageConfig.PASSWORD.getPropertyName());

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
        this.copyMode = conf.getBoolean(HoloStorageConfig.COPY_MODE.getPropertyName(), false);

        // else options
        this.retryCount = conf.getInt(HoloStorageConfig.RETRY_COUNT.getPropertyName(), 3);
        this.retrySleepInitMs =
                conf.getLong(HoloStorageConfig.RETRY_SLEEP_INIT_MS.getPropertyName(), 1000L);
        this.retrySleepStepMs =
                conf.getLong(HoloStorageConfig.RETRY_SLEEP_STEP_MS.getPropertyName(), 10000L);
        this.connectionMaxIdleMs =
                conf.getLong(HoloStorageConfig.CONNECTION_MAX_IDLE_MS.getPropertyName(), 60000L);
    }

    public void closeClient() {
        if (client == null) {
            return;
        }
        try {
            client.flush();
        } catch (HoloClientException e) {
            throw new RuntimeException("Failed to close client", e);
        } finally {
            client.close();
        }
        client = null;
    }

    public HoloClient createOrGetClient() throws HoloClientException {
        if (client == null) {
            HoloConfig holoConfig = generateHoloConfig();

            try {
                client = new HoloClient(holoConfig);
            } catch (HoloClientException e) {
                throw e;
            }
        }
        return client;
    }

    protected HoloConfig generateHoloConfig() {
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

        holoConfig.setRetryCount(retryCount);
        holoConfig.setRetrySleepInitMs(retrySleepInitMs);
        holoConfig.setRetrySleepStepMs(retrySleepStepMs);
        holoConfig.setConnectionMaxIdleMs(connectionMaxIdleMs);

        holoConfig.setReadBatchSize(readBatchSize);
        holoConfig.setReadThreadSize(readThreadSize);
        holoConfig.setReadBatchQueueSize(readBatchQueueSize);
        holoConfig.setScanFetchSize(scanFetchSize);
        holoConfig.setScanTimeoutSeconds(scanTimeoutSeconds);

        return holoConfig;
    }

    public TableSchema getTableSchema() throws HoloClientException {
        if (client == null) {
            createOrGetClient();
        }
        try {
            return client.getTableSchema(TableName.valueOf(tableName));
        } catch (HoloClientException e) {
            throw e;
        }
    }

    public boolean isCopyMode() {
        return copyMode;
    }
}
