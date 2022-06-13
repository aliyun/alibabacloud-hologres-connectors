package com.alibaba.ververica.connectors.hologres;

import org.apache.flink.configuration.Configuration;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.ververica.connectors.hologres.config.HologresConfigs;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCClientProvider;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCConfigs;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/** HologresJDBCRecordReaderWriterTest. */
public class HologresJDBCConfigTest extends HologresTestBase {
    public HologresJDBCConfigTest() throws IOException {}

    @Test
    public void testDefaultHologresJDBCHoloClientConfig() {
        Configuration configuration = new Configuration();
        configuration.set(HologresConfigs.ENDPOINT, this.endpoint);
        configuration.set(HologresConfigs.DATABASE, this.database);
        configuration.set(HologresConfigs.USERNAME, this.username);
        configuration.set(HologresConfigs.PASSWORD, this.password);
        configuration.set(HologresConfigs.TABLE, this.sinkTable);

        // common
        String mutateType = "InsertOrIgnore";
        boolean dynamicPartition = false;

        // jdbc connection
        int connectionPoolSize = 3;
        int retryCount = 10;
        long retrySleepStepMs = 5000L;
        long retrySleepInitMs = 1000L;
        long connectionMaxIdleMs = 60000L;
        long metaCacheTTL = 60000L;
        int metaAutoRefreshFactor = -1;

        // jdbc read
        int readBatchSize = 128;
        int readBatchQueueSize = 256;
        int scanFetchSize = 256;
        int scanTimeoutSeconds = 256;

        // jdbc write
        int writeBatchSize = 256;
        long writeBatchByteSize = 2097152L;
        long writeBatchTotalByteSize = 20971520L;
        long writeMaxIntervalMs = 10000L;
        boolean useLegacyPutHandler = false;
        boolean enableDefaultForNotNullColumn = true;

        // generateHoloConfig
        HologresConnectionParam param = new HologresConnectionParam(configuration);
        HologresJDBCClientProvider hologresJDBCClientProvider =
                new HologresJDBCClientProvider(param);
        HoloConfig config = hologresJDBCClientProvider.generateHoloConfig();

        // compare HoloConfig
        assertEquals(config.getWriteMode(), getJDBCWriteMode(mutateType));

        assertEquals(config.getWriteThreadSize(), connectionPoolSize);
        assertEquals(config.getReadThreadSize(), connectionPoolSize);

        assertEquals(config.getRetryCount(), retryCount);
        assertEquals(config.getRetrySleepInitMs(), retrySleepInitMs);
        assertEquals(config.getRetrySleepStepMs(), retrySleepStepMs);
        assertEquals(config.getConnectionMaxIdleMs(), connectionMaxIdleMs);
        assertEquals(config.getMetaCacheTTL(), metaCacheTTL);
        assertEquals(config.getMetaAutoRefreshFactor(), metaAutoRefreshFactor);

        assertEquals(config.getReadBatchSize(), readBatchSize);
        assertEquals(config.getReadBatchQueueSize(), readBatchQueueSize);
        assertEquals(config.getScanFetchSize(), scanFetchSize);
        assertEquals(config.getScanTimeoutSeconds(), scanTimeoutSeconds);

        assertEquals(config.getWriteBatchByteSize(), writeBatchByteSize);
        assertEquals(config.getWriteBatchTotalByteSize(), writeBatchTotalByteSize);
        assertEquals(config.getWriteBatchSize(), writeBatchSize);
        assertEquals(config.getWriteMaxIntervalMs(), writeMaxIntervalMs);
        assertEquals(config.isUseLegacyPutHandler(), useLegacyPutHandler);
        assertEquals(config.isEnableDefaultForNotNullColumn(), enableDefaultForNotNullColumn);
        assertEquals(config.isDynamicPartition(), dynamicPartition);
    }

    @Test
    public void testHologresJDBCHoloClientConfig() {
        Configuration configuration = new Configuration();
        configuration.set(HologresConfigs.ENDPOINT, this.endpoint);
        configuration.set(HologresConfigs.DATABASE, this.database);
        configuration.set(HologresConfigs.USERNAME, this.username);
        configuration.set(HologresConfigs.PASSWORD, this.password);
        configuration.set(HologresConfigs.TABLE, this.sinkTable);

        // common
        String mutateType = "InsertOrUpdate";
        boolean dynamicPartition = true;
        configuration.set(HologresConfigs.MUTATE_TYPE, mutateType);
        configuration.set(HologresConfigs.CREATE_MISSING_PARTITION_TABLE, dynamicPartition);

        // jdbc connection
        int connectionPoolSize = 4;
        int retryCount = 8;
        long retrySleepStepMs = 10123L;
        long retrySleepInitMs = 10234L;
        long connectionMaxIdleMs = 60123L;
        long metaCacheTTL = 60234L;
        int metaAutoRefreshFactor = 6;
        configuration.set(
                HologresJDBCConfigs.OPTIONAL_CLIENT_CONNECTION_POOL_SIZE, connectionPoolSize);
        configuration.set(HologresJDBCConfigs.OPTIONAL_JDBC_RETRY_COUNT, retryCount);
        configuration.set(HologresJDBCConfigs.OPTIONAL_RETRY_SLEEP_INIT_MS, retrySleepInitMs);
        configuration.set(HologresJDBCConfigs.OPTIONAL_RETRY_SLEEP_STEP_MS, retrySleepStepMs);
        configuration.set(
                HologresJDBCConfigs.OPTIONAL_JDBC_CONNECTION_MAX_IDLE_MS, connectionMaxIdleMs);
        configuration.set(HologresJDBCConfigs.OPTIONAL_JDBC_META_CACHE_TTL, metaCacheTTL);
        configuration.set(
                HologresJDBCConfigs.OPTIONAL_JDBC_META_AUTO_REFRESH_FACTOR, metaAutoRefreshFactor);

        // jdbc read
        int readBatchSize = 98765;
        int readBatchQueueSize = 8765;
        int scanFetchSize = 765;
        int scanTimeoutSeconds = 65;
        configuration.set(HologresJDBCConfigs.OPTIONAL_JDBC_READ_BATCH_SIZE, readBatchSize);
        configuration.set(
                HologresJDBCConfigs.OPTIONAL_JDBC_READ_BATCH_QUEUE_SIZE, readBatchQueueSize);
        configuration.set(HologresJDBCConfigs.OPTIONAL_JDBC_SCAN_FETCH_SIZE, scanFetchSize);
        configuration.set(
                HologresJDBCConfigs.OPTIONAL_JDBC_SCAN_TIMEOUT_SECONDS, scanTimeoutSeconds);

        // jdbc write
        int writeBatchSize = 123;
        long writeBatchByteSize = 1234L;
        long writeBatchTotalByteSize = 12345L;
        long writeMaxIntervalMs = 123456L;
        boolean useLegacyPutHandler = true;
        boolean enableDefaultForNotNullColumn = false;
        configuration.set(HologresJDBCConfigs.OPTIONAL_JDBC_WRITE_BATCH_SIZE, writeBatchSize);
        configuration.set(
                HologresJDBCConfigs.OPTIONAL_JDBC_WRITE_BATCH_BYTE_SIZE, writeBatchByteSize);
        configuration.set(
                HologresJDBCConfigs.OPTIONAL_JDBC_WRITE_BATCH_TOTAL_BYTE_SIZE,
                writeBatchTotalByteSize);
        configuration.set(
                HologresJDBCConfigs.OPTIONAL_JDBC_WRITE_FLUSH_INTERVAL, writeMaxIntervalMs);
        configuration.set(
                HologresJDBCConfigs.OPTIONAL_JDBC_USE_LEGACY_PUT_HANDLER, useLegacyPutHandler);
        configuration.set(
                HologresJDBCConfigs.OPTIONAL_JDBC_ENABLE_DEFAULT_FOR_NOT_NULL_COLUMN,
                enableDefaultForNotNullColumn);

        // generateHoloConfig
        HologresConnectionParam param = new HologresConnectionParam(configuration);
        HologresJDBCClientProvider hologresJDBCClientProvider =
                new HologresJDBCClientProvider(param);
        HoloConfig config = hologresJDBCClientProvider.generateHoloConfig();

        // compare HoloConfig
        assertEquals(config.getWriteMode(), getJDBCWriteMode(mutateType));
        assertEquals(config.isDynamicPartition(), dynamicPartition);

        assertEquals(config.getRetryCount(), retryCount);
        assertEquals(config.getRetrySleepInitMs(), retrySleepInitMs);
        assertEquals(config.getRetrySleepStepMs(), retrySleepStepMs);
        assertEquals(config.getConnectionMaxIdleMs(), connectionMaxIdleMs);
        assertEquals(config.getMetaCacheTTL(), metaCacheTTL);
        assertEquals(config.getMetaAutoRefreshFactor(), metaAutoRefreshFactor);

        assertEquals(config.getReadThreadSize(), connectionPoolSize);
        assertEquals(config.getReadBatchSize(), readBatchSize);
        assertEquals(config.getReadBatchQueueSize(), readBatchQueueSize);
        assertEquals(config.getScanFetchSize(), scanFetchSize);
        assertEquals(config.getScanTimeoutSeconds(), scanTimeoutSeconds);

        assertEquals(config.getWriteThreadSize(), connectionPoolSize);
        assertEquals(config.getWriteBatchSize(), writeBatchSize);
        assertEquals(config.getWriteBatchByteSize(), writeBatchByteSize);
        assertEquals(config.getWriteBatchTotalByteSize(), writeBatchTotalByteSize);
        assertEquals(config.getWriteMaxIntervalMs(), writeMaxIntervalMs);
        assertEquals(config.isUseLegacyPutHandler(), useLegacyPutHandler);
        assertEquals(config.isEnableDefaultForNotNullColumn(), enableDefaultForNotNullColumn);
    }

    public WriteMode getJDBCWriteMode(String jdbcWriteMode) {
        switch (jdbcWriteMode.toLowerCase()) {
            case "insertorignore":
                return WriteMode.INSERT_OR_IGNORE;
            case "insertorreplace":
                return WriteMode.INSERT_OR_REPLACE;
            case "insertorupdate":
                return WriteMode.INSERT_OR_UPDATE;
            default:
                throw new RuntimeException("Invalid upsert type: " + jdbcWriteMode);
        }
    }
}
