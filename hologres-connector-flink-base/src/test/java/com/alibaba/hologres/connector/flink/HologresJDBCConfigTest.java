package com.alibaba.hologres.connector.flink;

import org.apache.flink.configuration.Configuration;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.connector.flink.config.HologresConfigs;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.config.JDBCOptions;
import com.alibaba.hologres.connector.flink.config.WriteMode;
import com.alibaba.hologres.connector.flink.jdbc.HologresJDBCClientProvider;
import com.alibaba.hologres.connector.flink.utils.HologresUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/** HologresJDBCRecordReaderWriterTest. */
@RunWith(PowerMockRunner.class)
@PrepareForTest({HologresUtils.class})
public class HologresJDBCConfigTest {
    public HologresJDBCConfigTest() {
        PowerMockito.mockStatic(HologresUtils.class);
        PowerMockito.when(
                        HologresUtils.chooseBestWriteMode(
                                ArgumentMatchers.any(JDBCOptions.class),
                                ArgumentMatchers.anyBoolean()))
                .thenReturn(WriteMode.INSERT);
    }

    @Test
    public void testDefaultHologresJDBCHoloClientConfig() {
        Configuration configuration = new Configuration();

        // common
        String mutateType = "InsertOrUpdate";
        boolean dynamicPartition = false;

        // jdbc connection
        int connectionPoolSize = 3;
        int retryCount = 10;
        long retrySleepStepMs = 5000L;
        long retrySleepInitMs = 1000L;
        long connectionMaxIdleMs = 60000L;
        long metaCacheTTL = 60000L;
        boolean fixedConnectionMode = false;

        // jdbc read
        int readBatchSize = 128;
        int readBatchQueueSize = 256;
        int scanFetchSize = 256;
        int scanTimeoutSeconds = 28800;

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
        assertEquals(config.getOnConflictAction(), getOnConflictAction(mutateType));

        assertEquals(config.getWriteThreadSize(), connectionPoolSize);
        assertEquals(config.getReadThreadSize(), connectionPoolSize);
        assertEquals(config.isUseFixedFe(), fixedConnectionMode);

        assertEquals(config.getRetryCount(), retryCount);
        assertEquals(config.getRetrySleepInitMs(), retrySleepInitMs);
        assertEquals(config.getRetrySleepStepMs(), retrySleepStepMs);
        assertEquals(config.getConnectionMaxIdleMs(), connectionMaxIdleMs);
        assertEquals(config.getMetaCacheTTL(), metaCacheTTL);
        assertEquals(config.getMetaAutoRefreshFactor(), 4);

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

        // common
        String mutateType = "InsertOrUpdate";
        boolean dynamicPartition = true;
        configuration.set(HologresConfigs.ON_CONFLICT_ACTION, mutateType);
        configuration.set(HologresConfigs.CREATE_MISSING_PARTITION_TABLE, dynamicPartition);

        // jdbc connection
        int connectionPoolSize = 4;
        int retryCount = 8;
        long retrySleepStepMs = 10123L;
        long connectionMaxIdleMs = 60123L;
        long metaCacheTTL = 60234L;
        boolean fixedConnectionMode = true;
        configuration.set(HologresConfigs.CONNECTION_POOL_SIZE, connectionPoolSize);
        configuration.set(HologresConfigs.JDBC_RETRY_COUNT, retryCount);
        configuration.set(HologresConfigs.RETRY_SLEEP_STEP_MS, retrySleepStepMs);
        configuration.set(HologresConfigs.JDBC_CONNECTION_MAX_IDLE_MS, connectionMaxIdleMs);
        configuration.set(HologresConfigs.JDBC_META_CACHE_TTL, metaCacheTTL);
        configuration.set(HologresConfigs.CONNECTION_FIXED, fixedConnectionMode);

        // jdbc read
        int readBatchSize = 98765;
        int readBatchQueueSize = 8765;
        int scanFetchSize = 765;
        int scanTimeoutSeconds = 65;
        configuration.set(HologresConfigs.READ_BATCH_SIZE, readBatchSize);
        configuration.set(HologresConfigs.READ_BATCH_QUEUE_SIZE, readBatchQueueSize);
        configuration.set(HologresConfigs.SCAN_FETCH_SIZE, scanFetchSize);
        configuration.set(HologresConfigs.SCAN_TIMEOUT_SECONDS, scanTimeoutSeconds);

        // jdbc write
        int writeBatchSize = 123;
        long writeBatchByteSize = 1234L;
        long writeBatchTotalByteSize = 12345L;
        long writeMaxIntervalMs = 123456L;
        boolean useLegacyPutHandler = true;
        configuration.set(HologresConfigs.WRITE_BATCH_SIZE, writeBatchSize);
        configuration.set(HologresConfigs.WRITE_BATCH_BYTE_SIZE, writeBatchByteSize);
        configuration.set(HologresConfigs.WRITE_BATCH_TOTAL_BYTE_SIZE, writeBatchTotalByteSize);
        configuration.set(HologresConfigs.WRITE_FLUSH_INTERVAL, writeMaxIntervalMs);
        configuration.set(HologresConfigs.WRITE_USE_LEGACY_PUT_HANDLER, useLegacyPutHandler);

        // generateHoloConfig
        HologresConnectionParam param = new HologresConnectionParam(configuration);
        HologresJDBCClientProvider hologresJDBCClientProvider =
                new HologresJDBCClientProvider(param);
        HoloConfig config = hologresJDBCClientProvider.generateHoloConfig();

        // compare HoloConfig
        assertEquals(config.getOnConflictAction(), getOnConflictAction(mutateType));
        assertEquals(config.isDynamicPartition(), dynamicPartition);

        assertEquals(config.getRetryCount(), retryCount);
        assertEquals(config.getRetrySleepStepMs(), retrySleepStepMs);
        assertEquals(config.getConnectionMaxIdleMs(), connectionMaxIdleMs);
        assertEquals(config.getMetaCacheTTL(), metaCacheTTL);
        assertEquals(config.isUseFixedFe(), fixedConnectionMode);

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
    }

    @Test
    public void testDefaultConnectionParam() {
        Configuration configuration = new Configuration();
        HologresConnectionParam param = new HologresConnectionParam(configuration);
        assertEquals(
                "HologresConnectionParam{\n"
                        + "\tJDBC Options=JDBCOptions{database='null', table='null', username='null', password='********', endpoint='null', "
                        + "connection.ssl.mode='DISABLE', connection.ssl.root-cert.location='null', enableAkv4='false', akv4Region='null'},\n"
                        + "\tconnection.pool.size=3,\n"
                        + "\tconnection.fixed=false,\n"
                        + "\tconnection.direct=false,\n"
                        + "\tretry-count=10,\n"
                        + "\tretry-sleep-step-ms=5000,\n"
                        + "\tconnection.max-idle-ms=60000,\n"
                        + "\tconnection.meta-cache-ttl-ms=60000,\n"
                        + "\tsource.scan.fetch-size=256,\n"
                        + "\tsource.scan.timeout-seconds=28800,\n"
                        + "\tsource.filter-push-down.enabled=false,\n"
                        + "\tsink.write-mode=INSERT,\n"
                        + "\tsink.ignore-delete=true,\n"
                        + "\tsink.create-missing-partition=false,\n"
                        + "\tsink.ignore-null-when-update.enabled=false,\n"
                        + "\tsink.remove-u0000-in-text.enabled=true,\n"
                        + "\tsink.deduplication.enabled=true,\n"
                        + "\tsink.aggressive.enabled=false,\n"
                        + "\tsink.on-conflict-action=INSERT_OR_UPDATE,\n"
                        + "\tsink.insert.batch-size=256,\n"
                        + "\tsink.insert.batch-byte-size=2097152,\n"
                        + "\tsink.insert.batch-total-byte-size=20971520,\n"
                        + "\tsink.insert.flush-interval-ms=10000,\n"
                        + "\tsink.insert.legacy-put-handler=false,\n"
                        + "\tsink.insert.dirty-data-strategy=EXCEPTION,\n"
                        + "\tsink.check-and-put.condition=null,\n"
                        + "\tsink.insert-expression=conflictUpdateSet:null\n"
                        + "conflictWhere:null,\n"
                        + "\tsink.hold-on-update-before.enabled=false,\n"
                        + "\tsink.reshuffle-by-holo-distribution-key.enabled=false,\n"
                        + "\tsink.copy.format=binary,\n"
                        + "\tsink.affect-rows.enabled=false,\n"
                        + "\tsink.dirty-data-check.enabled=true,\n"
                        + "\tsink.multi-table.datatype-tolerant.enabled=false,\n"
                        + "\tlookup.insert-if-not-exists=false,\n"
                        + "\tlookup.read.batch-size=128,\n"
                        + "\tlookup.read.batch-queue-size=256,\n"
                        + "\tserverless-computing.enabled=false,\n"
                        + "\tserverless-computing.query-priority=3,\n"
                        + "\tstatement-timeout-seconds=28800,\n"
                        + "}\n",
                param.toString());
    }

    private OnConflictAction getOnConflictAction(String onConflictAction) {
        switch (onConflictAction.toLowerCase()) {
            case "insertorignore":
                return OnConflictAction.INSERT_OR_IGNORE;
            case "insertorreplace":
                return OnConflictAction.INSERT_OR_REPLACE;
            case "insertorupdate":
                return OnConflictAction.INSERT_OR_UPDATE;
            default:
                throw new RuntimeException("Invalid upsert type: " + onConflictAction);
        }
    }
}
