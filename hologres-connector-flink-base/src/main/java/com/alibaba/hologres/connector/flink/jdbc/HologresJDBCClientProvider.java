package com.alibaba.hologres.connector.flink.jdbc;

import org.apache.flink.annotation.VisibleForTesting;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.ExecutionPool;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.config.JDBCOptions;

import java.util.Objects;

/** HoloClient factory which supports create holo client based on ak or sts. */
public class HologresJDBCClientProvider {
    private HologresConnectionParam param;
    private JDBCOptions jdbcOptions;
    private HoloClient client;

    public HologresJDBCClientProvider(HologresConnectionParam param) {
        this.param = param;
        this.jdbcOptions = param.getJdbcOptions();
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

    public HoloClient getClient() {
        if (client != null) {
            return client;
        }
        try {
            HoloConfig holoConfig = generateHoloConfig();
            client = new HoloClient(holoConfig);
            // use pool name to get share connection pool.
            if (param.getConnectionPoolName() != null) {
                ExecutionPool pool = ExecutionPool.buildOrGet(getUniquePoolName(), holoConfig);
                client.setPool(pool);
            }
            return client;
        } catch (HoloClientException e) {
            throw new RuntimeException("Fail to create holo client", e);
        }
    }

    @VisibleForTesting
    public HoloConfig generateHoloConfig() {
        HoloConfig holoConfig = new HoloConfig();
        holoConfig.setJdbcUrl(jdbcOptions.getDbUrl());
        holoConfig.setAppName("hologres-connector-flink");
        holoConfig.setUsername(jdbcOptions.getUsername());
        holoConfig.setPassword(jdbcOptions.getPassword());
        holoConfig.setUseAKv4(jdbcOptions.isEnableAkv4());
        if (jdbcOptions.isEnableAkv4()) {
            holoConfig.setRegion(jdbcOptions.getAkv4Region());
        }
        // connection config
        holoConfig.setRetryCount(param.getJdbcRetryCount());
        holoConfig.setRetrySleepStepMs(param.getJdbcRetrySleepStepMs());
        holoConfig.setConnectionMaxIdleMs(param.getJdbcConnectionMaxIdleMs());
        holoConfig.setMetaCacheTTL(param.getJdbcMetaCacheTTL());
        holoConfig.setUseFixedFe(param.isEnableFixedConnectionMode());
        holoConfig.setSslMode(jdbcOptions.getSslMode());
        holoConfig.setSslRootCertLocation(jdbcOptions.getSslRootCertLocation());
        holoConfig.setEnableDirectConnection(param.isDirectConnect());
        holoConfig.setEnableAffectedRows(param.isEnableAffectedRows());

        // reader config
        holoConfig.setReadThreadSize(param.getConnectionPoolSize());
        holoConfig.setReadBatchSize(param.getJdbcReadBatchSize());
        holoConfig.setReadBatchQueueSize(param.getJdbcReadBatchQueueSize());
        holoConfig.setScanFetchSize(param.getScanFetchSize());
        holoConfig.setScanTimeoutSeconds(param.getScanTimeoutSeconds());
        holoConfig.setReadRetryCount(param.getJdbcRetryCount());

        // writer config
        holoConfig.setWriteThreadSize(param.getConnectionPoolSize());
        holoConfig.setWriteBatchSize(param.getWriteBatchSize());
        holoConfig.setWriteBatchByteSize(param.getWriteBatchByteSize());
        holoConfig.setWriteBatchTotalByteSize(param.getWriteBatchTotalByteSize());
        holoConfig.setWriteMaxIntervalMs(param.getWriteFlushInterval());
        holoConfig.setUseLegacyPutHandler(param.isUseLegacyPutHandler());
        holoConfig.setEnableDefaultForNotNullColumn(true);
        holoConfig.setOnConflictAction(param.getOnConflictAction());
        holoConfig.setEnableDeduplication(param.isEnableDeduplication());
        holoConfig.setRemoveU0000InTextColumnValue(param.isEnableRemoveU0000InText());
        holoConfig.setEnableAggressive(param.isEnableAggressive());
        holoConfig.setDynamicPartition(param.isCreateMissingPartTable());

        return holoConfig;
    }

    protected String getUniquePoolName() {
        // Different databases or even different instances may be used in user jobs. An exception
        // may occur if the same pool name is used.
        JDBCOptions jdbcOptions = param.getJdbcOptions();
        return "ExecutionPool-"
                + Objects.hash(
                        jdbcOptions.getEndpoint(),
                        jdbcOptions.getDatabase(),
                        jdbcOptions.getUsername(),
                        jdbcOptions.getPassword(),
                        jdbcOptions.getSslMode(),
                        jdbcOptions.getSslRootCertLocation())
                + param.getConnectionPoolName();
    }
}
