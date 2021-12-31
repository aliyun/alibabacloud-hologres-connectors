package com.alibaba.ververica.connectors.hologres.jdbc;

import org.apache.flink.annotation.VisibleForTesting;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.config.JDBCOptions;

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
            return client;
        } catch (HoloClientException e) {
            throw new RuntimeException("Fail to create holo client", e);
        }
    }

    @VisibleForTesting
    public HoloConfig generateHoloConfig() {
        HoloConfig holoConfig = new HoloConfig();
        holoConfig.setJdbcUrl(jdbcOptions.getDbUrl());
        holoConfig.setUsername(jdbcOptions.getUsername());
        holoConfig.setPassword(jdbcOptions.getPassword());

        // connection config
        holoConfig.setRetryCount(param.getJdbcRetryCount());
        holoConfig.setRetrySleepInitMs(param.getJdbcRetrySleepInitMs());
        holoConfig.setRetrySleepStepMs(param.getJdbcRetrySleepStepMs());
        holoConfig.setConnectionMaxIdleMs(param.getJdbcConnectionMaxIdleMs());
        holoConfig.setMetaCacheTTL(param.getJdbcMetaCacheTTL());
        holoConfig.setMetaAutoRefreshFactor(param.getJdbcMetaAutoRefreshFactor());

        // reader config
        holoConfig.setReadThreadSize(param.getConnectionPoolSize());
        holoConfig.setReadBatchSize(param.getJdbcReadBatchSize());
        holoConfig.setReadBatchQueueSize(param.getJdbcReadBatchQueueSize());
        holoConfig.setScanFetchSize(param.getJdbcScanFetchSize());
        holoConfig.setScanTimeoutSeconds(param.getScanTimeoutSeconds());

        // writer config
        holoConfig.setWriteThreadSize(param.getConnectionPoolSize());
        holoConfig.setWriteBatchSize(param.getJdbcWriteBatchSize());
        holoConfig.setWriteBatchByteSize(param.getJdbcWriteBatchByteSize());
        holoConfig.setWriteBatchTotalByteSize(param.getJdbcWriteBatchTotalByteSize());
        holoConfig.setWriteMaxIntervalMs(param.getJdbcWriteFlushInterval());
        holoConfig.setReWriteBatchedDeletes(param.getJdbcReWriteBatchedDeletes());
        holoConfig.setRewriteSqlMaxBatchSize(param.getJdbcRewriteSqlMaxBatchSize());
        holoConfig.setEnableDefaultForNotNullColumn(param.getJdbcEnableDefaultForNotNullColumn());
        holoConfig.setWriteMode(param.getJDBCWriteMode());

        holoConfig.setDynamicPartition(param.isCreateMissingPartTable());

        return holoConfig;
    }
}
