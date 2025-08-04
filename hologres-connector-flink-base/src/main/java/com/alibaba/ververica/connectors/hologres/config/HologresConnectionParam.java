/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.connectors.hologres.config;

import org.apache.flink.configuration.ReadableConfig;

import com.alibaba.hologres.client.copy.CopyMode;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.checkandput.CheckAndPutCondition;
import com.alibaba.ververica.connectors.common.source.resolver.DirtyDataStrategy;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCConfigs;
import com.alibaba.ververica.connectors.hologres.utils.HologresUtils;
import com.alibaba.ververica.connectors.hologres.utils.JDBCUtils;

import java.io.Serializable;

/** A utility class of keeping the Hologres connection params. */
public class HologresConnectionParam implements Serializable {
    private static final long serialVersionUID = 1371382980680745051L;

    private final JDBCOptions options;

    // sink configuration
    private final int splitDataSize;
    private final boolean ignoreDelete;
    private final boolean createMissingPartTable;
    private final boolean ignoreNullWhenUpdate;
    private final boolean jdbcEnableRemoveU0000InText;
    private final boolean enableDeduplication;
    private final boolean enableAggressive;

    // JDBC connection
    private final int jdbcRetryCount;
    private final long jdbcRetrySleepInitMs;
    private final long jdbcRetrySleepStepMs;
    private final long jdbcConnectionMaxIdleMs;
    private final long jdbcMetaCacheTTL;
    private final int jdbcMetaAutoRefreshFactor;
    private final String connectionPoolName;
    private final int connectionPoolSize;
    private final boolean fixedConnectionMode;
    private final boolean enableServerlessComputing;
    private final Integer serverlessComputingQueryPriority;
    private final Integer statementTimeoutSeconds;

    // JDBC source
    private final int jdbcReadBatchSize;
    private final int jdbcReadBatchQueueSize;
    private final int jdbcScanFetchSize;
    private final int jdbcScanTimeoutSeconds;
    // JDBC sink
    private final OnConflictAction writeMode;
    private final int jdbcWriteBatchSize;
    private final long jdbcWriteBatchByteSize;
    private final long jdbcWriteBatchTotalByteSize;
    private final long jdbcWriteFlushInterval;
    private final boolean enableAffectedRows;
    private final boolean enableHoldOnUpdateBefore;
    private final boolean enableReshuffleByHolo;
    private final Boolean jdbcUseLegacyPutHandler;
    private final boolean jdbcEnableDefaultForNotNullColumn;
    private final CheckAndPutCondition checkAndPutCondition;
    private final DirtyDataStrategy dirtyDataStrategy;
    // JDBC dim
    private final boolean insertIfNotExists;
    // JDBC copy sink
    private final CopyMode copyMode;
    private final String copyWriteFormat;
    private boolean directConnect;

    public HologresConnectionParam(ReadableConfig properties) {
        this.options = JDBCUtils.getJDBCOptions(properties);
        this.writeMode = getJDBCWriteMode(properties);

        this.createMissingPartTable =
                properties.get(HologresConfigs.CREATE_MISSING_PARTITION_TABLE);
        this.ignoreNullWhenUpdate = properties.get(HologresConfigs.IGNORE_NULL_WHEN_UPDATE);

        this.splitDataSize = properties.get(HologresConfigs.OPTIONAL_SPLIT_DATA_SIZE);
        this.ignoreDelete = properties.get(HologresConfigs.OPTIONAL_SINK_IGNORE_DELETE);

        this.dirtyDataStrategy = properties.get(HologresConfigs.ACTION_ON_INSERT_ERROR);

        this.jdbcRetryCount = properties.get(HologresJDBCConfigs.OPTIONAL_JDBC_RETRY_COUNT);
        this.jdbcRetrySleepInitMs =
                properties.get(HologresJDBCConfigs.OPTIONAL_RETRY_SLEEP_INIT_MS);
        this.jdbcRetrySleepStepMs =
                properties.get(HologresJDBCConfigs.OPTIONAL_RETRY_SLEEP_STEP_MS);
        this.jdbcConnectionMaxIdleMs =
                properties.get(HologresJDBCConfigs.OPTIONAL_JDBC_CONNECTION_MAX_IDLE_MS);
        this.jdbcMetaCacheTTL = properties.get(HologresJDBCConfigs.OPTIONAL_JDBC_META_CACHE_TTL);
        this.jdbcMetaAutoRefreshFactor =
                properties.get(HologresJDBCConfigs.OPTIONAL_JDBC_META_AUTO_REFRESH_FACTOR);
        this.connectionPoolName =
                properties.get(HologresJDBCConfigs.OPTIONAL_JDBC_SHARED_CONNECTION_POOL_NAME);
        this.fixedConnectionMode =
                properties.get(HologresJDBCConfigs.OPTIONAL_JDBC_FIXED_CONNECTION_MODE);
        this.connectionPoolSize =
                properties.get(HologresJDBCConfigs.OPTIONAL_CLIENT_CONNECTION_POOL_SIZE);
        this.enableServerlessComputing =
                properties.get(HologresJDBCConfigs.ENABLE_SERVERLESS_COMPUTING);
        this.serverlessComputingQueryPriority =
                properties.get(HologresJDBCConfigs.SERVERLESS_COMPUTING_QUERY_PRIORITY);
        this.statementTimeoutSeconds =
                properties.get(HologresJDBCConfigs.STATEMENT_TIMEOUT_SECONDS);

        this.jdbcReadBatchSize = properties.get(HologresJDBCConfigs.OPTIONAL_JDBC_READ_BATCH_SIZE);
        this.jdbcReadBatchQueueSize =
                properties.get(HologresJDBCConfigs.OPTIONAL_JDBC_READ_BATCH_QUEUE_SIZE);
        this.jdbcScanFetchSize = properties.get(HologresJDBCConfigs.OPTIONAL_JDBC_SCAN_FETCH_SIZE);
        this.jdbcScanTimeoutSeconds =
                properties.get(HologresJDBCConfigs.OPTIONAL_JDBC_SCAN_TIMEOUT_SECONDS);
        this.jdbcWriteBatchSize =
                properties.get(HologresJDBCConfigs.OPTIONAL_JDBC_WRITE_BATCH_SIZE);
        this.jdbcWriteBatchByteSize =
                properties.get(HologresJDBCConfigs.OPTIONAL_JDBC_WRITE_BATCH_BYTE_SIZE);
        this.jdbcWriteBatchTotalByteSize =
                properties.get(HologresJDBCConfigs.OPTIONAL_JDBC_WRITE_BATCH_TOTAL_BYTE_SIZE);
        this.jdbcWriteFlushInterval =
                properties.get(HologresJDBCConfigs.OPTIONAL_JDBC_WRITE_FLUSH_INTERVAL);
        this.enableAffectedRows = properties.get(HologresJDBCConfigs.OPTIONAL_ENABLE_AFFECTED_ROWS);
        this.enableHoldOnUpdateBefore =
                properties.get(HologresJDBCConfigs.OPTIONAL_ENABLE_HOLD_ON_UPDATE_BEFORE);
        this.enableReshuffleByHolo =
                properties.get(HologresJDBCConfigs.OPTIONAL_ENABLE_RESHUFFLE_BY_HOLO);
        this.jdbcUseLegacyPutHandler =
                properties.get(HologresJDBCConfigs.OPTIONAL_JDBC_USE_LEGACY_PUT_HANDLER);
        this.jdbcEnableDefaultForNotNullColumn =
                properties.get(
                        HologresJDBCConfigs.OPTIONAL_JDBC_ENABLE_DEFAULT_FOR_NOT_NULL_COLUMN);
        this.jdbcEnableRemoveU0000InText =
                properties.get(HologresJDBCConfigs.OPTIONAL_ENABLE_REMOVE_U0000_IN_TEXT);
        this.enableDeduplication =
                properties.get(HologresJDBCConfigs.OPTIONAL_ENABLE_DEDUPLICATION);
        this.enableAggressive = properties.get(HologresJDBCConfigs.OPTIONAL_ENABLE_AGGRESSIVE);
        this.insertIfNotExists = properties.get(HologresJDBCConfigs.INSERT_IF_NOT_EXISTS);
        this.copyMode = properties.get(HologresJDBCConfigs.COPY_WRITE_MODE);
        this.copyWriteFormat = properties.get(HologresJDBCConfigs.COPY_WRITE_FORMAT);
        this.directConnect = properties.get(HologresJDBCConfigs.DIRECT_CONNECT);
        this.checkAndPutCondition = HologresUtils.getCheckAndPutCondition(properties);
    }

    public static OnConflictAction getJDBCWriteMode(ReadableConfig tableProperties) {
        OnConflictAction writeMode = OnConflictAction.INSERT_OR_IGNORE;
        if (tableProperties.get(HologresConfigs.INSERT_OR_UPDATE)) {
            writeMode = OnConflictAction.INSERT_OR_UPDATE;
        }
        if (tableProperties.getOptional(HologresConfigs.MUTATE_TYPE).isPresent()) {
            String mutateType = tableProperties.get(HologresConfigs.MUTATE_TYPE).toLowerCase();
            switch (mutateType) {
                case "insertorignore":
                    writeMode = OnConflictAction.INSERT_OR_IGNORE;
                    break;
                case "insertorreplace":
                    writeMode = OnConflictAction.INSERT_OR_REPLACE;
                    break;
                case "insertorupdate":
                    writeMode = OnConflictAction.INSERT_OR_UPDATE;
                    break;
                default:
                    throw new RuntimeException("Could not recognize mutate type " + mutateType);
            }
        }
        return writeMode;
    }

    public JDBCOptions getJdbcOptions() {
        return this.options;
    }

    public int getConnectionPoolSize() {
        return this.connectionPoolSize;
    }

    public boolean isFixedConnectionMode() {
        return fixedConnectionMode;
    }

    public String getDatabase() {
        return options.getDatabase();
    }

    public String getTable() {
        return options.getTable();
    }

    public String getEndpoint() {
        return options.getEndpoint();
    }

    public String getUsername() {
        return options.getUsername();
    }

    public String getPassword() {
        return options.getPassword();
    }

    public boolean isCreateMissingPartTable() {
        return createMissingPartTable;
    }

    public boolean isIgnoreNullWhenUpdate() {
        return ignoreNullWhenUpdate;
    }

    public int getSplitDataSize() {
        return splitDataSize;
    }

    public boolean isIgnoreDelete() {
        return ignoreDelete;
    }

    public DirtyDataStrategy getDirtyDataStrategy() {
        return dirtyDataStrategy;
    }

    public int getJdbcWriteBatchSize() {
        return this.jdbcWriteBatchSize;
    }

    public long getJdbcWriteBatchByteSize() {
        return jdbcWriteBatchByteSize;
    }

    public long getJdbcWriteBatchTotalByteSize() {
        return jdbcWriteBatchTotalByteSize;
    }

    public long getJdbcWriteFlushInterval() {
        return this.jdbcWriteFlushInterval;
    }

    public int getJdbcReadBatchSize() {
        return jdbcReadBatchSize;
    }

    public int getJdbcReadBatchQueueSize() {
        return jdbcReadBatchQueueSize;
    }

    public int getJdbcScanFetchSize() {
        return jdbcScanFetchSize;
    }

    public int getScanTimeoutSeconds() {
        return jdbcScanTimeoutSeconds;
    }

    public boolean isJdbcUseLegacyPutHandler() {
        return jdbcUseLegacyPutHandler;
    }

    public boolean getJdbcEnableDefaultForNotNullColumn() {
        return jdbcEnableDefaultForNotNullColumn;
    }

    public boolean isJdbcEnableRemoveU0000InText() {
        return jdbcEnableRemoveU0000InText;
    }

    public boolean isEnableDeduplication() {
        return enableDeduplication;
    }

    public boolean isEnableAggressive() {
        return enableAggressive;
    }

    public int getJdbcRetryCount() {
        return jdbcRetryCount;
    }

    public long getJdbcRetrySleepInitMs() {
        return jdbcRetrySleepInitMs;
    }

    public long getJdbcRetrySleepStepMs() {
        return jdbcRetrySleepStepMs;
    }

    public long getJdbcConnectionMaxIdleMs() {
        return jdbcConnectionMaxIdleMs;
    }

    public long getJdbcMetaCacheTTL() {
        return jdbcMetaCacheTTL;
    }

    public int getJdbcMetaAutoRefreshFactor() {
        return jdbcMetaAutoRefreshFactor;
    }

    public boolean isInsertIfNotExists() {
        return this.insertIfNotExists;
    }

    public String getJdbcSharedConnectionPoolName() {
        return connectionPoolName;
    }

    public OnConflictAction getJDBCWriteMode() {
        return this.writeMode;
    }

    public CopyMode getCopyMode() {
        return copyMode;
    }

    public String getCopyWriteFormat() {
        return copyWriteFormat;
    }

    public boolean isDirectConnect() {
        return directConnect;
    }

    public void setDirectConnect(boolean directConnect) {
        this.directConnect = directConnect;
    }

    public boolean isEnableAffectedRows() {
        return enableAffectedRows;
    }

    public boolean isEnableHoldOnUpdateBefore() {
        return enableHoldOnUpdateBefore;
    }

    public boolean isEnableReshuffleByHolo() {
        return enableReshuffleByHolo;
    }

    public CheckAndPutCondition getCheckAndPutCondition() {
        return checkAndPutCondition;
    }

    public boolean isEnableServerlessComputing() {
        return enableServerlessComputing;
    }

    public Integer getServerlessComputingQueryPriority() {
        return serverlessComputingQueryPriority;
    }

    public Integer getStatementTimeoutSeconds() {
        return statementTimeoutSeconds;
    }

    @Override
    public String toString() {
        return "HologresConnectionParam{"
                + "options="
                + options
                + ", splitDataSize="
                + splitDataSize
                + ", ignoreDelete="
                + ignoreDelete
                + ", createMissingPartTable="
                + createMissingPartTable
                + ", ignoreNullWhenUpdate="
                + ignoreNullWhenUpdate
                + ", remove-u0000-in-text.enabled="
                + jdbcEnableRemoveU0000InText
                + ", deduplication.enabled="
                + enableDeduplication
                + ", affect-rows.enabled"
                + enableAffectedRows
                + ", aggressive.enabled="
                + enableAggressive
                + ", reshuffle-by-holo-distribution-key.enabled="
                + enableReshuffleByHolo
                + ", jdbcRetryCount="
                + jdbcRetryCount
                + ", jdbcRetrySleepInitMs="
                + jdbcRetrySleepInitMs
                + ", jdbcRetrySleepStepMs="
                + jdbcRetrySleepStepMs
                + ", jdbcConnectionMaxIdleMs="
                + jdbcConnectionMaxIdleMs
                + ", jdbcMetaCacheTTL="
                + jdbcMetaCacheTTL
                + ", jdbcMetaAutoRefreshFactor="
                + jdbcMetaAutoRefreshFactor
                + ", connectionPoolName='"
                + connectionPoolName
                + ", connectionPoolSize="
                + connectionPoolSize
                + ", fixedConnectionMode="
                + fixedConnectionMode
                + ", jdbcReadBatchSize="
                + jdbcReadBatchSize
                + ", jdbcReadBatchQueueSize="
                + jdbcReadBatchQueueSize
                + ", jdbcScanFetchSize="
                + jdbcScanFetchSize
                + ", jdbcScanTimeoutSeconds="
                + jdbcScanTimeoutSeconds
                + ", writeMode="
                + writeMode
                + ", jdbcWriteBatchSize="
                + jdbcWriteBatchSize
                + ", jdbcWriteBatchByteSize="
                + jdbcWriteBatchByteSize
                + ", jdbcWriteBatchTotalByteSize="
                + jdbcWriteBatchTotalByteSize
                + ", jdbcWriteFlushInterval="
                + jdbcWriteFlushInterval
                + ", jdbcUseLegacyPutHandler="
                + jdbcUseLegacyPutHandler
                + ", jdbcEnableDefaultForNotNullColumn="
                + jdbcEnableDefaultForNotNullColumn
                + ", insertIfNotExists="
                + insertIfNotExists
                + ", copyMode="
                + copyMode
                + ", copyWriteFormat='"
                + copyWriteFormat
                + ", directConnect="
                + directConnect
                + ", checkAndPutCondition="
                + checkAndPutCondition
                + ", enableServerlessComputing="
                + enableServerlessComputing
                + ", serverlessComputingQueryPriority="
                + serverlessComputingQueryPriority
                + ", statementTimeoutSeconds="
                + statementTimeoutSeconds
                + ", enableHoldOnUpdateBefore="
                + enableHoldOnUpdateBefore
                + '}';
    }
}
