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

package com.alibaba.hologres.connector.flink.config;

import org.apache.flink.configuration.ReadableConfig;

import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.checkandput.CheckAndPutCondition;
import com.alibaba.hologres.client.model.expression.Expression;
import com.alibaba.hologres.connector.flink.utils.HologresUtils;
import com.alibaba.hologres.connector.flink.utils.JDBCUtils;

import java.io.Serializable;

/** A utility class of keeping the Hologres connection params. */
public class HologresConnectionParam implements Serializable {
    private static final long serialVersionUID = 1371382980680745051L;

    private final JDBCOptions options;

    // connection
    private final int jdbcRetryCount;
    private final long jdbcRetrySleepStepMs;
    private final long jdbcConnectionMaxIdleMs;
    private final long jdbcMetaCacheTTL;
    private final String connectionPoolName;
    private final int connectionPoolSize;
    private boolean enableFixedConnectionMode;
    private final boolean enableServerlessComputing;
    private final Integer serverlessComputingQueryPriority;
    private final Integer statementTimeoutSeconds;
    private boolean directConnect;

    // source
    private final int scanFetchSize;
    private final int scanTimeoutSeconds;
    private final boolean enableFilterPushDown;

    // sink configuration
    private final boolean useSinkV2;
    private final WriteMode writeMode;
    private final boolean ignoreDelete;
    private final boolean createMissingPartTable;
    private final boolean ignoreNullWhenUpdate;
    private final boolean enableRemoveU0000InText;
    private final boolean enableDeduplication;
    private final boolean enableAggressive;
    private final OnConflictAction onConflictAction;
    private final int writeBatchSize;
    private final long writeBatchByteSize;
    private final long writeBatchTotalByteSize;
    private final long writeFlushInterval;
    private final boolean enableAffectedRows;
    private final boolean enableHoldOnUpdateBefore;
    private final boolean enableReshuffleByHolo;
    private final Boolean useLegacyPutHandler;
    private final CheckAndPutCondition checkAndPutCondition;
    private final Expression insertExpression;
    private final String copyWriteFormat;
    private final DirtyDataStrategy dirtyDataStrategy;
    private final boolean dirtyDataCheck;
    private final boolean dataTypeTolerant;
    private final boolean extraColumnTolerant;

    // JDBC dim
    private final boolean insertIfNotExists;
    private final int jdbcReadBatchSize;
    private final int jdbcReadBatchQueueSize;

    public HologresConnectionParam(ReadableConfig properties) {
        this.options = JDBCUtils.getJDBCOptions(properties);
        this.onConflictAction = getOnConflictActionInternal(properties);

        this.createMissingPartTable =
                properties.get(HologresConfigs.CREATE_MISSING_PARTITION_TABLE);
        this.ignoreNullWhenUpdate = properties.get(HologresConfigs.IGNORE_NULL_WHEN_UPDATE);

        this.ignoreDelete = properties.get(HologresConfigs.SINK_IGNORE_DELETE);

        this.jdbcRetryCount = properties.get(HologresConfigs.JDBC_RETRY_COUNT);
        this.jdbcRetrySleepStepMs = properties.get(HologresConfigs.RETRY_SLEEP_STEP_MS);
        this.jdbcConnectionMaxIdleMs = properties.get(HologresConfigs.JDBC_CONNECTION_MAX_IDLE_MS);
        this.jdbcMetaCacheTTL = properties.get(HologresConfigs.JDBC_META_CACHE_TTL);

        this.connectionPoolName = properties.get(HologresConfigs.CONNECTION_POOL_NAME);
        this.enableFixedConnectionMode = properties.get(HologresConfigs.CONNECTION_FIXED);
        this.connectionPoolSize = properties.get(HologresConfigs.CONNECTION_POOL_SIZE);
        this.enableServerlessComputing =
                properties.get(HologresConfigs.ENABLE_SERVERLESS_COMPUTING);
        this.serverlessComputingQueryPriority =
                properties.get(HologresConfigs.SERVERLESS_COMPUTING_QUERY_PRIORITY);
        this.statementTimeoutSeconds = properties.get(HologresConfigs.STATEMENT_TIMEOUT_SECONDS);

        this.jdbcReadBatchSize = properties.get(HologresConfigs.READ_BATCH_SIZE);
        this.jdbcReadBatchQueueSize = properties.get(HologresConfigs.READ_BATCH_QUEUE_SIZE);
        this.scanFetchSize = properties.get(HologresConfigs.SCAN_FETCH_SIZE);
        this.scanTimeoutSeconds = properties.get(HologresConfigs.SCAN_TIMEOUT_SECONDS);
        this.enableFilterPushDown = properties.get(HologresConfigs.ENABLE_FILTER_PUSH_DOWN);

        this.enableReshuffleByHolo = properties.get(HologresConfigs.ENABLE_RESHUFFLE_BY_HOLO);
        this.writeMode =
                getWriteModeInternal(
                        properties.get(HologresConfigs.WRITE_MODE), options, enableReshuffleByHolo);
        this.useSinkV2 = properties.get(HologresConfigs.ENABLE_API_VERSION_V2);
        this.writeBatchSize = properties.get(HologresConfigs.WRITE_BATCH_SIZE);
        this.writeBatchByteSize = properties.get(HologresConfigs.WRITE_BATCH_BYTE_SIZE);
        this.writeBatchTotalByteSize = properties.get(HologresConfigs.WRITE_BATCH_TOTAL_BYTE_SIZE);
        this.writeFlushInterval = properties.get(HologresConfigs.WRITE_FLUSH_INTERVAL);
        this.enableAffectedRows = properties.get(HologresConfigs.ENABLE_AFFECTED_ROWS);
        this.enableHoldOnUpdateBefore =
                properties.get(HologresConfigs.ENABLE_HOLD_ON_UPDATE_BEFORE);
        this.useLegacyPutHandler = properties.get(HologresConfigs.WRITE_USE_LEGACY_PUT_HANDLER);
        this.enableRemoveU0000InText = properties.get(HologresConfigs.ENABLE_REMOVE_U0000_IN_TEXT);
        this.enableDeduplication = properties.get(HologresConfigs.ENABLE_DEDUPLICATION);
        this.enableAggressive = properties.get(HologresConfigs.ENABLE_AGGRESSIVE);
        this.insertIfNotExists = properties.get(HologresConfigs.INSERT_IF_NOT_EXISTS);
        this.copyWriteFormat = properties.get(HologresConfigs.COPY_WRITE_FORMAT);
        this.directConnect = properties.get(HologresConfigs.CONNECTION_DIRECT);
        this.checkAndPutCondition = HologresUtils.getCheckAndPutCondition(properties);
        this.insertExpression =
                new Expression(
                        properties.get(HologresConfigs.INSERT_CONFLICT_UPDATE_SET),
                        properties.get(HologresConfigs.INSERT_CONFLICT_WHERE));
        this.dirtyDataStrategy = properties.get(HologresConfigs.INSERT_DIRTY_DATA_STRATEGY);
        this.dirtyDataCheck = properties.get(HologresConfigs.ENABLE_DIRTY_DATA_CHECK);
        this.dataTypeTolerant =
                properties.get(HologresConfigs.ENABLE_MULTI_TABLE_DATATYPE_TOLERANT);
        this.extraColumnTolerant =
                properties.get(HologresConfigs.ENABLE_MULTI_TABLE_EXTRA_COLUMN_TOLERANT);
    }

    private static OnConflictAction getOnConflictActionInternal(ReadableConfig tableProperties) {
        OnConflictAction conflictAction;
        String mutateType = tableProperties.get(HologresConfigs.ON_CONFLICT_ACTION).toLowerCase();
        switch (mutateType) {
            case "insertorignore":
            case "insert_or_ignore":
                conflictAction = OnConflictAction.INSERT_OR_IGNORE;
                break;
            case "insertorreplace":
            case "insert_or_replace":
                conflictAction = OnConflictAction.INSERT_OR_REPLACE;
                break;
            case "insertorupdate":
            case "insert_or_update":
                conflictAction = OnConflictAction.INSERT_OR_UPDATE;
                break;
            default:
                throw new RuntimeException("Could not recognize mutate type " + mutateType);
        }
        return conflictAction;
    }

    private static WriteMode getWriteModeInternal(
            WriteMode writeMode, JDBCOptions options, boolean enableReshuffleByHolo) {
        if (writeMode != WriteMode.AUTO) {
            return writeMode;
        }
        return HologresUtils.chooseBestWriteMode(options, enableReshuffleByHolo);
    }

    public JDBCOptions getJdbcOptions() {
        return this.options;
    }

    public int getConnectionPoolSize() {
        return this.connectionPoolSize;
    }

    public String getConnectionPoolName() {
        return connectionPoolName;
    }

    public boolean isUseSinkV2() {
        return useSinkV2;
    }

    public WriteMode getWriteMode() {
        return writeMode;
    }

    public boolean isEnableFixedConnectionMode() {
        return enableFixedConnectionMode;
    }

    public void setFixedConnectionMode(boolean enableFixedConnectionMode) {
        this.enableFixedConnectionMode = enableFixedConnectionMode;
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

    public boolean isIgnoreDelete() {
        return ignoreDelete;
    }

    public int getWriteBatchSize() {
        return this.writeBatchSize;
    }

    public long getWriteBatchByteSize() {
        return writeBatchByteSize;
    }

    public long getWriteBatchTotalByteSize() {
        return writeBatchTotalByteSize;
    }

    public long getWriteFlushInterval() {
        return this.writeFlushInterval;
    }

    public int getJdbcReadBatchSize() {
        return jdbcReadBatchSize;
    }

    public int getJdbcReadBatchQueueSize() {
        return jdbcReadBatchQueueSize;
    }

    public int getScanFetchSize() {
        return scanFetchSize;
    }

    public int getScanTimeoutSeconds() {
        return scanTimeoutSeconds;
    }

    public boolean isUseLegacyPutHandler() {
        return useLegacyPutHandler;
    }

    public boolean isEnableRemoveU0000InText() {
        return enableRemoveU0000InText;
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

    public long getJdbcRetrySleepStepMs() {
        return jdbcRetrySleepStepMs;
    }

    public long getJdbcConnectionMaxIdleMs() {
        return jdbcConnectionMaxIdleMs;
    }

    public long getJdbcMetaCacheTTL() {
        return jdbcMetaCacheTTL;
    }

    public boolean isInsertIfNotExists() {
        return this.insertIfNotExists;
    }

    public OnConflictAction getOnConflictAction() {
        return onConflictAction;
    }

    public String getCopyWriteFormat() {
        return copyWriteFormat;
    }

    public DirtyDataStrategy getDirtyDataStrategy() {
        return dirtyDataStrategy;
    }

    public boolean isDirtyDataCheck() {
        return dirtyDataCheck;
    }

    public boolean isDataTypeTolerant() {
        return dataTypeTolerant;
    }

    public boolean isExtraColumnTolerant() {
        return extraColumnTolerant;
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

    public Expression getInsertExpression() {
        return insertExpression;
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

    public boolean isEnableFilterPushDown() {
        return enableFilterPushDown;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("HologresConnectionParam{\n");

        sb.append("\tJDBC Options=").append(options.toString()).append(",\n");

        sb.append("\t")
                .append(HologresConfigs.CONNECTION_POOL_SIZE.key())
                .append("=")
                .append(connectionPoolSize)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.CONNECTION_FIXED.key())
                .append("=")
                .append(enableFixedConnectionMode)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.CONNECTION_DIRECT.key())
                .append("=")
                .append(directConnect)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.JDBC_RETRY_COUNT.key())
                .append("=")
                .append(jdbcRetryCount)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.RETRY_SLEEP_STEP_MS.key())
                .append("=")
                .append(jdbcRetrySleepStepMs)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.JDBC_CONNECTION_MAX_IDLE_MS.key())
                .append("=")
                .append(jdbcConnectionMaxIdleMs)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.JDBC_META_CACHE_TTL.key())
                .append("=")
                .append(jdbcMetaCacheTTL)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.SCAN_FETCH_SIZE.key())
                .append("=")
                .append(scanFetchSize)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.SCAN_TIMEOUT_SECONDS.key())
                .append("=")
                .append(scanTimeoutSeconds)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.ENABLE_FILTER_PUSH_DOWN.key())
                .append("=")
                .append(enableFilterPushDown)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.WRITE_MODE.key())
                .append("=")
                .append(writeMode)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.SINK_IGNORE_DELETE.key())
                .append("=")
                .append(ignoreDelete)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.CREATE_MISSING_PARTITION_TABLE.key())
                .append("=")
                .append(createMissingPartTable)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.IGNORE_NULL_WHEN_UPDATE.key())
                .append("=")
                .append(ignoreNullWhenUpdate)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.ENABLE_REMOVE_U0000_IN_TEXT.key())
                .append("=")
                .append(enableRemoveU0000InText)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.ENABLE_DEDUPLICATION.key())
                .append("=")
                .append(enableDeduplication)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.ENABLE_AGGRESSIVE.key())
                .append("=")
                .append(enableAggressive)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.ON_CONFLICT_ACTION.key())
                .append("=")
                .append(onConflictAction)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.WRITE_BATCH_SIZE.key())
                .append("=")
                .append(writeBatchSize)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.WRITE_BATCH_BYTE_SIZE.key())
                .append("=")
                .append(writeBatchByteSize)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.WRITE_BATCH_TOTAL_BYTE_SIZE.key())
                .append("=")
                .append(writeBatchTotalByteSize)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.WRITE_FLUSH_INTERVAL.key())
                .append("=")
                .append(writeFlushInterval)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.WRITE_USE_LEGACY_PUT_HANDLER.key())
                .append("=")
                .append(useLegacyPutHandler)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.INSERT_DIRTY_DATA_STRATEGY.key())
                .append("=")
                .append(dirtyDataStrategy)
                .append(",\n");

        sb.append("\t")
                .append("sink.check-and-put.condition")
                .append("=")
                .append(checkAndPutCondition)
                .append(",\n");

        sb.append("\t")
                .append("sink.insert-expression")
                .append("=")
                .append(insertExpression)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.ENABLE_HOLD_ON_UPDATE_BEFORE.key())
                .append("=")
                .append(enableHoldOnUpdateBefore)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.ENABLE_RESHUFFLE_BY_HOLO.key())
                .append("=")
                .append(enableReshuffleByHolo)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.COPY_WRITE_FORMAT.key())
                .append("=")
                .append(copyWriteFormat)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.ENABLE_AFFECTED_ROWS.key())
                .append("=")
                .append(enableAffectedRows)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.ENABLE_DIRTY_DATA_CHECK.key())
                .append("=")
                .append(dirtyDataCheck)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.ENABLE_MULTI_TABLE_DATATYPE_TOLERANT.key())
                .append("=")
                .append(dataTypeTolerant)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.INSERT_IF_NOT_EXISTS.key())
                .append("=")
                .append(insertIfNotExists)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.READ_BATCH_SIZE.key())
                .append("=")
                .append(jdbcReadBatchSize)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.READ_BATCH_QUEUE_SIZE.key())
                .append("=")
                .append(jdbcReadBatchQueueSize)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.ENABLE_SERVERLESS_COMPUTING.key())
                .append("=")
                .append(enableServerlessComputing)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.SERVERLESS_COMPUTING_QUERY_PRIORITY.key())
                .append("=")
                .append(serverlessComputingQueryPriority)
                .append(",\n");

        sb.append("\t")
                .append(HologresConfigs.STATEMENT_TIMEOUT_SECONDS.key())
                .append("=")
                .append(statementTimeoutSeconds)
                .append(",\n");

        sb.append("}\n");

        return sb.toString();
    }
}
