package com.alibaba.hologres.connector.flink.sink.v2.stage;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.copy.CopyUtil;
import com.alibaba.hologres.client.copy.in.CopyInStageWrapper;
import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.connector.flink.arrow.RowDataArrowWriter;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.utils.JDBCUtils;
import com.alibaba.hologres.org.postgresql.PGProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 公共 Stage 操作工具类, 供三种模式的 writer/committer 复用.
 *
 * <p>包含:
 *
 * <ul>
 *   <li>创建/关闭 Stage
 *   <li>执行 INSERT INTO target SELECT FROM stages
 *   <li>Best-effort 清理 stages
 *   <li>初始化 HoloConfig 和 TableSchema
 * </ul>
 */
public final class StageHelper {

    private static final Logger LOG = LoggerFactory.getLogger(StageHelper.class);
    private static final AtomicInteger STAGE_COUNTER = new AtomicInteger(0);
    private static final int STAGE_CREATE_MAX_ATTEMPTS = 2;
    private static final long STAGE_CREATE_RETRY_SLEEP_MS = 1_000L;
    private static final String STAGE_APP_NAME = "hologres-connector-flink-stage";
    private static final String STAGE_NAME_PATTERN = "^[A-Za-z0-9_]+$";

    private StageHelper() {}

    // ==================== Stage 生命周期 ====================

    /** 生成唯一 stage 名称. */
    public static String generateStageName(int subtaskId, int attemptNumber) {
        return String.format(
                "flink_stage_%d_%d_%d_%d",
                subtaskId,
                attemptNumber,
                System.currentTimeMillis(),
                STAGE_COUNTER.getAndIncrement());
    }

    /** 通过 JDBC 创建 internal stage. */
    public static void createStage(HologresConnectionParam param, String stageName, int ttl)
            throws Exception {
        validateStageName(stageName);
        String url = param.getJdbcOptions().getDbUrl();
        executeStageCreateWithRetry(
                () -> {
                    try (Connection conn =
                                    JDBCUtils.createConnection(
                                            param.getJdbcOptions(), url, true, 3, STAGE_APP_NAME);
                            Statement stmt = conn.createStatement()) {
                        if (param.isEnableServerlessComputing()) {
                            stmt.execute("SET hg_computing_resource = 'serverless'");
                        }
                        stmt.execute(
                                "call hologres.hg_create_internal_stage('"
                                        + stageName
                                        + "', 'default_group', "
                                        + ttl
                                        + ");");
                    }
                },
                STAGE_CREATE_RETRY_SLEEP_MS,
                stageName);
        LOG.info("StageHelper: Stage created successfully: {}", stageName);
    }

    @FunctionalInterface
    interface StageCreateAction {
        void execute() throws SQLException;
    }

    static void executeStageCreateWithRetry(
            StageCreateAction action, long retrySleepMs, String stageName) throws SQLException {
        for (int attempt = 1; attempt <= STAGE_CREATE_MAX_ATTEMPTS; attempt++) {
            try {
                action.execute();
                return;
            } catch (SQLException e) {
                if (attempt >= STAGE_CREATE_MAX_ATTEMPTS || !isRetryableStageCreateFailure(e)) {
                    throw e;
                }
                LOG.warn(
                        "StageHelper: failed to create Stage {}, retrying attempt {}/{} after {} ms",
                        stageName,
                        attempt + 1,
                        STAGE_CREATE_MAX_ATTEMPTS,
                        retrySleepMs,
                        e);
                try {
                    Thread.sleep(retrySleepMs);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    SQLException interrupted =
                            new SQLException(
                                    "Interrupted while retrying Stage creation: " + stageName,
                                    interruptedException);
                    interrupted.addSuppressed(e);
                    throw interrupted;
                }
            }
        }
    }

    static boolean isRetryableStageCreateFailure(SQLException exception) {
        ExceptionCode code = HoloClientException.fromSqlException(exception).getCode();
        switch (code) {
            case CONNECTION_ERROR:
            case META_NOT_MATCH:
            case READ_ONLY:
            case TOO_MANY_CONNECTIONS:
            case BUSY:
            case TIMEOUT:
                return true;
            default:
                return false;
        }
    }

    /** 创建 CopyInStageWrapper 用于写入数据. */
    public static CopyInStageWrapper<RowData> createCopyInStageWrapper(
            HoloConfig holoConfig,
            TableSchema tableSchema,
            String stageName,
            int subtaskId,
            List<String> columnNames,
            int maxArrowBatchSize,
            int fileSizeLimit,
            RowType rowType,
            boolean compressionEnabled)
            throws IOException {
        validateStageColumns(tableSchema, columnNames, stageName);
        RowDataArrowWriter arrowWriter =
                new RowDataArrowWriter(
                        tableSchema, columnNames, maxArrowBatchSize, rowType, compressionEnabled);
        try {
            return new CopyInStageWrapper<>(
                    holoConfig, stageName, "data_" + subtaskId, arrowWriter, fileSizeLimit);
        } catch (Exception e) {
            throw new IOException("Failed to create CopyInStageWrapper: " + stageName, e);
        }
    }

    /** 关闭 CopyInStageWrapper（seal stage）. 失败时抛出异常, 阻止产出不完整的 committable. */
    public static void closeCopyInStageWrapper(
            CopyInStageWrapper<RowData> wrapper, String stageName, int subtaskId)
            throws IOException {
        if (wrapper != null) {
            try {
                wrapper.close();
                LOG.info("StageHelper[subtask={}]: Stage {} sealed", subtaskId, stageName);
            } catch (Exception e) {
                throw new IOException(
                        "StageHelper[subtask=" + subtaskId + "]: failed to seal Stage " + stageName,
                        e);
            }
        }
    }

    /** Best-effort 关闭 CopyInStageWrapper, 异常只 warn 不抛. 用于 close() 等清理场景. */
    public static void closeCopyInStageWrapperQuietly(
            CopyInStageWrapper<RowData> wrapper, String stageName, int subtaskId) {
        if (wrapper != null) {
            try {
                wrapper.close();
                LOG.info(
                        "StageHelper[subtask={}]: Stage {} sealed (cleanup)", subtaskId, stageName);
            } catch (Exception e) {
                LOG.warn(
                        "StageHelper[subtask={}]: failed to close Stage {}",
                        subtaskId,
                        stageName,
                        e);
            }
        }
    }

    // ==================== HoloConfig / TableSchema ====================

    /** 创建 HoloConfig. */
    public static HoloConfig createHoloConfig(HologresConnectionParam param) {
        HoloConfig holoConfig = new HoloConfig();
        holoConfig.setJdbcUrl(param.getJdbcOptions().getDbUrl());
        holoConfig.setUsername(param.getUsername());
        holoConfig.setPassword(param.getPassword());
        holoConfig.setUseAKv4(false);
        holoConfig.setAppName(STAGE_APP_NAME);
        return holoConfig;
    }

    /** 获取 TableSchema. */
    public static TableSchema getTableSchema(HoloConfig holoConfig, String qualifiedTableName)
            throws Exception {
        try (HoloClient client = new HoloClient(holoConfig)) {
            return client.getTableSchema(qualifiedTableName);
        }
    }

    /** 校验 Stage 写入列在 Hologres 目标表中存在，避免在 Arrow schema 创建阶段抛出难以理解的 NPE. */
    public static void validateStageColumns(
            TableSchema tableSchema, List<String> columnNames, String tableOrStageName) {
        if (tableSchema == null) {
            throw new IllegalArgumentException(
                    "Hologres Stage sink cannot validate columns because target table schema is null"
                            + ", target="
                            + tableOrStageName);
        }
        if (columnNames == null || columnNames.isEmpty()) {
            throw new IllegalArgumentException(
                    "Hologres Stage sink requires at least one target column, target="
                            + tableOrStageName);
        }

        StringBuilder missingColumns = new StringBuilder();
        for (String columnName : columnNames) {
            if (tableSchema.getColumnIndex(columnName) == null) {
                if (missingColumns.length() > 0) {
                    missingColumns.append(", ");
                }
                missingColumns.append(columnName);
            }
        }
        if (missingColumns.length() == 0) {
            return;
        }

        throw new IllegalArgumentException(
                "Hologres Stage sink column(s) ["
                        + missingColumns
                        + "] do not exist in target table schema, target="
                        + tableOrStageName
                        + ", sinkColumns="
                        + columnNames
                        + ", hologresColumns="
                        + getSchemaColumnNames(tableSchema)
                        + ". Check column names, case sensitivity, and partial insert target columns.");
    }

    // ==================== 提交逻辑 ====================

    /**
     * 执行 INSERT INTO target SELECT FROM stages SQL. Stage sink 提供 at-least-once 语义; 重复执行后的结果取决于目标表
     * OnConflictAction 配置.
     *
     * @param targetShard 目标 shard ID, -1 表示不设置
     */
    public static void executeInsert(
            HologresConnectionParam param,
            String qualifiedTableName,
            List<String> columnNames,
            List<String> stageNames,
            long totalRecords,
            int targetShard,
            boolean internalStageSerialReadEnabled)
            throws Exception {
        executeInsert(
                param,
                qualifiedTableName,
                columnNames,
                stageNames,
                totalRecords,
                targetShard < 0 ? Collections.emptyList() : Collections.singletonList(targetShard),
                internalStageSerialReadEnabled,
                0);
    }

    public static void executeInsert(
            HologresConnectionParam param,
            String qualifiedTableName,
            List<String> columnNames,
            List<String> stageNames,
            long totalRecords,
            List<Integer> targetShards,
            boolean internalStageSerialReadEnabled,
            long queryTimeoutMs)
            throws Exception {
        HoloConfig holoConfig = createHoloConfig(param);
        TableSchema tableSchema = getTableSchema(holoConfig, qualifiedTableName);

        OnConflictAction conflictAction = param.getOnConflictAction();
        String insertSql =
                CopyUtil.buildInsertTableSelectFromStageSql(
                        tableSchema, columnNames, stageNames, conflictAction);
        LOG.info("StageHelper: SQL: {}", insertSql);

        long startMs = System.currentTimeMillis();
        String queryId = "";
        String url = param.getJdbcOptions().getDbUrl();
        try (Connection conn =
                        JDBCUtils.createConnection(
                                param.getJdbcOptions(), url, true, 3, STAGE_APP_NAME);
                Statement stmt = conn.createStatement()) {
            configureStatementTimeout(stmt, queryTimeoutMs);
            configureInsertSession(stmt, param, targetShards, internalStageSerialReadEnabled);
            stmt.clearWarnings();
            try {
                stmt.execute(insertSql);
            } finally {
                queryId = getQueryIdFromNotice(stmt);
                LOG.info(
                        "StageHelper: Stage INSERT queryId={}",
                        queryId.isEmpty() ? "unknown" : queryId);
            }
        } catch (Exception e) {
            throw new IOException(
                    "Stage INSERT failed, queryId="
                            + (queryId.isEmpty() ? "unknown" : queryId)
                            + ", stages="
                            + stageNames
                            + ", records="
                            + totalRecords
                            + ", targetShards="
                            + targetShards
                            + ". If this failure happens during recovery, the Internal Stage may "
                            + "have expired by sink.stage.ttl or been cleaned up before replay; "
                            + "increase sink.stage.ttl so it covers the checkpoint interval, "
                            + "Stage INSERT duration, and job recovery time.",
                    e);
        }
        LOG.info(
                "StageHelper: Stage INSERT completed, queryId={}, cost={} ms, stages={}, "
                        + "records={}, targetShards={}",
                queryId.isEmpty() ? "unknown" : queryId,
                System.currentTimeMillis() - startMs,
                stageNames.size(),
                totalRecords,
                targetShards);
    }

    public static void executeInsert(
            HologresConnectionParam param,
            String qualifiedTableName,
            List<String> columnNames,
            List<String> stageNames,
            long totalRecords,
            int targetShard,
            boolean internalStageSerialReadEnabled,
            long queryTimeoutMs)
            throws Exception {
        executeInsert(
                param,
                qualifiedTableName,
                columnNames,
                stageNames,
                totalRecords,
                targetShard < 0 ? Collections.emptyList() : Collections.singletonList(targetShard),
                internalStageSerialReadEnabled,
                queryTimeoutMs);
    }

    static void configureStatementTimeout(Statement stmt, long queryTimeoutMs) throws Exception {
        if (queryTimeoutMs <= 0) {
            return;
        }
        long timeoutSeconds = Math.max(1, (queryTimeoutMs + 999) / 1000);
        stmt.setQueryTimeout((int) Math.min(timeoutSeconds, Integer.MAX_VALUE));
    }

    static void configureInsertSession(
            Statement stmt,
            HologresConnectionParam param,
            int targetShard,
            boolean internalStageSerialReadEnabled)
            throws Exception {
        configureInsertSession(
                stmt,
                param,
                targetShard < 0 ? Collections.emptyList() : Collections.singletonList(targetShard),
                internalStageSerialReadEnabled);
    }

    static void configureInsertSession(
            Statement stmt,
            HologresConnectionParam param,
            List<Integer> targetShards,
            boolean internalStageSerialReadEnabled)
            throws Exception {
        stmt.execute("set hg_experimental_display_query_id = on");
        if (param.isEnableServerlessComputing()) {
            stmt.execute("SET hg_computing_resource = 'serverless'");
            Integer requiredCores = param.getServerlessComputingRequiredCores();
            if (requiredCores != null) {
                stmt.execute(
                        "set hg_experimental_serverless_computing_required_cores = "
                                + requiredCores);
                LOG.info(
                        "StageHelper: using Serverless computing resource, cores={}",
                        requiredCores);
            } else {
                LOG.info(
                        "StageHelper: using Serverless computing resource with auto-detected cores");
            }
        }
        if (targetShards != null && !targetShards.isEmpty()) {
            String shardList =
                    targetShards.stream().map(String::valueOf).collect(Collectors.joining(","));
            stmt.execute(String.format("set hg_experimental_target_shard_list = '%s'", shardList));
            LOG.info("StageHelper: set target_shard_list={}", shardList);
        }
        stmt.execute(
                "set hg_experimental_internal_stage_serial_read = "
                        + (internalStageSerialReadEnabled ? "on" : "off"));
        LOG.info(
                "StageHelper: set hg_experimental_internal_stage_serial_read={}",
                internalStageSerialReadEnabled ? "on" : "off");
    }

    static String getQueryIdFromNotice(Statement statement) {
        try {
            return ConnectionUtil.getQueryIdFromNotice(statement.getWarnings());
        } catch (SQLException e) {
            LOG.warn("StageHelper: failed to read Stage INSERT QueryId from NOTICE", e);
            return "";
        }
    }

    /** executeInsert 的便捷重载, 不设置 target shard (BATCH 模式). */
    public static void executeInsert(
            HologresConnectionParam param,
            String qualifiedTableName,
            List<String> columnNames,
            List<String> stageNames,
            long totalRecords)
            throws Exception {
        executeInsert(param, qualifiedTableName, columnNames, stageNames, totalRecords, -1);
    }

    public static void executeInsert(
            HologresConnectionParam param,
            String qualifiedTableName,
            List<String> columnNames,
            List<String> stageNames,
            long totalRecords,
            int targetShard)
            throws Exception {
        executeInsert(
                param,
                qualifiedTableName,
                columnNames,
                stageNames,
                totalRecords,
                targetShard,
                false);
    }

    /** Best-effort 清理 stages (drop internal stage). 失败不影响数据一致性, 残留 stage 依赖 TTL 自动过期. */
    public static void cleanupStages(HologresConnectionParam param, List<String> stageNames) {
        String url = param.getJdbcOptions().getDbUrl();
        try (Connection conn =
                        JDBCUtils.createConnection(
                                param.getJdbcOptions(), url, true, 3, STAGE_APP_NAME);
                Statement stmt = conn.createStatement()) {
            for (String stageName : stageNames) {
                try {
                    validateStageName(stageName);
                    stmt.execute("call hologres.hg_drop_internal_stage('" + stageName + "');");
                    LOG.info("StageHelper: dropped Stage {}", stageName);
                } catch (Exception e) {
                    LOG.warn("StageHelper: failed to drop Stage {}", stageName, e);
                }
            }
        } catch (Exception e) {
            LOG.warn("StageHelper: failed to close cleanup connection", e);
        }
    }

    // ==================== 工具方法 ====================

    public static Properties getConnectionProperties(HologresConnectionParam param) {
        Properties props = new Properties();
        props.setProperty("user", param.getUsername());
        props.setProperty("password", param.getPassword());
        PGProperty.APPLICATION_NAME.set(props, STAGE_APP_NAME);
        return props;
    }

    static void validateStageName(String stageName) {
        if (stageName == null || !stageName.matches(STAGE_NAME_PATTERN)) {
            throw new IllegalArgumentException("Invalid stage name: " + stageName);
        }
    }

    private static List<String> getSchemaColumnNames(TableSchema tableSchema) {
        if (tableSchema == null || tableSchema.getColumnSchema() == null) {
            return Collections.emptyList();
        }
        return java.util.Arrays.stream(tableSchema.getColumnSchema())
                .map(column -> column == null ? "<null>" : column.getName())
                .collect(Collectors.toList());
    }
}
