package com.alibaba.hologres.client.copy.in;

import com.alibaba.hologres.client.copy.CopyContextCommon;
import com.alibaba.hologres.client.copy.CopyFormat;
import com.alibaba.hologres.client.copy.CopyMode;
import com.alibaba.hologres.client.copy.CopyUtil;
import com.alibaba.hologres.client.copy.in.binaryrow.RecordBinaryRowOutputStream;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.TableSchema;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;

public class CopyInContext extends CopyContextCommon {
    public static final Logger LOG = LoggerFactory.getLogger(CopyInContext.class);
    private static final HoloVersion BULK_LOAD_BINARY_MIN_VERSION = new HoloVersion("5.0.0");
    private static final String BULK_LOAD_BINARY_VERSION_ERROR =
            "binary 格式仅在 Hologres 5.0.0 及以上版本可用，当前版本不支持";

    // for copy in
    public CopyInOutputStream inOs = null;
    public RecordOutputStream recordOs = null;

    private final CopyMode copyMode;
    private final OnConflictAction onConflictAction;

    /**
     * 逻辑分区子句, 如 PARTITION(ds='2025-01-11', "KIND"=100) PARTITION(ds='2025-01-12', "KIND"=200).
     * 空字符串表示不指定分区. 对 bulk_load/bulk_load_on_conflict 模式: 指定分区可将表级锁降级为分区级锁, 并发写不同分区不再互相阻塞; 对 stream
     * 模式: stream 本身是行锁, 分区子句不影响锁级别, 主要起数据过滤作用(不属于声明分区的数据会被丢弃).
     */
    private String partitionClause = "";

    private BitSet needSetColumns;

    private boolean enableCheckSchemaVersion = false;

    public CopyInContext(
            Connection conn,
            TableSchema schema,
            List<String> columns,
            CopyFormat copyFormat,
            CopyMode copyMode,
            OnConflictAction onConflictAction,
            int maxCellBufferSize) {
        super(conn, schema, columns, copyFormat, maxCellBufferSize);
        this.copyMode = copyMode;
        this.onConflictAction = onConflictAction;
    }

    /** 带逻辑分区子句的构造函数 (Hologres V5.0+). */
    public CopyInContext(
            Connection conn,
            TableSchema schema,
            List<String> columns,
            CopyFormat copyFormat,
            CopyMode copyMode,
            OnConflictAction onConflictAction,
            int maxCellBufferSize,
            String partitionClause) {
        this(conn, schema, columns, copyFormat, copyMode, onConflictAction, maxCellBufferSize);
        if (partitionClause != null) {
            this.partitionClause = partitionClause;
        }
    }

    public void setPartitionClause(String partitionClause) {
        if (partitionClause != null) {
            this.partitionClause = partitionClause;
        }
    }

    @Override
    public void init() throws IOException {
        try {
            if (copyFormat == CopyFormat.BINARY && copyMode != CopyMode.STREAM) {
                try {
                    checkBulkLoadBinaryVersion(ConnectionUtil.getHoloVersion(conn), copyMode);
                } catch (SQLException e) {
                    throw new IOException("Failed to query Hologres version", e);
                }
            }
            // RecordOutputStream putRecord时是按照schema中的字段顺序, 因此需要保证 columns 中的字段顺序与 schema 中的字段顺序一致
            List<String> orderedColumns = new ArrayList<>();
            needSetColumns = new BitSet(schema.getColumnSchema().length);
            for (int i = 0; i < schema.getColumnSchema().length; i++) {
                Column column = schema.getColumnSchema()[i];
                if (columns.contains(column.getName())) {
                    orderedColumns.add(column.getName());
                    needSetColumns.set(i);
                }
            }
            // 传入的columns中有schema中不存在的字段
            if (!new HashSet<>(orderedColumns).containsAll(columns)) {
                for (String column : columns) {
                    if (!orderedColumns.contains(column)) {
                        throw new IOException("column " + column + " is not in schema " + schema);
                    }
                }
            }
            String copySql =
                    CopyUtil.buildCopyInSql(
                            schema.getTableNameObj().getFullName(),
                            orderedColumns,
                            copyFormat,
                            schema.getPrimaryKeys() != null && schema.getPrimaryKeys().length > 0,
                            schema.getSchemaVersion(),
                            onConflictAction,
                            copyMode,
                            enableCheckSchemaVersion,
                            partitionClause,
                            copyFormat == CopyFormat.BINARY && copyMode != CopyMode.STREAM);
            LOG.info("copy in backendPid: {}", ConnectionUtil.getBackendPidByConn(conn));
            LOG.info("copy in sql: {}", copySql);
            copyManager = new CopyManager(conn.unwrap(PgConnection.class));
            inOs = new CopyInOutputStream(copyManager.copyIn(copySql));
            switch (copyFormat) {
                case BINARY:
                    recordOs =
                            new RecordBinaryOutputStream(
                                    inOs,
                                    schema,
                                    conn.unwrap(BaseConnection.class),
                                    maxCellBufferSize);
                    break;
                case BINARYROW:
                    recordOs =
                            new RecordBinaryRowOutputStream(
                                    inOs,
                                    schema,
                                    conn.unwrap(BaseConnection.class),
                                    maxCellBufferSize);
                    break;
                case CSV:
                    recordOs =
                            new RecordTextOutputStream(
                                    inOs,
                                    schema,
                                    conn.unwrap(BaseConnection.class),
                                    maxCellBufferSize);
                    break;
                default:
                    throw new IOException("unsupported copy in format: " + copyFormat);
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
        isInit.set(true);
    }

    /**
     * Validate the server version before opening a bulk-load binary COPY stream. Keeping this check
     * separate makes the version gate deterministic and unit-testable without a database.
     */
    static void checkBulkLoadBinaryVersion(HoloVersion version, CopyMode copyMode)
            throws IOException {
        if (copyMode != CopyMode.STREAM
                && (version == null || version.compareTo(BULK_LOAD_BINARY_MIN_VERSION) < 0)) {
            throw new IOException(BULK_LOAD_BINARY_VERSION_ERROR);
        }
    }

    public OnConflictAction getOnConflictAction() {
        return onConflictAction;
    }

    public void setEnableCheckSchemaVersion(boolean enableCheckSchemaVersion) {
        this.enableCheckSchemaVersion = enableCheckSchemaVersion;
    }

    public void close() throws IOException {
        if (recordOs != null) {
            recordOs.close();
        }
        // Log copy statistics and clear accumulated warnings when copy ends
        CopyUtil.logAndClearCopyWarnings(conn);
        copyManager = null;
    }

    public void checkContextOpen() throws IOException {
        if (IsNotInitialized()) {
            init();
        }
        if (!isInit.get() || conn == null || inOs == null || recordOs == null) {
            throw new IOException("copy in context is not initialized");
        }
    }

    public BitSet getNeedSetColumns() {
        return needSetColumns;
    }

    public List<String> getColumns() {
        return columns;
    }
}
