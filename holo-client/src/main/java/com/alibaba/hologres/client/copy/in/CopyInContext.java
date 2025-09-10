package com.alibaba.hologres.client.copy.in;

import com.alibaba.hologres.client.copy.CopyContextCommon;
import com.alibaba.hologres.client.copy.CopyFormat;
import com.alibaba.hologres.client.copy.CopyMode;
import com.alibaba.hologres.client.copy.CopyUtil;
import com.alibaba.hologres.client.copy.in.binaryrow.RecordBinaryRowOutputStream;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.OnConflictAction;
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

    // for copy in
    public CopyInOutputStream inOs = null;
    public RecordOutputStream recordOs = null;

    private final CopyMode copyMode;
    private final OnConflictAction onConflictAction;

    private BitSet needSetColumns;

    public CopyInContext(
            Connection conn,
            String tableName,
            List<String> columns,
            CopyFormat copyFormat,
            CopyMode copyMode,
            OnConflictAction onConflictAction,
            int maxCellBufferSize) {
        super(conn, tableName, columns, copyFormat, maxCellBufferSize);
        this.copyMode = copyMode;
        this.onConflictAction = onConflictAction;
    }

    @Override
    public void init() throws IOException {
        checkConnAndGetSchema();
        try {
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
                            onConflictAction,
                            copyMode);
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
                    throw new RuntimeException("unsupported copy in format: " + copyFormat);
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
        isInit.set(true);
    }

    public OnConflictAction getOnConflictAction() {
        return onConflictAction;
    }

    public void close() throws IOException {
        if (recordOs != null) {
            recordOs.close();
        }
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
