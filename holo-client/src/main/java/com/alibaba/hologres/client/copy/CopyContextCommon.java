package com.alibaba.hologres.client.copy;

import com.alibaba.hologres.client.model.TableSchema;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class CopyContextCommon {
    public static final Logger LOG = LoggerFactory.getLogger(CopyContextCommon.class);

    protected AtomicBoolean isInit = new AtomicBoolean(false);
    protected final Connection conn;
    protected final List<String> columns;
    protected final CopyFormat copyFormat;
    protected final int maxCellBufferSize;
    protected CopyManager copyManager;
    protected TableSchema schema;

    public CopyContextCommon(
            Connection conn,
            TableSchema schema,
            List<String> columns,
            CopyFormat copyFormat,
            int maxCellBufferSize) {
        this.conn = conn;
        this.schema = schema;
        this.columns = columns;
        this.copyFormat = copyFormat;
        this.maxCellBufferSize = maxCellBufferSize;
    }

    protected abstract void init() throws IOException;

    // The connection is managed by the caller and we will not close it.
    protected abstract void close() throws IOException;

    public boolean IsNotInitialized() {
        return !isInit.get();
    }

    public TableSchema getSchema() throws IOException {
        if (IsNotInitialized()) {
            init();
        }
        return schema;
    }
}
