package com.alibaba.ververica.connectors.hologres.api;

import org.apache.flink.table.types.logical.LogicalType;

import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/** Abstract for different Holgores IO interface. */
public abstract class HologresIOClient<T> implements Serializable {
    protected final HologresConnectionParam param;
    protected final String[] fieldNames;
    protected final LogicalType[] logicalTypes;

    public HologresIOClient(
            HologresConnectionParam param, String[] fieldNames, LogicalType[] logicalTypes) {
        this.param = param;
        this.fieldNames = fieldNames;
        this.logicalTypes = logicalTypes;
    }

    public void open() throws IOException {
        open(null, null);
    }

    public abstract void open(@Nullable Integer taskNumber, @Nullable Integer numTasks)
            throws IOException;

    public abstract void close() throws IOException;
}
