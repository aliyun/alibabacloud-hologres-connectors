package com.alibaba.ververica.connectors.hologres.api;

import org.apache.flink.table.types.logical.LogicalType;

import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;

import java.io.IOException;

/** Abstract for different Holgores IO writer. */
public abstract class HologresWriter<T> extends HologresIOClient<T> {
    public HologresWriter(
            HologresConnectionParam param, String[] fieldNames, LogicalType[] logicalTypes) {
        super(param, fieldNames, logicalTypes);
    }

    public abstract long writeAddRecord(T record) throws IOException;

    public abstract long writeDeleteRecord(T record) throws IOException;

    public abstract void flush() throws IOException;
}
