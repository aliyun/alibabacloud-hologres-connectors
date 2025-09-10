package com.alibaba.hologres.connector.flink.api;

import org.apache.flink.table.types.logical.LogicalType;

import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Abstract for different Holgores IO reader. */
public abstract class HologresReader<T> extends HologresIOClient<T> {
    protected final String[] primaryKeys;

    public HologresReader(
            HologresConnectionParam param,
            String[] fieldNames,
            LogicalType[] logicalTypes,
            String[] primaryKeys) {
        super(param, fieldNames, logicalTypes);
        this.primaryKeys = primaryKeys;
    }

    public abstract CompletableFuture<T> asyncGet(T record) throws IOException;

    public abstract CompletableFuture<List<T>> asyncGetMany(T record) throws IOException;

    public abstract T get(T record) throws IOException;

    public abstract List<T> getMany(T record) throws IOException;
}
