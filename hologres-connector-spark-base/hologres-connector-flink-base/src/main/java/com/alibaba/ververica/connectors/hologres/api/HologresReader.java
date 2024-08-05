package com.alibaba.ververica.connectors.hologres.api;

import org.apache.flink.table.api.TableSchema;

import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Abstract for different Holgores IO reader. */
public abstract class HologresReader<T> extends HologresIOClient<T> {
    protected final String[] primaryKeys;

    public HologresReader(
            HologresConnectionParam param, TableSchema tableSchema, String[] primaryKeys) {
        super(param, tableSchema);
        this.primaryKeys = primaryKeys;
    }

    public abstract CompletableFuture<T> asyncGet(T record) throws IOException;

    public abstract CompletableFuture<List<T>> asyncGetMany(T record) throws IOException;

    public abstract T get(T record) throws IOException;

    public abstract List<T> getMany(T record) throws IOException;
}
