package com.alibaba.ververica.connectors.hologres.source.lookup;

import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import com.alibaba.ververica.connectors.common.dim.cache.CacheStrategy;
import com.alibaba.ververica.connectors.hologres.api.HologresReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Hologres row fetcher based on LRU Cache with async mode. */
public class HologresAsyncLookupFunction extends AbstractHologresLookupFunction<List<RowData>>
        implements AsyncFunction<RowData, RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(HologresAsyncLookupFunction.class);

    public HologresAsyncLookupFunction(
            String sqlTableName,
            TableSchema tableSchema,
            String[] index,
            CacheStrategy cacheStrategy,
            HologresReader<RowData> hologresReader,
            boolean hasPrimaryKey) {
        super(sqlTableName, tableSchema, index, cacheStrategy, hologresReader, hasPrimaryKey);
    }

    @Override
    public void asyncInvoke(RowData row, ResultFuture<RowData> resultFuture) throws Exception {
        Object key = getSourceKey(row);
        if (key == null) {
            LOG.debug("Join Hologres on an empty key of row: {}", row);
            resultFuture.complete(Collections.emptyList());
            return;
        }
        List<RowData> cachedRow = cache.get(key);
        if (cachedRow != null) {
            resultFuture.complete(cachedRow);
            return;
        }
        CompletableFuture<List<RowData>> result;
        if (hasPrimaryKey) {
            result =
                    hologresReader
                            .asyncGet(row)
                            .thenApply(
                                    r ->
                                            r == null
                                                    ? Collections.emptyList()
                                                    : Lists.newArrayList(r));
        } else {
            result = hologresReader.asyncGetMany(row);
        }
        result.handleAsync(
                (resultRows, throwable) -> {
                    try {
                        if (throwable != null) {
                            resultFuture.completeExceptionally(throwable);
                        } else {
                            if (resultRows == null || resultRows.isEmpty()) {
                                if (cacheStrategy.isCacheEmpty()) {
                                    cache.put(key, Collections.emptyList());
                                }
                                resultFuture.complete(Collections.emptyList());
                            } else {
                                cache.put(key, resultRows);
                                resultFuture.complete(resultRows);
                            }
                        }
                    } catch (Throwable t) {
                        resultFuture.completeExceptionally(t);
                    }
                    return null;
                });
    }
}
