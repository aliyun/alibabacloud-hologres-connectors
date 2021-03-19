package com.alibaba.ververica.connectors.hologres.source.lookup;

import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import com.alibaba.ververica.connectors.common.dim.cache.CacheStrategy;
import com.alibaba.ververica.connectors.hologres.api.HologresReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/** Hologres row fetcher based on LRU Cache with async mode. */
public class HologresAsyncLookupFunction extends AbstractHologresLookupFunction
        implements AsyncFunction<RowData, RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(HologresAsyncLookupFunction.class);

    public HologresAsyncLookupFunction(
            String sqlTableName,
            TableSchema tableSchema,
            String[] index,
            CacheStrategy cacheStrategy,
            HologresReader<RowData> hologresReader) {
        super(sqlTableName, tableSchema, index, cacheStrategy, hologresReader);
    }

    @Override
    public void asyncInvoke(RowData row, ResultFuture<RowData> resultFuture) throws Exception {
        Object key = getSourceKey(row);
        if (key == null) {
            LOG.debug("Join Hologres on an empty key of row: {}", row);
            resultFuture.complete(Collections.emptyList());
            return;
        }
        org.apache.flink.table.data.RowData cachedRow = one2oneCache.get(key);
        if (cachedRow != null) {
            if (cachedRow.getArity() == 0) {
                // key is cached and not hit
                resultFuture.complete(Collections.emptyList());
            } else {
                resultFuture.complete(Collections.singleton(cachedRow));
            }
            return;
        }

        CompletableFuture<RowData> result;
        result = hologresReader.asyncGet(row);
        result.handleAsync(
                (resultRows, throwable) -> {
                    try {
                        if (throwable != null) {
                            resultFuture.completeExceptionally(throwable);
                        } else {
                            if (resultRows == null) {
                                if (cacheStrategy.isCacheEmpty()) {
                                    one2oneCache.put(key, new GenericRowData(0));
                                }
                                resultFuture.complete(Collections.emptyList());
                            } else {
                                one2oneCache.put(key, resultRows);
                                resultFuture.complete(Collections.singleton(resultRows));
                            }
                        }
                    } catch (Throwable t) {
                        resultFuture.completeExceptionally(t);
                    }
                    return null;
                });
    }
}
