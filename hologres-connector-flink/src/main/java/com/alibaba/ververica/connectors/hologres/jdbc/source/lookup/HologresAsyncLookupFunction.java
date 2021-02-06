package com.alibaba.ververica.connectors.hologres.jdbc.source.lookup;

import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import com.alibaba.hologres.client.Get;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.ververica.connectors.common.dim.cache.CacheStrategy;
import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.function.BiFunction;

/**
 * Hologres row fetcher based on LRU Cache with async mode.
 */
public class HologresAsyncLookupFunction extends AbstractHologresLookupFunction
		implements AsyncFunction<RowData, RowData> {
	private static final Logger LOG = LoggerFactory.getLogger(HologresAsyncLookupFunction.class);

	public HologresAsyncLookupFunction (
			String sqlTableName,
			TableSchema tableSchema,
			String[] index,
			HologresParams params,
			CacheStrategy cacheStrategy) {
		super(sqlTableName, tableSchema, index, params, cacheStrategy);
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

		Get getRequest = createGetRequest(row);
		holoClient.get(getRequest).handle(new BiFunction<Record, Throwable, Object>() {
			@Override
			public Object apply(Record rowData, Throwable throwable) {
				try {
					// caught an error
					if (throwable != null) {
						resultFuture.completeExceptionally(throwable);
					} else {
						if (rowData == null) {
							if (cacheStrategy.isCacheEmpty()) {
								one2oneCache.put(key, new GenericRowData(0));
							}
							resultFuture.complete(Collections.emptyList());
						} else {
							org.apache.flink.table.data.RowData resultRow = parseToRow(rowData);
							one2oneCache.put(key, resultRow);
							resultFuture.complete(Collections.singleton(resultRow));
						}
					}
				} catch (Throwable e) {
					resultFuture.completeExceptionally(e);
				}
				return null;
			}
		});
	}
}
