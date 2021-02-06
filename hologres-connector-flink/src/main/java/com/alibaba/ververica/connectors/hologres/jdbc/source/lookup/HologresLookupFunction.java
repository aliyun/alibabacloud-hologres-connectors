package com.alibaba.ververica.connectors.hologres.jdbc.source.lookup;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import com.alibaba.hologres.client.Get;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.ververica.connectors.common.dim.cache.CacheStrategy;
import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hologres row fetcher based on LRU Cache with sync mode.
 */
public class HologresLookupFunction extends AbstractHologresLookupFunction
		implements FlatMapFunction<RowData, RowData> {

	private static final long serialVersionUID = 5111593589582551115L;
	private static final Logger LOG = LoggerFactory.getLogger(HologresLookupFunction.class);

	public HologresLookupFunction(
			String sqlTableName,
			TableSchema tableSchema,
			String[] index,
			HologresParams param,
			CacheStrategy cacheStrategy) {
		super(sqlTableName, tableSchema, index, param, cacheStrategy);
	}

	@Override
	public void flatMap(RowData row, Collector<RowData> collector) throws Exception {
		Object key = getSourceKey(row);
		if (key == null) {
			LOG.debug("Join Hologres on an empty key of row: {}", row);
			return;
		}
		org.apache.flink.table.data.RowData cachedRow = one2oneCache.get(key);
		if (null != cachedRow) {
			if (cachedRow.getArity() > 0) {
				collector.collect(cachedRow);
			}
			return;
		}

		Get get = createGetRequest(row);
		Record rowData = holoClient.get(get).get();

		// if rowData == null then represents no data in single get mode
		if (rowData != null) {
			// parse to Row
			org.apache.flink.table.data.RowData resultRow = parseToRow(rowData);
			one2oneCache.put(key, resultRow);
			collector.collect(resultRow);
		} else if (cacheStrategy.isCacheEmpty()) {
			one2oneCache.put(key, new GenericRowData(0));
		}
	}
}
