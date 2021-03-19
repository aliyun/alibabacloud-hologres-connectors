package com.alibaba.ververica.connectors.hologres.source.lookup;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import com.alibaba.ververica.connectors.common.dim.cache.CacheStrategy;
import com.alibaba.ververica.connectors.hologres.api.HologresReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Hologres row fetcher based on LRU Cache with sync mode. */
public class HologresLookupFunction extends AbstractHologresLookupFunction
        implements FlatMapFunction<RowData, RowData> {
    private static final long serialVersionUID = 5111593589582551115L;
    private static final Logger LOG = LoggerFactory.getLogger(HologresLookupFunction.class);

    public HologresLookupFunction(
            String sqlTableName,
            TableSchema tableSchema,
            String[] index,
            CacheStrategy cacheStrategy,
            HologresReader<RowData> hologresReader) {
        super(sqlTableName, tableSchema, index, cacheStrategy, hologresReader);
    }

    @Override
    public void flatMap(RowData rowData, Collector<RowData> collector) throws Exception {
        Object key = getSourceKey(rowData);
        if (key == null) {
            LOG.debug("Join Hologres on an empty key of row: {}", rowData);
            return;
        }
        org.apache.flink.table.data.RowData cachedRow = one2oneCache.get(key);
        if (null != cachedRow) {
            if (cachedRow.getArity() > 0) {
                collector.collect(cachedRow);
            }
            return;
        }

        RowData resultRow = hologresReader.get(rowData);

        if (resultRow != null) {
            // parse to Row
            one2oneCache.put(key, resultRow);
            collector.collect(resultRow);
        } else if (cacheStrategy.isCacheEmpty()) {
            one2oneCache.put(key, new GenericRowData(0));
        }
    }
}
