package com.alibaba.ververica.connectors.hologres.source.lookup;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import com.alibaba.ververica.connectors.common.dim.cache.CacheStrategy;
import com.alibaba.ververica.connectors.hologres.api.HologresReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/** Hologres row fetcher based on LRU Cache with sync mode. */
public class HologresLookupFunction extends AbstractHologresLookupFunction<List<RowData>>
        implements FlatMapFunction<RowData, RowData> {
    private static final long serialVersionUID = 5111593589582551115L;
    private static final Logger LOG = LoggerFactory.getLogger(HologresLookupFunction.class);

    public HologresLookupFunction(
            String sqlTableName,
            TableSchema tableSchema,
            String[] index,
            CacheStrategy cacheStrategy,
            HologresReader<RowData> hologresReader,
            boolean hasPrimaryKey) {
        super(sqlTableName, tableSchema, index, cacheStrategy, hologresReader, hasPrimaryKey);
    }

    @Override
    public void flatMap(RowData rowData, Collector<RowData> collector) throws Exception {
        Object key = getSourceKey(rowData);
        if (key == null) {
            LOG.debug("Join Hologres on an empty key of row: {}", rowData);
            return;
        }
        List<RowData> resultRow = cache.get(key);
        if (null != resultRow) {
            resultRow.forEach(collector::collect);
            return;
        }

        if (hasPrimaryKey) {
            RowData result = hologresReader.get(rowData);
            if (result != null) {
                resultRow = Collections.singletonList(result);
            }
        } else {
            resultRow = hologresReader.getMany(rowData);
        }

        if (resultRow != null) {
            // parse to Row
            cache.put(key, resultRow);
            resultRow.forEach(collector::collect);
        } else if (cacheStrategy.isCacheEmpty()) {
            cache.put(key, Collections.emptyList());
        }
    }
}
