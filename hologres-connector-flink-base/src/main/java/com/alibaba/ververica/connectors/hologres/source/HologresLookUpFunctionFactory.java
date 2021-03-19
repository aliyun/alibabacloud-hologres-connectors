package com.alibaba.ververica.connectors.hologres.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;

import com.alibaba.ververica.connectors.common.dim.cache.CacheStrategy;
import com.alibaba.ververica.connectors.hologres.api.HologresReader;

/** An utility class to create Hologres Look up function. */
public interface HologresLookUpFunctionFactory {
    AsyncFunction<RowData, RowData> createAsyncFunction(
            String sqlTableName,
            TableSchema tableSchema,
            String[] index,
            CacheStrategy cacheStrategy,
            HologresReader<RowData> hologresReader,
            boolean hasPrimaryKey);

    FlatMapFunction<RowData, RowData> createFunction(
            String sqlTableName,
            TableSchema tableSchema,
            String[] index,
            CacheStrategy cacheStrategy,
            HologresReader<RowData> hologresReader,
            boolean hasPrimaryKey);
}
