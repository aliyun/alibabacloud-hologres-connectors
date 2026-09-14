/*
 *  Copyright (c) 2021, Alibaba Group;
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.hologres.connector.flink.sink.v2;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.sink.repartition.HoloKeySelector;
import com.alibaba.hologres.connector.flink.sink.repartition.HoloPartitioner;
import com.alibaba.hologres.connector.flink.utils.SchemaUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** A {@link Sink V2} for Hologres. */
public class HologresSink<InputT> implements Sink<InputT>, SupportsPreWriteTopology<InputT> {
    private final HologresConnectionParam param;

    // TableSchema is not Serialiaze, just get what we need from TableSchema;
    private final String[] targetFieldNames;
    private final LogicalType[] targetFieldTypes;
    private final Integer[] targetColumnIndexs;
    private final Map<String, LogicalType> primarykeys;
    private final int shardCount;
    private final Integer[] distributionKeyIndexes;
    private final LogicalType[] routingFieldTypes;
    private Integer parallelism;

    public HologresSink(
            HologresConnectionParam param, TableSchema tableSchema, Integer[] targetColumnIndexes) {
        this(param, tableSchema, targetColumnIndexes, 0, new Integer[0], new LogicalType[0]);
    }

    public HologresSink(
            HologresConnectionParam param,
            TableSchema tableSchema,
            Integer[] targetColumnIndexes,
            int shardCount,
            Integer[] distributionKeyIndexes,
            LogicalType[] routingFieldTypes) {
        this.param = param;
        String[] targetFieldNames =
                SchemaUtil.getTargetFieldNames(tableSchema, targetColumnIndexes);
        LogicalType[] targetFieldTypes = SchemaUtil.getLogicalTypes(tableSchema, targetFieldNames);
        Map<String, LogicalType> pks = new HashMap<>();
        tableSchema
                .getPrimaryKey()
                .map(UniqueConstraint::getColumns)
                .orElse(Collections.emptyList())
                .forEach(
                        name ->
                                pks.put(
                                        name,
                                        tableSchema
                                                .getFieldDataType(name)
                                                .orElseThrow(IllegalArgumentException::new)
                                                .getLogicalType()));
        this.targetFieldNames = targetFieldNames;
        this.targetFieldTypes = targetFieldTypes;
        this.primarykeys = pks;
        this.targetColumnIndexs = targetColumnIndexes;
        this.shardCount = shardCount;
        this.distributionKeyIndexes = distributionKeyIndexes;
        this.routingFieldTypes = routingFieldTypes;
    }

    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }

    @Override
    @SuppressWarnings("unchecked")
    public DataStream<InputT> addPreWriteTopology(DataStream<InputT> input) {
        if (distributionKeyIndexes.length == 0) {
            return input;
        }
        DataStream<RowData> rowDataInput = (DataStream<RowData>) (DataStream<?>) input;
        return (DataStream<InputT>)
                (DataStream<?>)
                        rowDataInput.partitionCustom(
                                new HoloPartitioner(parallelism),
                                new HoloKeySelector<>(
                                        shardCount, distributionKeyIndexes, routingFieldTypes));
    }

    @Override
    public SinkWriter<InputT> createWriter(InitContext initContext) throws IOException {
        return new HologresSinkWriter<>(
                param,
                targetFieldNames,
                targetFieldTypes,
                targetColumnIndexs,
                primarykeys,
                shardCount,
                distributionKeyIndexes,
                routingFieldTypes,
                initContext);
    }
}
