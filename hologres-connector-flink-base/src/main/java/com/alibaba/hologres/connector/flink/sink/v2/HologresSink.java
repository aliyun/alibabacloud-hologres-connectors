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
import org.apache.flink.table.types.logical.LogicalType;

import com.alibaba.hologres.connector.flink.api.HologresTableSchema;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.sink.repartition.HoloKeySelector;
import com.alibaba.hologres.connector.flink.sink.repartition.HoloPartitioner;
import com.alibaba.hologres.connector.flink.utils.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** A {@link Sink V2} for Hologres. */
public class HologresSink<InputT> implements Sink<InputT>, SupportsPreWriteTopology<InputT> {
    private static final Logger LOG = LoggerFactory.getLogger(HologresSink.class);

    private final HologresConnectionParam param;
    private final HologresTableSchema hologresTableSchema;
    private final Integer[] distributionKeyIndexes;

    // TableSchema is not Serialiaze, just get what we need from TableSchema;
    private final String[] targetFieldNames;
    private final LogicalType[] targetFieldTypes;
    private final Integer[] targetColumnIndexs;
    private final Map<String, LogicalType> primarykeys;

    // user can custom set sink parallelism, default null
    private Integer parallelism;

    public HologresSink(
            HologresConnectionParam param,
            TableSchema tableSchema,
            HologresTableSchema hologresTableSchema,
            Integer[] targetColumnIndexes) {
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
        this.hologresTableSchema = hologresTableSchema;
        this.distributionKeyIndexes = getDistributionKeyIndexesInFlink(tableSchema);
        this.targetFieldNames = targetFieldNames;
        this.targetFieldTypes = targetFieldTypes;
        this.primarykeys = pks;
        this.targetColumnIndexs = targetColumnIndexes;
    }

    @Override
    public SinkWriter<InputT> createWriter(InitContext initContext) throws IOException {
        return new HologresSinkWriter<>(
                param,
                targetFieldNames,
                targetFieldTypes,
                targetColumnIndexs,
                primarykeys,
                initContext);
    }

    @Override
    public DataStream<InputT> addPreWriteTopology(DataStream<InputT> input) {
        // 必须指定使用reshuffle参数
        if (!param.isEnableReshuffleByHolo()) {
            return input;
        }

        LOG.info(
                "PreWriteTopology: reShuffle by hologres distribution keys, input parallelism: {}, shardCount: {}, distribution keys: {}",
                input.getParallelism(),
                hologresTableSchema.getShardCount(),
                hologresTableSchema.get().getDistributionKeys());

        HoloKeySelector<InputT> keySelector =
                new HoloKeySelector<>(
                        hologresTableSchema.getShardCount(),
                        distributionKeyIndexes,
                        targetFieldTypes);
        HoloPartitioner partitioner = new HoloPartitioner(parallelism);
        return input.partitionCustom(partitioner, keySelector);
    }

    /**
     * Get distribution key indexes.
     *
     * @param tableSchema table schema
     * @return distribution key indexes
     */
    private Integer[] getDistributionKeyIndexesInFlink(TableSchema tableSchema) {
        Map<String, Integer> columnNameToIndex = new HashMap<>();
        for (int i = 0; i < tableSchema.getFieldCount(); i++) {
            columnNameToIndex.put(tableSchema.getFieldNames()[i], i);
        }

        String[] distributionKeys = hologresTableSchema.get().getDistributionKeys();
        Integer[] distributionKeyIndexesInFlink = new Integer[distributionKeys.length];
        int i = 0;
        for (String dk : distributionKeys) {
            distributionKeyIndexesInFlink[i++] = columnNameToIndex.get(dk);
        }
        return distributionKeyIndexesInFlink;
    }

    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }
}
