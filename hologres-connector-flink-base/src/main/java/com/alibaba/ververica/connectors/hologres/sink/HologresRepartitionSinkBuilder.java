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

package com.alibaba.ververica.connectors.hologres.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.sink.v2.HologresSink;
import com.alibaba.ververica.connectors.hologres.utils.SchemaUtil;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.ververica.connectors.hologres.sink.HologresStreamPartitioner.partition;

/**
 * HologresRepartitionSinkBuilder.
 *
 * <p>Receive the upstream datastream input and construct the datastream sink output after
 * Repartition based on the distribution key of holo.
 */
public class HologresRepartitionSinkBuilder {

    private final HologresConnectionParam param;
    protected final String[] fieldNames;

    protected final LogicalType[] fieldTypes;
    protected final Map<String, LogicalType> primarykeys;
    private final HologresTableSchema hologresTableSchema;
    private final Integer[] distributionKeyIndexes;
    private DataStream<RowData> input;
    @Nullable protected Integer parallelism;

    public HologresRepartitionSinkBuilder(
            HologresConnectionParam param,
            TableSchema tableSchema,
            HologresTableSchema hologresTableSchema) {
        this.param = param;
        String[] fieldNames = tableSchema.getFieldNames();
        LogicalType[] fieldTypes = SchemaUtil.getLogicalTypes(tableSchema);
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
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.primarykeys = pks;
        this.hologresTableSchema = hologresTableSchema;
        this.distributionKeyIndexes = getKeyIndexes(tableSchema);
    }

    /** From {@link DataStream} with {@link RowData}. */
    public HologresRepartitionSinkBuilder forRowData(DataStream<RowData> input) {
        this.input = input;
        return this;
    }

    /** Set sink parallelism. */
    public HologresRepartitionSinkBuilder parallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public DataStreamSink<?> build() {
        DataStream<RowData> partitioned =
                partition(
                        input,
                        fieldNames,
                        fieldTypes,
                        hologresTableSchema.getShardCount(),
                        distributionKeyIndexes);
        DataStreamSink<RowData> sink =
                partitioned.sinkTo(new HologresSink<>(param, fieldNames, fieldTypes, primarykeys));
        if (parallelism != null) {
            sink.setParallelism(parallelism);
        }

        return sink;
    }

    private Integer[] getKeyIndexes(TableSchema tableSchema) {
        Map<String, Integer> columnNameToIndex = new HashMap<>();
        for (int i = 0; i < tableSchema.getFieldCount(); i++) {
            columnNameToIndex.put(tableSchema.getFieldNames()[i], i);
        }
        int[] distributionKeyIndexesInHolo = hologresTableSchema.get().getDistributionKeyIndex();
        Integer[] distributionKeyIndexesInFlink = new Integer[distributionKeyIndexesInHolo.length];
        for (int i = 0; i < distributionKeyIndexesInHolo.length; i++) {
            distributionKeyIndexesInFlink[i] =
                    columnNameToIndex.get(
                            hologresTableSchema
                                    .get()
                                    .getColumn(distributionKeyIndexesInHolo[i])
                                    .getName());
        }
        return distributionKeyIndexesInFlink;
    }
}
