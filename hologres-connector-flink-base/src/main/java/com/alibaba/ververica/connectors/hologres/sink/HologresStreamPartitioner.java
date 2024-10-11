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

import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import com.alibaba.ververica.connectors.hologres.sink.repartition.HoloKeySelector;

import java.util.Arrays;

/**
 * A {@link StreamPartitioner} for Hologres.
 *
 * <p>Customize the selectChannel method to send data to the appropriate task.
 */
public class HologresStreamPartitioner extends StreamPartitioner<RowData> {
    protected final String[] keyNames;
    protected final HoloKeySelector keySelector;

    private HologresStreamPartitioner(
            int shardCount, String[] fieldNames, LogicalType[] fieldTypes, Integer[] keyIndexes) {
        this.keyNames = new String[keyIndexes.length];
        for (int i = 0; i < keyIndexes.length; i++) {
            keyNames[i] = fieldNames[keyIndexes[i]];
        }
        keySelector = new HoloKeySelector(shardCount, keyIndexes, fieldTypes);
    }

    @Override
    public StreamPartitioner<RowData> copy() {
        return this;
    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        return SubtaskStateMapper.FULL;
    }

    @Override
    public boolean isPointwise() {
        return false;
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<RowData>> record) {
        int shard = keySelector.getKey(record.getInstance().getValue());
        return shard % numberOfChannels;
    }

    @Override
    public String toString() {
        return "shuffle by holo distribution key: " + Arrays.toString(keyNames);
    }

    /** Partition the input stream by the distribution key. */
    public static DataStream<RowData> partition(
            DataStream<RowData> input,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            int shardCount,
            Integer[] keyIndexes) {
        HologresStreamPartitioner partitioner =
                new HologresStreamPartitioner(shardCount, fieldNames, fieldTypes, keyIndexes);
        PartitionTransformation<RowData> partitioned =
                new PartitionTransformation<>(input.getTransformation(), partitioner);
        return new DataStream<>(input.getExecutionEnvironment(), partitioned);
    }
}
