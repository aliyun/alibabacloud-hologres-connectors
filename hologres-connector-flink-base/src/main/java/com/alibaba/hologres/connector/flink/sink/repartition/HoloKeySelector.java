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

package com.alibaba.hologres.connector.flink.sink.repartition;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.concurrent.ConcurrentSkipListMap;

/**
 * HoloKeySelector.
 *
 * <p>According to the information of the distribution key field in Flink RowData, calculate the
 * shard where the holo is written.
 */
public class HoloKeySelector<InputT> implements KeySelector<InputT, Integer> {
    private final ConcurrentSkipListMap<Integer, Integer> splitRange =
            new ConcurrentSkipListMap<>();
    private final Integer[] keyIndexes;
    private final LogicalType[] fieldTypes;

    public HoloKeySelector(int shardCount, Integer[] keyIndexes, LogicalType[] fieldTypes) {
        int[][] range = RowDataShardUtil.split(shardCount);
        for (int i = 0; i < shardCount; ++i) {
            splitRange.put(range[i][0], i);
        }
        this.keyIndexes = keyIndexes;
        this.fieldTypes = fieldTypes;
    }

    /** 根据Row中的distribution key字段的信息，计算写入holo时所处的shard. */
    @Override
    public Integer getKey(InputT value) {
        if (value instanceof RowData) {
            int raw = RowDataShardUtil.hash((RowData) value, keyIndexes, fieldTypes);
            int hash = Integer.remainderUnsigned(raw, RowDataShardUtil.RANGE_END);
            return splitRange.floorEntry(hash).getValue();
        } else {
            throw new RuntimeException("HoloKeySelector only support RowData.");
        }
    }
}
