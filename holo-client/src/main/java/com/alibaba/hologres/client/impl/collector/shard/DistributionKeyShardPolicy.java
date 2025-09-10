/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.collector.shard;

import com.alibaba.hologres.client.impl.util.ShardUtil;
import com.alibaba.hologres.client.model.Record;

import java.util.concurrent.ConcurrentSkipListMap;

/** 使用distribution key去做shard,没有distribution key则随机. */
public class DistributionKeyShardPolicy implements ShardPolicy {

    private ConcurrentSkipListMap<Integer, Integer> splitRange = new ConcurrentSkipListMap<>();

    @Override
    public void init(int shardCount) {
        splitRange.clear();
        int[][] range = ShardUtil.split(shardCount);
        for (int i = 0; i < shardCount; ++i) {
            splitRange.put(range[i][0], i);
        }
    }

    @Override
    public int locate(Record record) {
        int raw = ShardUtil.hash(record, record.getSchema().getDistributionKeyIndex());
        int hash = Integer.remainderUnsigned(raw, ShardUtil.RANGE_END);
        return splitRange.floorEntry(hash).getValue();
    }
}
