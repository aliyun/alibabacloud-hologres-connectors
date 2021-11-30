/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.collector.shard;

import com.alibaba.hologres.client.model.Record;

/**
 * shard策略.
 */
public interface ShardPolicy {

	void init(int shardCount);

	int locate(Record record);

}
