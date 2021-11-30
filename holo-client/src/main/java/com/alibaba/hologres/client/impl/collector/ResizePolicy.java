/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.collector;

import com.alibaba.hologres.client.HoloConfig;
import org.postgresql.model.TableName;

/**
 * Resize策略.
 */
public interface ResizePolicy {

	void init(HoloConfig config);

	/**
	 * 计算一个表新的shard数.
	 * @param tableName 表名
	 * @param stat collector统计信息
	 * @param currentSize 当前的shard数
	 * @param workerCount 当前的worker数
	 * @param currentNano 当前JVM纳秒数
	 * @return 新的shard数
	 */
	int calculate(TableName tableName, CollectorStatistics stat, int currentSize, int workerCount, long currentNano);
}
