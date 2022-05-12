/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.collector;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.utils.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 默认Resize策略.
 * 默认策略是假定调用方会定时调用flush方法，
 */
public class DefaultResizePolicy implements ResizePolicy {

	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultResizePolicy.class);

	private int maxShardCount;

	@Override
	public void init(HoloConfig config) {
		this.maxShardCount = config.getMaxShardCount();
	}

	@Override
	public int calculate(TableName tableName, CollectorStatistics stat, int currentSize, int workerCount, long currentNano) {
		int fullCount = stat.getFullBatchCount();
		int notFullCount = stat.getNotFullBatchCount();
		//最近1分钟，每1s花在write上的时间是多少ms，越大说明越饱和
		int load = (int) Metrics.registry().meter(Metrics.METRICS_WRITE_COST_MS_ALL).getOneMinuteRate() / workerCount;

		int maxSize = maxShardCount > 0 ? maxShardCount : workerCount * 2;

		int newSize = currentSize;

		if (fullCount == 0 /*压根没有攒够批的就缩*/) {
			newSize = currentSize > 1 ? currentSize / 2 : 1;
		} else if (fullCount * 2 <= notFullCount /* 满批太少了*/) {
			newSize = currentSize > 1 ? currentSize - 1 : currentSize;
		} else if (fullCount > notFullCount * 10 && load < 850 /*满批非常多，并且load也还行的话就扩*/) {
			newSize = (currentSize + maxSize) / 2;
		} else if (fullCount > notFullCount * 3 && load < 850 /*满批很多，并且load也还行的话就扩*/) {
			newSize = currentSize >= maxSize ? currentSize : maxSize + 1;
		}
		if (newSize != currentSize) {
			LOGGER.info("table {} size change {}->{}, fullCount = {}, notFullCount = {}, load = {} ", tableName, currentSize, newSize, fullCount, notFullCount, load);
		}
		return newSize;
	}
}
