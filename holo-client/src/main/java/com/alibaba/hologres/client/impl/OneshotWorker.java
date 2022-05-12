/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.impl.action.AbstractAction;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 只跑一次的worker，没有while.
 */
public class OneshotWorker extends Worker {

	public OneshotWorker(HoloConfig config, AtomicBoolean started, int index, boolean isShardEnv) {
		super(config, started, index, isShardEnv);
	}

	@Override
	public void run() {
		LOGGER.info("worker:{} start", this);
		try {
			AbstractAction action = recordCollector.get(2000L, TimeUnit.MILLISECONDS);
			/*
			 * 每个循环做2件事情：
			 * 1 有action就执行action
			 * 2 根据connectionMaxIdleMs释放空闲connection
			 * */
			if (null != action) {
				try {
					handle(action);
				} finally {
					recordCollector.clear();
				}
			} else {

			}
		} catch (Throwable e) {
			LOGGER.error("should not happen", e);
		}
		LOGGER.info("worker:{} stop", this);
		connectionHolder.close();
	}
}
