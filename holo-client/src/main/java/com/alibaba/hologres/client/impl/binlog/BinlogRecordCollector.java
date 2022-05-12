/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.binlog;

import com.alibaba.hologres.client.model.binlog.BinlogRecord;

/**
 * 数据管道.
 */
public interface BinlogRecordCollector {

	/**
	 * size == 0 表示追上了.
	 * 此处list并不会根据ignoreDelete和ingnoreUpdateBefore，需要接受者自行处理.
	 *
	 * @param shardId
	 * @param array   array会被不断peek，pop
	 * @return 最后一条写成功的
	 * @throws InterruptedException
	 */
	BinlogRecord emit(int shardId, ArrayBuffer<BinlogRecord> array) throws InterruptedException;

	/**
	 * 捕捉到异常喂进来以后，worker那边就不会再工作了.
	 *
	 * @param e
	 */
	void exceptionally(int shardId, Throwable e);
}
