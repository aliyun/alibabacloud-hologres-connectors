/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.utils.Tuple;

import java.sql.PreparedStatement;

/**
 * preparedStatement，是否需要batchExecute.
 */
public class PreparedStatementWithBatchInfo extends Tuple<PreparedStatement, Boolean> {
	long byteSize;
	int batchCount;
	Put.MutationType type;
	public PreparedStatementWithBatchInfo(PreparedStatement preparedStatement, Boolean isBatch, Put.MutationType type) {
		super(preparedStatement, isBatch);
		this.type = type;
	}

	public Put.MutationType getType() {
		return type;
	}

	public void setType(Put.MutationType type) {
		this.type = type;
	}

	public int getBatchCount() {
		return batchCount;
	}

	public void setBatchCount(int batchCount) {
		this.batchCount = batchCount;
	}

	public long getByteSize() {
		return byteSize;
	}

	public void setByteSize(long byteSize) {
		this.byteSize = byteSize;
	}
}
