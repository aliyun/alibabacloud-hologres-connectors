/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.action;

import com.alibaba.hologres.client.impl.collector.BatchState;
import com.alibaba.hologres.client.model.Record;

import java.util.List;

/**
 * pa.
 */
public class PutAction extends AbstractAction<Void> {

	final List<Record> recordList;
	final long byteSize;
	BatchState state;

	public PutAction(List<Record> recordList, long byteSize, BatchState state) {
		this.recordList = recordList;
		this.byteSize = byteSize;
		this.state = state;
	}

	public List<Record> getRecordList() {
		return recordList;
	}

	public long getByteSize() {
		return byteSize;
	}

	public BatchState getState() {
		return state;
	}
}
