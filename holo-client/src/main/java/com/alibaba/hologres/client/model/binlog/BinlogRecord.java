/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model.binlog;

import com.alibaba.hologres.client.impl.binlog.BinlogEventType;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;

import java.io.Serializable;

/**
 * Binlog Record, 包含binlog相关参数.
 */
public class BinlogRecord extends Record implements Serializable {
	private final long lsn;
	private final BinlogEventType eventType;
	private final long timestamp;

	public BinlogRecord(TableSchema schema, long lsn, BinlogEventType eventType, long timestamp) {
		super(schema);
		this.lsn = lsn;
		this.eventType = eventType;
		this.timestamp = timestamp;
	}

	public long getBinlogLsn() {
		return lsn;
	}

	public BinlogEventType getBinlogEventType() {
		return eventType;
	}

	public long getBinlogTimestamp() {
		return timestamp;
	}

	public boolean isHeartBeat() {
		return false;
	}
}
