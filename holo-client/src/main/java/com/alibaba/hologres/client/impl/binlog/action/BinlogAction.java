/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.binlog.action;

import com.alibaba.hologres.client.impl.action.AbstractAction;
import com.alibaba.hologres.client.impl.binlog.BinlogRecordCollector;
import com.alibaba.hologres.client.impl.binlog.TableSchemaSupplier;
import com.alibaba.hologres.client.utils.Tuple;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * Binlog消费请求.
 */
public class BinlogAction extends AbstractAction<Void> {
	final String tableName;
	final int shardId;
	final long lsn;
	final String timestamp;
	final BinlogRecordCollector collector;
	final String slotName;
	final TableSchemaSupplier supplier;
	//Reader通过这个queue把flush请求发过去，worker搞定了以后通过future通知reader
	final Queue<Tuple<CompletableFuture<Void>, Long>> commitJob;

	// hologres 2.1版本起，可以不创建slot，仅使用表名消费。同时也不需要commit
	public BinlogAction(String tableName, int shardId, long lsn, String timestamp, BinlogRecordCollector collector, TableSchemaSupplier supplier) {
		this(tableName, null, shardId, lsn, timestamp, collector, supplier, null);
	}

	public BinlogAction(String tableName, String slotName, int shardId, long lsn, String timestamp, BinlogRecordCollector collector, TableSchemaSupplier supplier, Queue<Tuple<CompletableFuture<Void>, Long>> commitJob) {
		this.tableName = tableName;
		this.slotName = slotName;
		this.shardId = shardId;
		this.lsn = lsn;
		this.timestamp = timestamp;
		this.collector = collector;
		this.supplier = supplier;
		this.commitJob = commitJob;
	}

	public String getTableName() {
		return tableName;
	}

	public String getSlotName() {
		return slotName;
	}

	public int getShardId() {
		return shardId;
	}

	public long getLsn() {
		return lsn;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public BinlogRecordCollector getCollector() {
		return collector;
	}

	public TableSchemaSupplier getSupplier() {
		return supplier;
	}

	public Queue<Tuple<CompletableFuture<Void>, Long>> getCommitJob() {
		return commitJob;
	}
}
