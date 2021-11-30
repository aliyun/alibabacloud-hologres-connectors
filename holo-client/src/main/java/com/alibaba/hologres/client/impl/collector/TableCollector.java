/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.collector;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.exception.HoloClientWithDetailsException;
import com.alibaba.hologres.client.impl.ExecutionPool;
import com.alibaba.hologres.client.impl.collector.shard.DistributionKeyShardPolicy;
import com.alibaba.hologres.client.impl.collector.shard.ShardPolicy;
import com.alibaba.hologres.client.model.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * PutAction收集器（表级别）.
 * 每一个HoloClient对应一个ActionClient
 * ActionCollector
 * - TableCollector PutAction收集器（表级别）
 * - TableShardCollector PutAction收集器（shard级别，此shard和holo的shard是2个概念，仅代表客户端侧的数据攒批的分区）
 * - queue  GetAction的队列
 */
public class TableCollector {
	public static final Logger LOG = LoggerFactory.getLogger(TableCollector.class);

	private TableShardCollector[] pairArray;
	private final HoloConfig config;
	private final ExecutionPool pool;
	private CollectorStatistics stat;
	private final ShardPolicy shardPolicy;
	private final long recordSampleInterval; //nano
	private long lastSampleTime = 0L;

	public TableCollector(HoloConfig config, ExecutionPool pool) {
		this.config = config;
		this.pool = pool;
		stat = new CollectorStatistics();
		this.shardPolicy = new DistributionKeyShardPolicy();
		this.recordSampleInterval = config.getRecordSampleInterval() * 1000000L;
		initTableShardCollector(config.getWriteThreadSize());
	}

	private void initTableShardCollector(int size) {
		pairArray = new TableShardCollector[size];
		for (int i = 0; i < pairArray.length; ++i) {
			pairArray[i] = new TableShardCollector(config, pool, stat, pairArray.length);
		}
		shardPolicy.init(size);
	}

	public void resize(int size) {
		if (pairArray.length != size) {
			initTableShardCollector(size);
		}
	}

	public long getByteSize() {
		return Arrays.stream(pairArray).collect(Collectors.summingLong(TableShardCollector::getByteSize));
	}

	public void append(Record record) throws HoloClientException {
		long nano = System.nanoTime();
		if (recordSampleInterval > 0 && nano - lastSampleTime > recordSampleInterval) {
			Object attachmentObj = null;
			try {
				if (record.getAttachmentList() != null && record.getAttachmentList().size() > 0) {
					attachmentObj = record.getAttachmentList().get(0);
				}
				LOG.info("sample data: table name={}, record={}, attachObj={}", record.getSchema().getTableNameObj(), record, attachmentObj);
			} catch (Exception e) {
				LOG.warn("sample data fail", e);
			}
			lastSampleTime = nano;
		}
		int index = shardPolicy.locate(record);
		pairArray[index].append(record);
	}

	public boolean flush(boolean force) throws HoloClientException {
		return flush(force, true);
	}

	public boolean flush(boolean force, boolean async) throws HoloClientException {
		return flush(force, async, null);
	}

	public boolean flush(boolean force, boolean async, AtomicInteger uncommittedActionCount) throws HoloClientException {
		HoloClientWithDetailsException exception = null;

		int doneCount = 0;

		for (TableShardCollector pair : pairArray) {
			try {
				doneCount += pair.flush(force, async, uncommittedActionCount) ? 1 : 0;
			} catch (HoloClientWithDetailsException e) {
				if (exception == null) {
					exception = e;
				} else {
					exception.merge(e);
				}
			}
		}

		if (exception != null) {
			throw exception;
		}
		return doneCount == pairArray.length;
	}

	public int getShardCount() {
		return pairArray.length;
	}

	public CollectorStatistics getStat() {
		return stat;
	}
}
