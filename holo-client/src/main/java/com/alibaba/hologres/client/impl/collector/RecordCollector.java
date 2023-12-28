/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.collector;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.impl.ExecutionPool;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.RecordKey;
import com.alibaba.hologres.client.model.WriteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 将多条record记录在内存中进行聚合.
 */
public class RecordCollector {

	public static final Logger LOGGER = LoggerFactory.getLogger(RecordCollector.class);

	private final WriteMode mode;
	private final int maxRecords;
	private final long maxByteSize;
	private final long maxWaitTime;

	private final int shardCount;
	private ExecutionPool pool;

	public RecordCollector(HoloConfig config, ExecutionPool pool, int shardCount) {
		this.mode = config.getWriteMode();
		this.maxRecords = config.getWriteBatchSize();
		this.maxByteSize = config.getWriteBatchByteSize();
		this.maxWaitTime = config.getWriteMaxIntervalMs();
		this.pool = pool;
		this.shardCount = shardCount;
	}

	Map<RecordKey, Record> deleteRecords = new HashMap<>();
	Map<RecordKey, Record> records = new HashMap<>();

	int size = 0;
	long byteSize = 0L;
	long startTimeMs = -1L;

	public boolean append(Record record) {
		if (startTimeMs == -1) {
			startTimeMs = System.currentTimeMillis();
		}
		Map<RecordKey, Record> recordMap = records;
		Map<RecordKey, Record> deleteMap = deleteRecords;

		RecordKey key = new RecordKey(record);
		Record origin = recordMap.get(key);
		if (origin != null) {
			switch (record.getType()) {
				case DELETE:
					Record deleteRecord = deleteMap.get(key);
					/*
					 * 如果delete列表没有这个key
					 * record.attachmentList=origin.attachmentList+record.attachmentList
					 * 否则
					 * record.attachmentList=deleteRecord.attachmentList+origin.attachmentList+record.attachmentList
					 * */
					if (null != deleteRecord) {
						size += -1;
						byteSize -= deleteRecord.getByteSize();
						origin.cover(deleteRecord);
					}
					record.cover(origin);
					recordMap.remove(key);
					byteSize -= origin.getByteSize();
					byteSize += record.getByteSize();
					deleteMap.put(key, record);
					break;
				case INSERT:
					switch (mode) {
						case INSERT_OR_UPDATE:
							byteSize -= origin.getByteSize();
							origin.merge(record);
							byteSize += origin.getByteSize();
							origin.setType(Put.MutationType.INSERT);
							break;
						case INSERT_OR_IGNORE:
							origin.addAttachmentList(record.getAttachmentList());
							break;
						case INSERT_OR_REPLACE:
							record.cover(origin);
							byteSize -= origin.getByteSize();
							byteSize += record.getByteSize();
							recordMap.put(key, record);
							break;
					}
					break;
			}
		} else {
			Record baseRecord;
			switch (record.getType()) {
				case DELETE:
					baseRecord = deleteMap.get(key);
					if (baseRecord == null) {
						size += 1;
					} else {
						byteSize -= baseRecord.getByteSize();
						record.cover(baseRecord);
					}
					byteSize += record.getByteSize();
					deleteMap.put(key, record);
					break;
				case INSERT:
					byteSize += record.getByteSize();
					recordMap.put(key, record);
					if (mode == WriteMode.INSERT_OR_REPLACE) {
						baseRecord = deleteMap.get(key);
						if (baseRecord == null) {
							size += 1;
						} else {
							byteSize -= baseRecord.getByteSize();
							record.cover(baseRecord);
							deleteMap.remove(key);
						}
					} else {
						++size;
					}
					break;
			}
		}
		BatchState bs = getBatchState();
		if (bs != BatchState.NotEnough) {
			return true;
		}
		return false;
	}

	public boolean isKeyExists(RecordKey key) {
		// 不论是delete还是insert，只要有相同的key，都将之前攒批的数据先flush掉
		return records.get(key) != null || deleteRecords.get(key) != null;
	}

	/**
	 * reason.
	 * 1 isSizeEnough
	 * 2 isByteSizeEnough
	 * 3 isTimeWaitEnough
	 * 4 timeCondition
	 * 5 byteSizeCondition
	 * 6 totalByteSizeCondition
	 * 7 force
	 * 8 retry one by one
	 */
	public BatchState getBatchState() {
		long afterLastCommit = System.currentTimeMillis() - startTimeMs;
		// 行数够多少条
		boolean isSizeEnough = size >= maxRecords;
		if (isSizeEnough) {
			return BatchState.SizeEnough;
		}
		// 大小够多少条
		boolean isByteSizeEnough = byteSize >= maxByteSize;
		if (isByteSizeEnough) {
			return BatchState.ByteSizeEnough;
		}
		boolean isTimeWaitEnough = startTimeMs > -1 && afterLastCommit >= maxWaitTime;
		if (isTimeWaitEnough) {
			return BatchState.TimeWaitEnough;
		}
		boolean isEarlyCommit = false;
		//当已经凑够2的指数时
		if (size > 0 && (size & (size - 1)) == 0) {
			// 已经过去了maxWaitTime 40%的时间，统计上来说，不能再翻倍，那就提早commit
			boolean timeCondition = startTimeMs > -1 && afterLastCommit * 5 > maxWaitTime * 2;
			if (timeCondition) {
				return BatchState.TimeCondition;
			}
			//当前行数的数据已经超过40%maxByteSize，可能不能再翻倍，那就提早commit
			boolean byteSizeCondition = byteSize * 5 > maxByteSize * 2;
			if (byteSizeCondition) {
				return BatchState.ByteSizeCondition;
			}
			//当当前的行数的数据超过1/4的剩余avaliable
			long availableByteSize = pool.getAvailableByteSize();
			boolean totalByteSizeCondition = byteSize * shardCount > availableByteSize;
			if (totalByteSizeCondition) {
				return BatchState.TotalByteSizeCondition;
			}
			isEarlyCommit = timeCondition
					|| byteSizeCondition
					|| totalByteSizeCondition;
			if (isEarlyCommit) {
				if (timeCondition) {
					LOGGER.debug("table {} earlyCommit[timeCondition].afterLastCommit({}) > 40% maxWaitTime({})", afterLastCommit, maxWaitTime);
				} else if (byteSizeCondition) {
					LOGGER.debug("table {} earlyCommit[byteSizeCondition].byteSize({}) > 40% maxByteSize({})", byteSize, maxByteSize);
				} else {
					LOGGER.debug("table {} earlyCommit[totalByteSizeCondition].afterLastCommit({}) > 40% availableByteSize({})", afterLastCommit, maxWaitTime);
				}
			}
		}
		return BatchState.NotEnough;
		//return isSizeEnough || isByteSizeEnough || isTimeWaitEnough || isEarlyCommit;
	}

	public int size() {
		return size;
	}

	public long getByteSize() {
		return byteSize;
	}

	/**
	 * @return 永远是先给delete，再给upsert
	 */
	public List<Record> getRecords() {
		List<Record> list = new ArrayList<>();
		list.addAll(deleteRecords.values());
		list.addAll(records.values());
		return list;
	}

	public WriteMode getMode() {
		return mode;
	}

	public void clear() {
		startTimeMs = -1;
		size = 0;
		byteSize = 0L;
		records.clear();
		deleteRecords.clear();
	}

}


