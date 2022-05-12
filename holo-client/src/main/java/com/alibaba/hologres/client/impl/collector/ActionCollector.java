/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.collector;

import com.alibaba.hologres.client.Get;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.exception.HoloClientWithDetailsException;
import com.alibaba.hologres.client.impl.ExecutionPool;
import com.alibaba.hologres.client.model.Partition;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.IdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Action收集器.
 * 每一个HoloClient对应一个ActionClient
 * ActionCollector
 * - TableCollector PutAction收集器（表级别）
 * - TableShardCollector PutAction收集器（shard级别，此shard和holo的shard是2个概念，仅代表客户端侧的数据攒批的分区）
 * - queue  GetAction的队列
 */
public class ActionCollector {
	private static final Logger LOGGER = LoggerFactory.getLogger(ActionCollector.class);
	Map<TableName, TableCollector> map;

	private ReentrantReadWriteLock flushLock = new ReentrantReadWriteLock();
	final HoloConfig config;
	final ExecutionPool pool;
	final ArrayBlockingQueue<Get> queue;

	private final ResizePolicy resizePolicy;
	private final long writerShardCountResizeIntervalNano;

	public ActionCollector(HoloConfig config, ExecutionPool pool, ArrayBlockingQueue<Get> queue) {
		map = new ConcurrentHashMap<>();
		this.config = config;
		this.pool = pool;
		this.queue = queue;
		this.resizePolicy = new DefaultResizePolicy();
		resizePolicy.init(config);
		this.writerShardCountResizeIntervalNano = config.getWriterShardCountResizeIntervalMs() * 1000000L;

	}

	public long getByteSize() {
		return map.values().stream().collect(Collectors.summingLong(TableCollector::getByteSize));
	}

	public void append(Record record) throws HoloClientException {
		flushLock.readLock().lock();
		try {
			TableCollector pairArray = map.computeIfAbsent(record.getSchema().getTableNameObj(), (tableName) -> new TableCollector(config, pool));
			pairArray.append(record);
			HoloClientException exception = lastException.getAndSet(null);
			if (null != exception) {
				throw exception;
			}
		} finally {
			flushLock.readLock().unlock();
		}
	}

	/**
	 * 当Get一张分区主表时，重写Get请求的TableSchema.
	 *
	 * @param get get请求
	 * @return true，跳过对get的处理
	 * 跳过有2种情况
	 * 1 分区不存在，那么complete null
	 * 2 获取分区或者根据分区信息获取TableSchema异常 那么complete exception
	 */
	private boolean rewriteForPartitionTable(Get get) {
		Record record = get.getRecord();
		TableSchema schema = record.getSchema();
		TableName tableName = schema.getTableNameObj();

		if (schema.isPartitionParentTable() && schema.getPartitionIndex() > -1) {
			try {
				Partition partition = pool.getOrSubmitPartition(tableName, String.valueOf(record.getObject(schema.getPartitionIndex())), Types.VARCHAR == schema.getColumn(schema.getPartitionIndex()).getType());
				if (partition == null) {
					//分区不存在，结果肯定是null
					get.getFuture().complete(null);
					return true;
				} else {
					TableSchema newSchema = pool.getOrSubmitTableSchema(TableName.valueOf(IdentifierUtil.quoteIdentifier(partition.getSchemaName(), true), IdentifierUtil.quoteIdentifier(partition.getTableName(), true)), false);
					record.changeToChildSchema(newSchema);
				}
			} catch (HoloClientException e) {
				get.getFuture().completeExceptionally(e);
				return true;
			}
		}
		return false;
	}

	public void appendGet(Get get) {
		get.setFuture(new CompletableFuture<>());
		if (get.isFullColumn()) {
			for (int i = 0; i < get.getRecord().getSchema().getColumnSchema().length; ++i) {
				if (!get.getRecord().isSet(i)) {
					get.getRecord().setObject(i, null);
				}
			}
		}
		if (rewriteForPartitionTable(get)) {
			return;
		}
		try {
			if (!queue.offer(get, 10000L, TimeUnit.MILLISECONDS)) {
				get.getFuture().completeExceptionally(new TimeoutException());
			}
		} catch (InterruptedException e) {
			get.getFuture().completeExceptionally(e);
		}
	}

	public void appendGet(List<Get> list) {
		for (Get get : list) {
			get.setFuture(new CompletableFuture<>());
			if (get.isFullColumn()) {
				for (int i = 0; i < get.getRecord().getSchema().getColumnSchema().length; ++i) {
					if (!get.getRecord().isSet(i)) {
						get.getRecord().setObject(i, null);
					}
				}
			}
		}
		try {

			long start = System.currentTimeMillis();
			boolean timeout = false;
			for (Iterator<Get> iter = list.iterator(); iter.hasNext(); ) {
				Get get = iter.next();
				if (timeout) {
					get.getFuture().completeExceptionally(new TimeoutException());
					continue;
				}
				if (rewriteForPartitionTable(get)) {
					continue;
				}
				if (!queue.offer(get, 10000L, TimeUnit.MILLISECONDS)) {
					get.getFuture().completeExceptionally(new TimeoutException());
					timeout = true;
				}
			}
		} catch (InterruptedException e) {
			for (Get get : list) {
				get.getFuture().completeExceptionally(e);
			}
		}
	}

	AtomicReference<HoloClientWithDetailsException> lastException = new AtomicReference<>(null);

	public void tryFlush() throws HoloClientException {
		flushLock.readLock().lock();
		try {
			for (Iterator<Map.Entry<TableName, TableCollector>> iter = map.entrySet().iterator(); iter.hasNext(); ) {
				TableCollector array = iter.next().getValue();
				try {
					array.flush(false);
				} catch (HoloClientWithDetailsException e) {
					lastException.accumulateAndGet(e, (lastOne, newOne) -> {
						if (lastOne == null) {
							return newOne;
						} else {
							return lastOne.merge(newOne);
						}
					});
				}
			}
		} finally {
			flushLock.readLock().unlock();
		}
	}

	public void flush(boolean internal) throws HoloClientException {
		flushLock.writeLock().lock();
		try {
			HoloClientWithDetailsException exception = null;
			int doneCount = 0;
			AtomicInteger uncommittedActionCount = new AtomicInteger(0);
			boolean async = true;
			while (true) {
				doneCount = 0;
				uncommittedActionCount.set(0);
				for (Iterator<Map.Entry<TableName, TableCollector>> iter = map.entrySet().iterator(); iter.hasNext(); ) {
					TableCollector array = iter.next().getValue();
					try {
						if (array.flush(true, async, uncommittedActionCount)) {
							++doneCount;
						}
					} catch (HoloClientWithDetailsException e) {
						if (null == exception) {
							exception = e;
						} else {
							exception.merge(e);
						}
					} catch (HoloClientException e) {
						throw e;
					}
				}
				if (doneCount == map.size()) {
					break;
				}
				if (uncommittedActionCount.get() == 0) {
					async = false;
				}
			}
			//此时所有的TableCollector的buffer都已经是空的了，根据统计信息尝试resize shard数
			resize();
			if (exception != null) {
				if (internal) {
					lastException.accumulateAndGet(exception, (lastOne, newOne) -> {
						if (lastOne == null) {
							return newOne;
						} else {
							return lastOne.merge(newOne);
						}
					});
				} else {
					HoloClientWithDetailsException last = lastException.getAndSet(null);
					if (null == last) {
						last = exception;
					} else {
						last.merge(exception);
					}
					throw last;
				}
			}
		} finally {
			flushLock.writeLock().unlock();
		}
	}

	private void resize() {
		long currentNano = System.nanoTime();
		for (Map.Entry<TableName, TableCollector> entry : map.entrySet()) {
			TableName tableName = entry.getKey();
			TableCollector tableCollector = entry.getValue();
			if (tableCollector.getStat().getNanoTime() + writerShardCountResizeIntervalNano < currentNano) {
				int currentSize = tableCollector.getShardCount();
				int size = resizePolicy.calculate(tableName, tableCollector.getStat(), tableCollector.getShardCount(), pool.getWorkerCount(), currentNano);
				if (currentSize != size) {
					LOGGER.info("resize table {} shard size , {} -> {}", tableName, currentSize, size);
					tableCollector.resize(size);
				}
				tableCollector.getStat().clear();
			}
		}
	}
}
