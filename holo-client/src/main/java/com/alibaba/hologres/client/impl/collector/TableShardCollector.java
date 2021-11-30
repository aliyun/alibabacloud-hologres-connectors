/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.collector;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.exception.HoloClientWithDetailsException;
import com.alibaba.hologres.client.impl.ExecutionPool;
import com.alibaba.hologres.client.impl.action.PutAction;
import com.alibaba.hologres.client.model.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * PutAction收集器（shard级别）.
 * 每一个HoloClient对应一个ActionClient
 * ActionCollector
 * - TableCollector PutAction收集器（表级别）
 * - TableShardCollector PutAction收集器（shard级别，此shard和holo的shard是2个概念，仅代表客户端侧的数据攒批的分区）
 * - queue  GetAction的队列
 */
public class TableShardCollector {
	public static final Logger LOGGER = LoggerFactory.getLogger(TableShardCollector.class);

	private RecordCollector buffer;
	private PutAction activeAction;
	private long activeActionByteSize = 0L;
	private final ExecutionPool pool;
	private final CollectorStatistics stat;

	public TableShardCollector(HoloConfig config, ExecutionPool pool, CollectorStatistics stat, int size) {
		buffer = new RecordCollector(config, pool, size);
		activeAction = null;
		this.pool = pool;
		this.stat = stat;
	}

	public synchronized void append(Record record) throws HoloClientException {
		boolean full = buffer.append(record);
		if (full) {
			HoloClientException exception = null;
			try {
				waitActionDone();
			} catch (HoloClientException e) {
				exception = e;
			}
			commit(buffer.getBatchState());
			if (exception != null) {
				throw exception;
			}
		} else {
			try {
				isActionDone();
			} catch (HoloClientException e) {
				throw e;
			}
		}
	}

	private void commit(BatchState state) throws HoloClientException {
		stat.add(state);
		activeAction = new PutAction(buffer.getRecords(), buffer.getByteSize(), state);
		try {
			while (!pool.submit(activeAction)) {
			}
			activeActionByteSize = activeAction.getByteSize();
		} catch (Exception e) {
			activeAction.getFuture().completeExceptionally(e);
			if (activeAction.getRecordList() != null) {
				for (Record record : activeAction.getRecordList()) {
					if (record.getPutFutures() != null) {
						for (CompletableFuture<Void> future : record.getPutFutures()) {
							if (!future.isDone()) {
								future.completeExceptionally(e);
							}
						}
					}
				}
			}
			if (!(e instanceof HoloClientException)) {
				throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "", e);
			} else {
				throw e;
			}
		} finally {
			buffer.clear();
		}

	}

	private void clearActiveAction() {
		activeAction = null;
		activeActionByteSize = 0L;
	}

	private void waitActionDone() throws HoloClientException {
		if (activeAction != null) {
			try {
				activeAction.getFuture().get();
			} catch (ExecutionException e) {
				Throwable cause = e.getCause();
				if (cause instanceof HoloClientException) {
					throw (HoloClientException) cause;
				} else {
					throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "unknow exception", cause);
				}
			} catch (InterruptedException e) {
			} finally {
				clearActiveAction();
			}
		}
	}

	private boolean isActionDone() throws HoloClientException {
		if (activeAction != null) {
			try {
				if (activeAction.getFuture().isDone()) {
					//LOGGER.info("Pair Done:{}",readable.getPutAction().getFuture());
					try {
						activeAction.getFuture().get();
					} finally {
						clearActiveAction();
					}
					return true;
				} else {
					return false;
				}
			} catch (ExecutionException e) {
				Throwable cause = e.getCause();
				clearActiveAction();
				if (cause instanceof HoloClientException) {
					throw (HoloClientException) cause;
				} else {
					throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "unknow exception", cause);
				}
			} catch (InterruptedException e) {
				return false;
			}
		} else {
			return true;
		}
	}

	/**
	 * 是否flush完成.
	 *
	 * @param force                  是否强制flush，强制flush只要buffer.size > 0就一定提交，否则还是看RecordCollector自己判断是不是应该提交
	 * @param async                  是否异步，同步的话，对于activeAction会wait到完成为止
	 * @param uncommittedActionCount 如果activeAction未完成，并且buffer.size > 0 ，加一，表示还有任务没有提交給worker
	 * @return true, 没有任何pending的记录
	 * @throws HoloClientException 异常
	 */
	public synchronized boolean flush(boolean force, boolean async, AtomicInteger uncommittedActionCount) throws HoloClientException {
		HoloClientWithDetailsException failedRecords = null;

		boolean readableDone = false;
		try {
			if (async) {
				readableDone = isActionDone();
			} else {
				readableDone = true;
				waitActionDone();
			}
		} catch (HoloClientWithDetailsException e) {
			readableDone = true;
			failedRecords = e;
		}
		boolean done = false;
		if (readableDone) {
			if (buffer.size > 0) {
				BatchState state = force ? BatchState.Force : buffer.getBatchState();
				if (state != BatchState.NotEnough) {
					commit(state);
				}
			} else {
				done = true;
			}
		} else if (uncommittedActionCount != null) {
			if (buffer.size > 0) {
				uncommittedActionCount.incrementAndGet();
			}
		}

		if (failedRecords != null) {
			throw failedRecords;
		}

		return done;
	}

	public long getByteSize() {
		return activeActionByteSize + buffer.getByteSize();
	}
}
