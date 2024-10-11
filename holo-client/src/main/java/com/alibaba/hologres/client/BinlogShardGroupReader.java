package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.binlog.BinlogEventType;
import com.alibaba.hologres.client.impl.binlog.BinlogRecordCollector;
import com.alibaba.hologres.client.impl.binlog.Committer;
import com.alibaba.hologres.client.model.binlog.BinlogRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * BinlogShardGroupReader 为一个范围的shard创建BinlogShardReader，并将各个reader返回的BinlogRecord放入queue.
 */
public class BinlogShardGroupReader implements Closeable {
	public static final Logger LOGGER = LoggerFactory.getLogger(BinlogShardGroupReader.class);

	private final HoloConfig config;
	private final Subscribe subscribe;
	private final Map<Integer, Committer> committerMap;
	private final AtomicBoolean started;

	BlockingQueue<BinlogRecord> queue;
	BinlogRecordCollector collector;

	List<Thread> threadList = new ArrayList<Thread>();

	public BinlogShardGroupReader(HoloConfig config, Subscribe subscribe, Map<Integer, Committer> committerMap, AtomicBoolean started, BinlogRecordCollector collector) {
		this.config = config;
		this.subscribe = subscribe;
		this.committerMap = committerMap;
		this.started = started;
		this.collector = collector;
		this.queue = collector.getQueue();
	}

	public BinlogShardGroupReader(HoloConfig config, Subscribe subscribe, AtomicBoolean started, BinlogRecordCollector collector) {
		this(config, subscribe, null, started, collector);
	}

	int bufferPosition = 0;
	List<BinlogRecord> buffer = new ArrayList<>();

	//当buffer消费完了就拿一批回来
	private void tryFetch(long target) throws InterruptedException, TimeoutException, HoloClientException {
		if (buffer.size() <= bufferPosition) {
			if (buffer.size() > 0) {
				buffer.clear();
			}
			BinlogRecord r = null;
			while (r == null) {
				if (null != collector.getException()) {
					throw collector.getException();
				}
				if (System.nanoTime() > target) {
					throw new TimeoutException();
				}
				r = queue.poll(1000, TimeUnit.MILLISECONDS);
				if (r != null) {
					buffer.add(r);
					queue.drainTo(buffer);
					bufferPosition = 0;
				}
			}
		}
	}

	public BinlogRecordCollector getCollector() {
		return collector;
	}

	/**
	 * Call getBinlogRecord until null or call BinlogShardGroupReader.cancel() to interrupt.
	 */
	public BinlogRecord getBinlogRecord() throws HoloClientException, InterruptedException, TimeoutException {
		return getBinlogRecord(config.getBinlogReadTimeoutMs());
	}

	/**
	 * Call getBinlogRecord until null or call BinlogShardGroupReader.cancel() to interrupt.
	 */
	public BinlogRecord getBinlogRecord(long timeoutMs) throws HoloClientException, InterruptedException, TimeoutException {
		if (null != collector.getException()) {
			throw collector.getException();
		}
		BinlogRecord r = null;

		long target = timeoutMs > 0 ? (System.nanoTime() + timeoutMs * 1000000L) : Long.MAX_VALUE;
		while (r == null) {
			tryFetch(target);
			if (buffer.size() > bufferPosition) {
				r = buffer.get(bufferPosition++);
			}
			if (r != null) {
				if (committerMap != null) {
					Committer committer = committerMap.get(r.getShardId());
					if (committer == null) {
						throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "reader for shard " + r.getShardId() + " is not exists!");
					}
					committer.updateLastReadLsn(r.getBinlogLsn());
                }
				if ((r.getBinlogEventType() == BinlogEventType.DELETE && config.getBinlogIgnoreDelete()) || (r.getBinlogEventType() == BinlogEventType.BEFORE_UPDATE && config.getBinlogIgnoreBeforeUpdate())) {
					r = null;
				}
			}
		}
		return r;
	}

	@Override
	public void close() {
		cancel();
	}

	@Deprecated
	public void commit(long timeoutMs) throws HoloClientException, TimeoutException, InterruptedException {
		if (committerMap == null) {
            throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "could not commit without committer");
        }
		List<CompletableFuture<Void>> futureList = new ArrayList<>();
		for (Map.Entry<Integer, Committer> entry : committerMap.entrySet()) {
			futureList.add(commitFlushedLsn(entry.getValue(), entry.getKey(), entry.getValue().getLastReadLsn(), timeoutMs));
		}
		long targetMs = System.currentTimeMillis() + timeoutMs;
		int index = 0;
		for (CompletableFuture<Void> future : futureList) {
			long currentMs = System.currentTimeMillis();
			if (currentMs < targetMs) {
				try {
					future.get(targetMs - currentMs, TimeUnit.MILLISECONDS);
				} catch (ExecutionException e) {
					Throwable cause = e.getCause();
					if (cause instanceof HoloClientException) {
						throw (HoloClientException) cause;
					} else {
						throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "commit fail", cause);
					}
				} catch (TimeoutException e) {
					throw e;
				}
			} else {
				throw new TimeoutException();
			}
		}
	}

	public CompletableFuture<Void> commitFlushedLsn(Committer committer, int shardId, long lsn, long timeoutMs) throws
			TimeoutException, InterruptedException {
		LOGGER.info("begin commit {} shardId {} flushedLsn to {}", subscribe.getTableName(), shardId, lsn);
		return committer.commit(lsn, timeoutMs).thenRun(() -> {
			LOGGER.info("done commit {} shardId {} flushedLsn to {}", subscribe.getTableName(), shardId, lsn);
		});
	}

	public void commitFlushedLsn(int shardId, long lsn, long timeoutMs) throws
			HoloClientException, TimeoutException, InterruptedException {
		Committer committer = committerMap.get(shardId);
		if (committer != null) {
			CompletableFuture<Void> future = commitFlushedLsn(committer, shardId, lsn, timeoutMs);
			try {
				future.get(timeoutMs, TimeUnit.MILLISECONDS);
			} catch (ExecutionException e) {
				Throwable cause = e.getCause();
				if (cause instanceof HoloClientException) {
					throw (HoloClientException) cause;
				} else {
					throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "commit fail", cause);
				}
			}
		} else {
			throw new HoloClientException(ExceptionCode.INVALID_REQUEST, "unknown shard " + shardId);
		}
	}

	//主要是为了在close的时候，确保thread都停了.
	public void addThread(Thread thread) {
		threadList.add(thread);
	}

	public void cancel() {
		started.set(false);
		for (Thread thread : threadList) {
			if (thread.isAlive()) {
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException ignore) {

				}
				thread.interrupt();
			}
		}
	}

	public boolean isCanceled() {
		return !started.get();
	}
}
