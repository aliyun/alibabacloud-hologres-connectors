/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.binlog;

import com.alibaba.hologres.client.utils.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 用来提交lsn的.
 */
@Deprecated
public class Committer {
	public static final Logger LOGGER = LoggerFactory.getLogger(Committer.class);

	final BlockingQueue<Tuple<CompletableFuture<Void>, Long>> queue;

	long lastReadLsn = -1;

	public Committer(BlockingQueue<Tuple<CompletableFuture<Void>, Long>> queue) {
		this.queue = queue;
	}

	public void updateLastReadLsn(long lastReadLsn) {
		this.lastReadLsn = lastReadLsn;
	}

	public CompletableFuture<Void> commit(long timeout) throws InterruptedException, TimeoutException {
		return commit(lastReadLsn, timeout);
	}

	public CompletableFuture<Void> commit(long lsn, long timeout) throws InterruptedException, TimeoutException {
		CompletableFuture<Void> future = new CompletableFuture<>();
		if (lsn < 0) {
			LOGGER.info("last read lsn {} < 0, skip commit it",  lsn);
			future.complete(null);
			return future;
		}
		boolean ret = queue.offer(new Tuple<>(future, lsn), timeout, TimeUnit.MILLISECONDS);
		if (!ret) {
			throw new TimeoutException();
		} else {
			return future;
		}
	}

	public long getLastReadLsn() {
		return lastReadLsn;
	}
}
