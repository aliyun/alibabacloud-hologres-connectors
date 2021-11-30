/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model;

import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.copy.CopyContext;
import org.postgresql.jdbc.TimestampUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * Import的结果.
 */
public class ImportContext {
	private NavigableMap<Integer, Integer> shardMap;
	private CompletableFuture<Long> totalRowCount;
	private CompletableFuture<Long>[] rowCounts;
	private OutputStream[] ostreams;
	private CopyContext[] copyContexts;
	private int shardCount;

	public ImportContext(NavigableMap<Integer, Integer> shardMap, CompletableFuture<Long>[] rowCounts, CopyContext[] copyContexts, OutputStream[] ostreams, int shardCount) {
		this.shardMap = shardMap;
		this.rowCounts = rowCounts;
		this.ostreams = ostreams;
		this.copyContexts = copyContexts;
		this.totalRowCount = CompletableFuture.allOf(rowCounts).thenApply(ignore -> Stream.of(rowCounts).mapToLong(CompletableFuture::join).sum());
		this.shardCount = shardCount;
	}

	public CompletableFuture<Long> getRowCount() {
		return totalRowCount;
	}

	public OutputStream getOutputStream(int shardId) {
		return ostreams[shardMap.floorEntry(shardId).getValue()];
	}

	public void cancel() throws HoloClientException {
		try {
			for (CopyContext copyContext : copyContexts) {
				copyContext.cancel();
			}
		} catch (SQLException e) {
			throw HoloClientException.fromSqlException(e);
		}
	}

	public TimestampUtils getTimestampUtils() {
		return copyContexts[0].getConn().getTimestampUtils();
	}

	public int getShardCount()  {
		return this.shardCount;
	}

	public void closeOstreams() throws IOException {
		for (OutputStream os : ostreams) {
			os.close();
		}
	}

}
