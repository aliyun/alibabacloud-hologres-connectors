/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model;

import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.copy.CopyContext;
import org.postgresql.jdbc.TimestampUtils;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * Export的结果.
 */
public class ExportContext {

	private CompletableFuture<Long> totalRowCount;
	private CompletableFuture<Long>[] rowCounts;
	private InputStream[] istreams;
	private CopyContext[] copyContexts;

	public ExportContext(CompletableFuture<Long>[] rowCounts, CopyContext[] copyContexts, InputStream[] istreams) {
		this.rowCounts = rowCounts;
		this.istreams = istreams;
		this.copyContexts = copyContexts;
		this.totalRowCount = CompletableFuture.allOf(rowCounts).thenApply(ignore -> Stream.of(rowCounts).mapToLong(CompletableFuture::join).sum());
	}

	public CompletableFuture<Long> getRowCount() {
		return totalRowCount;
	}

	public InputStream getInputStream(int n) {
		return istreams[n];
	}

	public int getThreadSize() {
		return istreams.length;
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
}
