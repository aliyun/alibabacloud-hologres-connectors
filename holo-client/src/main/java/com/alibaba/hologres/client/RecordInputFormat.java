/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.RecordReader;
import com.alibaba.hologres.client.model.ExportContext;
import com.alibaba.hologres.client.model.Record;
import org.postgresql.jdbc.TimestampUtils;
import org.postgresql.model.TableSchema;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RecordInputStream.
 */
public class RecordInputFormat {

	private final TableSchema schema;

	private final TimestampUtils timestampUtils;

	private final ExportContext exportContext;

	public RecordInputFormat(ExportContext exportContext, TableSchema schema) throws IOException {
		this.exportContext = exportContext;
		this.schema = schema;
		this.threadSize = exportContext.getThreadSize();
		this.numOpened = new AtomicInteger(threadSize);
		this.timestampUtils = exportContext.getTimestampUtils();
		this.queue = new ArrayBlockingQueue<Record>(1024);
		this.start();
	}

	int threadSize;
	AtomicInteger numOpened;
	BlockingQueue<Record> queue;

	ExecutorService threadPool =  Executors.newCachedThreadPool();

	//call getRecord until null or call RecordInputFormat.cancel() to interrupt
	public Record getRecord() {
		while (numOpened.get() > 0 || !queue.isEmpty()) {
			Record r;
			try {
				r = queue.poll(100, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				return null;
			}
			if (r != null) {
				return r;
			}
		}
		threadPool.shutdown();
		return null;
	}

	private void start() {
		for (int i = 0; i < threadSize; i++) {
			threadPool.execute(new RecordReader(exportContext.getInputStream(i), schema, queue, numOpened, timestampUtils));
		}
	}

	public void cancel() throws HoloClientException {
		threadPool.shutdownNow();
		numOpened.set(0);
		exportContext.cancel();
	}

}
