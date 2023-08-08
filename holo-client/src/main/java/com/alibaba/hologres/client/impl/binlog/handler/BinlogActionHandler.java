/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.binlog.handler;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.binlog.ArrayBuffer;
import com.alibaba.hologres.client.impl.binlog.BinlogEventType;
import com.alibaba.hologres.client.impl.binlog.BinlogRecordCollector;
import com.alibaba.hologres.client.impl.binlog.HoloBinlogDecoder;
import com.alibaba.hologres.client.impl.binlog.action.BinlogAction;
import com.alibaba.hologres.client.impl.handler.ActionHandler;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.binlog.BinlogHeartBeatRecord;
import com.alibaba.hologres.client.model.binlog.BinlogRecord;
import com.alibaba.hologres.client.utils.Tuple;
import org.postgresql.PGProperty;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * binlog消费的处理类.
 * 因为binlog消费的特殊性，就抛弃ConnectionHolder，单独跑了.
 */
public class BinlogActionHandler extends ActionHandler<BinlogAction> {

	public static final Logger LOG = LoggerFactory.getLogger(BinlogActionHandler.class);

	final Properties info;
	final String originalUrl;
	final int binlogReadBatchSize;
	final int maxRetryCount;
	final boolean binlogIgnoreBeforeUpdate;
	final boolean binlogIgnoreDelete;
	final boolean isEnableDirectConnection;
	final long binlogHeartBeatIntervalMs;
	final AtomicBoolean started;
	final ArrayBuffer<BinlogRecord> binlogRecordArray;
	int retryCount;

	public BinlogActionHandler(AtomicBoolean started, HoloConfig config, boolean isShardEnv) {
		super(config);
		this.started = started;
		this.info = new Properties();
		PGProperty.USER.set(info, config.getUsername());
		PGProperty.PASSWORD.set(info, config.getPassword());
		PGProperty.ASSUME_MIN_SERVER_VERSION.set(info, "9.4");
		PGProperty.APPLICATION_NAME.set(info, "holo_client_replication");
		PGProperty.REPLICATION.set(info, "database");
		PGProperty.SOCKET_TIMEOUT.set(info, "120");
		String jdbcUrl = config.getJdbcUrl();
		if (isShardEnv) {
			if (jdbcUrl.startsWith("jdbc:postgresql:")) {
				jdbcUrl = "jdbc:hologres:" + jdbcUrl.substring("jdbc:postgresql:".length());
			}
		}
		this.originalUrl = jdbcUrl;
		this.binlogReadBatchSize = config.getBinlogReadBatchSize();
		this.maxRetryCount = config.getRetryCount();
		this.binlogIgnoreBeforeUpdate = config.getBinlogIgnoreBeforeUpdate();
		this.binlogIgnoreDelete = config.getBinlogIgnoreDelete();
		this.binlogHeartBeatIntervalMs = config.getBinlogHeartBeatIntervalMs();
		this.binlogRecordArray = new ArrayBuffer<>(binlogReadBatchSize, BinlogRecord[].class);
		this.isEnableDirectConnection = config.isEnableDirectConnection();
	}

	@Override
	public void handle(BinlogAction action) {
		doHandle(action);
	}

	class ConnectionContext {
		PgConnection conn = null;
		PGReplicationStream pgReplicationStream = null;
		private final BinlogAction action;
		private long startLsn;
		private String startTime;
		private long timestamp;

		public ConnectionContext(BinlogAction action, long emittedLsn, String startTime) {
			this.action = action;
			this.startLsn = emittedLsn;
			this.startTime = startTime;
			this.timestamp = -1;
		}

		public void setEmittedLsn(long emittedLsn, long timestamp) {
			this.startLsn = emittedLsn;
			this.timestamp = timestamp;
			startTime = null;
		}

		public void updateTimestamp(long timestamp) {
			this.timestamp = timestamp;
		}

		public long getTimestamp() {
			return this.timestamp;
		}

		public void init() throws SQLException {
			try {
				String url = originalUrl;
				if (isEnableDirectConnection) {
					url = ConnectionUtil.getDirectConnectionJdbcUrl(originalUrl, info);
				}
				this.conn = DriverManager.getConnection(url, info).unwrap(PgConnection.class);
				ChainedLogicalStreamBuilder logicalStreamBuilder = this.conn
						.getReplicationAPI()
						.replicationStream()
						.logical()
						.withSlotName(action.getSlotName())
						.withSlotOption("parallel_index", action.getShardId())
						.withSlotOption("batch_size", binlogReadBatchSize)
						.withStatusInterval(10, TimeUnit.SECONDS);
				if (startLsn > -1) {
					logicalStreamBuilder.withSlotOption("start_lsn", String.valueOf(startLsn));
				}
				if (startTime != null) {
					logicalStreamBuilder.withSlotOption("start_time", startTime);
				}
				LOG.info("shard {} start, start_lsn={}, start_time={}", action.getShardId(), startLsn, startTime);
				this.pgReplicationStream = logicalStreamBuilder.start();
			} catch (SQLException e) {
				close();
				throw e;
			}
		}

		public boolean isInit() {
			return conn != null;
		}

		public void close() {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException ignore) {

				}
				this.conn = null;
			}
			this.pgReplicationStream = null;
		}
	}

	private void resetRetryCount() {
		this.retryCount = this.maxRetryCount;
	}

	private void doHandle(BinlogAction action) {
		ConnectionContext connContext = new ConnectionContext(action, action.getLsn(), action.getTimestamp());
		HoloBinlogDecoder decoder = null;
		try {
			decoder = new HoloBinlogDecoder(action.getSupplier(), this.binlogIgnoreDelete, this.binlogIgnoreBeforeUpdate);
		} catch (HoloClientException e) {
			action.getCollector().exceptionally(action.getShardId(), e);
			return;
		}

		resetRetryCount();
		while (started.get()) {
			try {
				connContext.init();
				fetch(action.getShardId(), action.getCollector(), connContext, decoder, action.getCommitJob());
			} catch (SQLException e) {
				if (--retryCount < 1) {
					//失败太多了，结束
					action.getCollector().exceptionally(action.getShardId(), e);
					break;
				} else {
					LOG.warn("shardId " + action.getShardId() + " binlog read fail, retry", e);
					continue;
				}
			} catch (HoloClientException | InterruptedException e) {
				//这种错误基本没救了，撤
				action.getCollector().exceptionally(action.getShardId(), e);
				break;
			} catch (Throwable e) {
				action.getCollector().exceptionally(action.getShardId(), e);
				throw e;
			} finally {
				connContext.close();
			}
		}
	}

	private void fetch(int shardId, BinlogRecordCollector collector, ConnectionContext connContext, HoloBinlogDecoder decoder, Queue<Tuple<CompletableFuture<Void>, Long>> commitJob) throws SQLException, HoloClientException, InterruptedException {
		// Replication Connection 不能执行其他sql，因此单独创建 Replication Connection.
		while (started.get()) {
			tryFlush(connContext, commitJob);
			if (binlogRecordArray.isReadable()) {
				while (started.get() && binlogRecordArray.remain() > 0) {
					tryFlush(connContext, commitJob);
					collector.emit(shardId, binlogRecordArray);
				}
			}
			ByteBuffer byteBuffer = connContext.pgReplicationStream.read();
			binlogRecordArray.beginWrite();
			decoder.decode(shardId, byteBuffer, binlogRecordArray);
			binlogRecordArray.beginRead();
			//如果成功消费了重置重试次数
			resetRetryCount();
			if (binlogRecordArray.remain() == 0) {
				if (binlogHeartBeatIntervalMs > -1) {
					long current = System.currentTimeMillis();
					if (current - connContext.getTimestamp() > binlogHeartBeatIntervalMs) {
						connContext.updateTimestamp(current);
						BinlogHeartBeatRecord record = new BinlogHeartBeatRecord(decoder.getSchema(), connContext.startLsn, BinlogEventType.HeartBeat, current * 1000L);
						record.setShardId(shardId);
						binlogRecordArray.beginWrite();
						binlogRecordArray.add(record);
						binlogRecordArray.beginRead();
					}
				}
			} else {
				BinlogRecord lastRecord = binlogRecordArray.last();
				connContext.setEmittedLsn(lastRecord.getBinlogLsn(), lastRecord.getBinlogTimestamp() / 1000L);
			}
			while (started.get() && binlogRecordArray.remain() > 0) {
				tryFlush(connContext, commitJob);
				collector.emit(shardId, binlogRecordArray);
			}

		}
	}

	private void tryFlush(ConnectionContext connContext, Queue<Tuple<CompletableFuture<Void>, Long>> commitJob) throws SQLException {

		Tuple<CompletableFuture<Void>, Long> job = commitJob.poll();
		if (job == null) {
			return;
		}
		int flushRetryCount = maxRetryCount;
		boolean done = false;
		try {
			while (!done && --flushRetryCount > 0) {
				try {
					if (!connContext.isInit()) {
						connContext.init();
					}
					connContext.pgReplicationStream.setFlushedLSN(LogSequenceNumber.valueOf(job.r));
					connContext.pgReplicationStream.forceUpdateStatus();
					job.l.complete(null);
					done = true;
				} catch (SQLException e) {
					if (flushRetryCount > 0) {
						connContext.close();
					} else {
						throw e;
					}
				}
			}
		} catch (SQLException e) {
			job.l.completeExceptionally(e);
			throw e;
		} finally {
			if (!job.l.isDone()) {
				job.l.completeExceptionally(new HoloClientException(ExceptionCode.INTERNAL_ERROR, "unknown exception when flush binlog lsn"));
			}
		}
	}

	@Override
	public String getCostMsMetricName() {
		//Binlog action没必要记录action cost，本来就是一次性的.
		return null;
	}
}
