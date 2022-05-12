/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.exception.HoloClientWithDetailsException;
import com.alibaba.hologres.client.impl.ConnectionHolder;
import com.alibaba.hologres.client.impl.PreparedStatementWithBatchInfo;
import com.alibaba.hologres.client.impl.UnnestUpsertStatementBuilder;
import com.alibaba.hologres.client.impl.UpsertStatementBuilder;
import com.alibaba.hologres.client.impl.action.PutAction;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.WriteFailStrategy;
import com.alibaba.hologres.client.utils.Metrics;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * PutAction处理类.
 */
public class PutActionHandler extends ActionHandler<PutAction> {

	public static final Logger LOGGER = LoggerFactory.getLogger(PutActionHandler.class);

	private static final String NAME = "put";

	private final HoloConfig config;
	private final UpsertStatementBuilder builder;
	protected final ConnectionHolder connectionHolder;

	public PutActionHandler(ConnectionHolder connectionHolder, HoloConfig config) {
		super(config);
		this.config = config;
		this.builder = config.isUseLegacyPutHandler() ? new UpsertStatementBuilder(config) : new UnnestUpsertStatementBuilder(config);
		this.connectionHolder = connectionHolder;
	}

	private void markRecordPutSuccess(Record record) {
		if (record.getPutFutures() != null) {
			for (CompletableFuture<Void> future : record.getPutFutures()) {
				try {
					future.complete(null);
				} catch (Exception e) {
					LOGGER.error("markRecordPutSuccess", e);
				}
			}
		}
	}

	private void markRecordPutFail(Record record, HoloClientException e) {
		if (record.getPutFutures() != null) {
			for (CompletableFuture<Void> future : record.getPutFutures()) {
				try {
					future.completeExceptionally(e);
				} catch (Exception e1) {
					LOGGER.error("markRecordPutFail", e1);
				}
			}
		}
	}

	private boolean isDirtyDataException(HoloClientException e) {
		boolean ret = false;
		switch (e.getCode()) {
			case TABLE_NOT_FOUND:
			case CONSTRAINT_VIOLATION:
			case DATA_TYPE_ERROR:
			case DATA_VALUE_ERROR:
				ret = true;
				break;
			default:
		}
		return ret;
	}

	@Override
	public void handle(PutAction action) {
		final List<Record> recordList = action.getRecordList();
		HoloClientException exception = null;
		try {
			doHandlePutAction(recordList);
			for (Record record : recordList) {
				markRecordPutSuccess(record);
			}
		} catch (HoloClientException e) {
			WriteFailStrategy strategy = config.getWriteFailStrategy();
			if (!isDirtyDataException(e)) {
				exception = e;
				//如果不是脏数据类异常的话，就不要one by one了
				strategy = WriteFailStrategy.NONE;
			}
			boolean useDefaultStrategy = true;
			switch (strategy) {
				case TRY_ONE_BY_ONE:
					LOGGER.warn("write data fail, current WriteFailStrategy is TRY_ONE_BY_ONE", e);
					if (e.getCode() != ExceptionCode.TABLE_NOT_FOUND) {
						List<Record> single = new ArrayList<>(1);
						HoloClientWithDetailsException fails = new HoloClientWithDetailsException(e);
						for (Record record : recordList) {
							try {
								single.add(record);
								doHandlePutAction(single);
								markRecordPutSuccess(record);
							} catch (HoloClientException subE) {
								if (!isDirtyDataException(subE)) {
									exception = subE;
								} else {
									fails.add(record, subE);
								}
								markRecordPutFail(record, subE);
							} catch (Exception subE) {
								//如果是致命错误最后就抛这种类型的错
								exception = new HoloClientException(ExceptionCode.INTERNAL_ERROR, "", subE);
								markRecordPutFail(record, exception);
							} finally {
								single.clear();
							}
						}
						if (exception == null && fails.size() > 0) {
							exception = fails;
						}
						useDefaultStrategy = false;
					}
					break;
				default:
			}
			if (useDefaultStrategy) {
				for (Record record : recordList) {
					markRecordPutFail(record, e);
				}

				if (exception == null) {
					HoloClientWithDetailsException localPutException = new HoloClientWithDetailsException(e);
					localPutException.add(recordList, e);
					exception = localPutException;
				}
			}
		} catch (Exception e) {
			exception = new HoloClientException(ExceptionCode.INTERNAL_ERROR, "", e);
			for (Record record : recordList) {
				markRecordPutFail(record, exception);
			}
		}
		if (exception != null) {
			action.getFuture().completeExceptionally(exception);
		} else {
			action.getFuture().complete(null);
		}
	}

	protected void doHandlePutAction(List<Record> list) throws HoloClientException {
		connectionHolder.retryExecuteWithVersion((connWithVersion) -> {
			Connection conn = connWithVersion.getConn();
			List<PreparedStatementWithBatchInfo> psArray = builder.buildStatements(conn, connWithVersion.getVersion(), list.get(0).getSchema(), list);
			try {

				long startTime = System.nanoTime() / 1000000L;
				long bytes = 0L;
				long batchCount = 0;
				for (PreparedStatementWithBatchInfo ps : psArray) {
					if (ps != null) {
						if (ps.r) {
							ps.l.executeBatch();
						} else {
							ps.l.execute();
						}
					}
					if (ps.getType() == Put.MutationType.INSERT) {
						bytes += ps.getByteSize();
						batchCount += ps.getBatchCount();
					}
				}
				//PgStatement.executeBatchInsert(psArray);
				MetricRegistry registry = Metrics.registry();
				long endTime = System.nanoTime() / 1000000L;
				String tableName = list.get(0).getSchema().getTableNameObj().getFullName();
				//registry.meter(Metrics.METRICS_WRITE_QPS + tableName).mark();
				//registry.meter(Metrics.METRICS_WRITE_RPS + tableName).mark(list.size());
				//registry.histogram(Metrics.METRICS_WRITE_LATENCY + tableName).update(endTime - startTime);
				registry.meter(Metrics.METRICS_WRITE_QPS).mark();
				registry.meter(Metrics.METRICS_WRITE_BPS).mark(bytes);
				if (batchCount > 0) {
					registry.histogram(Metrics.METRICS_WRITE_SQL_PER_BATCH).update(batchCount);
				}
				registry.histogram(Metrics.METRICS_WRITE_LATENCY).update(endTime - startTime);
				registry.meter(Metrics.METRICS_WRITE_RPS).mark(list.size());
			} finally {
				for (PreparedStatementWithBatchInfo ps : psArray) {
					if (ps != null && ps.l != null) {
						ps.l.close();
					}
				}
			}
			return null;
		});
	}

	@Override
	public String getCostMsMetricName() {
		return NAME + METRIC_COST_MS;
	}
}
