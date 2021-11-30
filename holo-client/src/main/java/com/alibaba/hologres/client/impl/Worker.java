/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.EqualsFilter;
import com.alibaba.hologres.client.Filter;
import com.alibaba.hologres.client.Get;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.RangeFilter;
import com.alibaba.hologres.client.Scan;
import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.exception.HoloClientWithDetailsException;
import com.alibaba.hologres.client.impl.action.AbstractAction;
import com.alibaba.hologres.client.impl.action.CopyAction;
import com.alibaba.hologres.client.impl.action.EmptyAction;
import com.alibaba.hologres.client.impl.action.GetAction;
import com.alibaba.hologres.client.impl.action.MetaAction;
import com.alibaba.hologres.client.impl.action.PutAction;
import com.alibaba.hologres.client.impl.action.ScanAction;
import com.alibaba.hologres.client.impl.action.SqlAction;
import com.alibaba.hologres.client.impl.copy.CopyContext;
import com.alibaba.hologres.client.impl.copy.InternalPipedOutputStream;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.RecordKey;
import com.alibaba.hologres.client.model.RecordScanner;
import com.alibaba.hologres.client.model.WriteFailStrategy;
import com.alibaba.hologres.client.utils.Metrics;
import com.codahale.metrics.MetricRegistry;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.copy.CopyOut;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.PgStatement;
import org.postgresql.model.Column;
import org.postgresql.model.TableSchema;
import org.postgresql.util.IdentifierUtil;
import org.postgresql.util.cache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * worker.
 */
public class Worker implements Runnable {
	public static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

	final ConnectionHolder connectionHolder;
	ObjectChan<AbstractAction> recordCollector = new ObjectChan<>();
	final UpsertStatementBuilder builder;
	final AtomicBoolean started;
	final HoloConfig config;
	AtomicReference<Throwable> fatal = new AtomicReference<>(null);
	private final String name;

	public Worker(HoloConfig config, AtomicBoolean started, int index, boolean isShardEnv) {
		this.config = config;
		connectionHolder = new ConnectionHolder(config, this, isShardEnv);
		builder = new UpsertStatementBuilder(config);
		this.started = started;
		this.name = "Worker-" + index;
	}

	public boolean offer(AbstractAction action) throws HoloClientException {
		if (fatal.get() != null) {
			throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "fatal", fatal.get());
		}
		if (action != null) {
			if (!started.get()) {
				throw new HoloClientException(ExceptionCode.ALREADY_CLOSE, "worker is close");
			}
			return this.recordCollector.set(action);
		} else {
			return this.recordCollector.set(new EmptyAction());
		}
	}

	@Override
	public void run() {
		LOGGER.info("worker:{} start", this);
		while (started.get()) {
			try {
				AbstractAction action = recordCollector.get(2000L, TimeUnit.MILLISECONDS);
				/*
				 * 每个循环做2件事情：
				 * 1 有action就执行action
				 * 2 根据connectionMaxIdleMs释放空闲connection
				 * */
				if (null != action) {
					String metricsName = null;
					long start = System.nanoTime();
					try {
						if (action instanceof PutAction) {
							metricsName = Metrics.METRICS_WRITE_COST_MS_ALL;
							handlePutAction((PutAction) action);
						} else if (action instanceof MetaAction) {
							metricsName = Metrics.METRICS_META_COST_MS_ALL;
							handleMetaAction((MetaAction) action);
						} else if (action instanceof GetAction) {
							metricsName = Metrics.METRICS_GET_COST_MS_ALL;
							handleGetAction((GetAction) action);
						} else if (action instanceof ScanAction) {
							metricsName = Metrics.METRICS_SCAN_COST_MS_ALL;
							handleScanAction((ScanAction) action);
						} else if (action instanceof SqlAction) {
							metricsName = Metrics.METRICS_SQL_COST_MS_ALL;
							handleSqlAction((SqlAction) action);
						} else if (action instanceof CopyAction) {
							metricsName = Metrics.METRICS_COPY_COST_MS_ALL;
							handleCopyAction((CopyAction) action);
						} else if (action instanceof EmptyAction) {
							//空操作
						} else {
							throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "Unknown action:" + action.getClass().getName());
						}
						//LOGGER.info("worker done:{}",action.getFuture());

					} catch (Throwable e){
						if (action.getFuture() != null && !action.getFuture().isDone()) {
							action.getFuture().completeExceptionally(e);
						}
						throw e;
					} finally {
						recordCollector.clear();
						long end = System.nanoTime();
						long cost = (end - start) / 1000000L;
						if (metricsName != null) {
							Metrics.registry().meter(metricsName).mark(cost);
						}
						Metrics.registry().meter(Metrics.METRICS_ALL_COST_MS_ALL).mark(cost);
						if (action.getSemaphore() != null) {
							action.getSemaphore().release();
						}

					}
				}
				if (System.currentTimeMillis() - connectionHolder.getLastActiveTs() > config.getConnectionMaxIdleMs()) {
					connectionHolder.close();
				}
			} catch (Throwable e) {
				LOGGER.error("should not happen", e);
				fatal.set(e);
				break;
			}

		}
		LOGGER.info("worker:{} stop", this);
		connectionHolder.close();

	}

	public long doCopyOut(final CopyContext copyContext, final OutputStream to) throws SQLException, IOException {
		byte[] buf;
		final CopyOut cp = (CopyOut) copyContext.getCopyOperation();
		try {
			while ((buf = cp.readFromCopy()) != null) {
				to.write(buf);
			}
			return cp.getHandledRowCount();
		} catch (IOException ioEX) {
			// if not handled this way the close call will hang, at least in 8.2
			copyContext.cancel();
			try { // read until exhausted or operation cancelled SQLException
				while ((buf = cp.readFromCopy()) != null) {
				}
			} catch (SQLException sqlEx) {
			} // typically after several kB
			throw ioEX;
		} finally { // see to it that we do not leave the connection locked
			copyContext.cancel();
		}
	}

	public long doCopyIn(final CopyContext copyContext, final InputStream from, int bufferSize)
			throws SQLException, IOException {
		final CopyIn cp = (CopyIn) copyContext.getCopyOperation();
		byte[] buf = new byte[bufferSize];
		int len;
		try {
			while ((len = from.read(buf)) >= 0) {
				if (len > 0) {
					cp.writeToCopy(buf, 0, len);
				}
			}
			return cp.endCopy();
		} finally { // see to it that we do not leave the connection locked
			if (from instanceof PipedInputStream) {
				from.close();
			}
			copyContext.cancel();
		}
	}

	private void handleCopyAction(final CopyAction action) {
		try {
			action.getFuture().complete((Long) connectionHolder.retryExecute((conn) -> {
				PgConnection pgConn = conn.unwrap(PgConnection.class);
				CopyManager manager = new CopyManager(pgConn);
				TableSchema schema = action.getSchema();
				OutputStream os = action.getOs();
				try {
					long ret = -1;
					switch (action.getMode()) {
						case OUT:
							try {
								StringBuilder sb = new StringBuilder();
								sb.append("COPY (select ");
								boolean first = true;
								for (Column column : schema.getColumnSchema()) {
									if (!first) {
										sb.append(",");
									}
									first = false;
									sb.append(IdentifierUtil.quoteIdentifier(column.getName(), true));
								}
								sb.append(" from ").append(schema.getTableNameObj().getFullName());
								if (action.getStartShardId() > -1 && action.getEndShardId() > -1) {
									sb.append(" where hg_shard_id>=").append(action.getStartShardId()).append(" and hg_shard_id<").append(action.getEndShardId());
								}
								sb.append(") TO STDOUT DELIMITER ',' ESCAPE '\\' CSV QUOTE '\"' NULL AS 'N'");
								String sql = sb.toString();
								LOGGER.info("copy sql:{}", sql);
								os = action.getOs();
								CopyOut copyOut = manager.copyOut(sql);
								CopyContext copyContext = new CopyContext(conn, copyOut);
								action.getReadyToStart().complete(new CopyContext(conn, copyOut));
								long rowCount = doCopyOut(copyContext, os);

								if (os instanceof InternalPipedOutputStream) {
									os.close();
								}
								ret = rowCount;
							} catch (Exception e) {
								action.getReadyToStart().completeExceptionally(e);
								throw e;
							}
							break;
						case IN: {
							try {
								if (action.getStartShardId() > -1 && action.getEndShardId() > -1) {
									StringBuilder sql = new StringBuilder("set hg_experimental_target_shard_list='");
									boolean first = true;
									for (int i = action.getStartShardId(); i < action.getEndShardId(); ++i) {
										if (!first) {
											sql.append(",");
										}
										first = false;
										sql.append(i);
									}
									sql.append("'");
									try (Statement stat = pgConn.createStatement()) {
										stat.execute(sql.toString());
									} catch (SQLException e) {
										LOGGER.error("", e);
									}
								}

								StringBuilder sb = new StringBuilder();
								sb.append("COPY ").append(schema.getTableNameObj().getFullName());
								sb.append(" FROM STDIN DELIMITER ',' ESCAPE '\\' CSV QUOTE '\"' NULL AS 'N'");
								String sql = sb.toString();
								LOGGER.info("copy sql:{}", sql);
								CopyIn copyIn = manager.copyIn(sql);
								CopyContext copyContext = new CopyContext(conn, copyIn);
								action.getReadyToStart().complete(copyContext);
								ret = doCopyIn(copyContext, action.getIs(), action.getBufferSize() > -1 ? action.getBufferSize() : config.getCopyInBufferSize());
							} catch (Exception e) {
								action.getReadyToStart().completeExceptionally(e);
								throw e;
							} finally {
								if (action.getStartShardId() > -1 && action.getEndShardId() > -1) {
									try (Statement stat = pgConn.createStatement()) {
										stat.execute("reset hg_experimental_target_shard_list");
									}
								}
							}
						}
						break;
						default:
							throw new SQLException("copy but InputStream and OutputStream both null");
					}
					return ret;
				} catch (Exception e) {
					if (os instanceof InternalPipedOutputStream) {
						try {
							os.close();
						} catch (IOException ignore) {
						}
					}
					throw new SQLException(e);
				}
			}, 1));
		} catch (HoloClientException e) {
			action.getFuture().completeExceptionally(e);
		}
	}

	private void handleSqlAction(final SqlAction action) {
		try {
			action.getFuture().complete(connectionHolder.retryExecute((conn) -> action.getHandler().apply(conn)));
		} catch (HoloClientException e) {
			action.getFuture().completeExceptionally(e);
		}
	}

	private void handleMetaAction(final MetaAction action) {
		try {
			action.getFuture().complete((TableSchema) connectionHolder.retryExecute((conn) -> {
				if ((Cache.MODE_ONLY_CACHE == action.getMode() || config.isRefreshMetaBeforeGetTableSchema()) && config.getRefreshMetaTimeout() > 0) {
					ConnectionUtil.refreshMeta(conn, config.getRefreshMetaTimeout());
				}
				return conn.getTableSchema(action.getTableName(), action.getMode());
			}));
		} catch (HoloClientException e) {
			action.getFuture().completeExceptionally(e);
		}
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
		boolean ret = true;
		switch (e.getCode()) {
			case CONNECTION_ERROR:
			case AUTH_FAIL:
			case ALREADY_CLOSE:
			case READ_ONLY:
			case META_NOT_MATCH:
			case INTERNAL_ERROR:
			case PERMISSION_DENY:
			case SYNTAX_ERROR:
			case BUSY:
				ret = false;
				break;
			default:
		}
		return ret;
	}

	private void handlePutAction(PutAction action) {
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
								}
								markRecordPutFail(record, subE);
								fails.add(record, subE);
							} catch (Exception subE) {
								//如果是致命错误最后就抛这种类型的错
								exception = new HoloClientException(ExceptionCode.INTERNAL_ERROR, "", subE);
								markRecordPutFail(record, exception);
								fails.add(record, exception);
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

	private void doHandlePutAction(List<Record> list) throws HoloClientException {
		connectionHolder.retryExecute((conn) -> {
			PreparedStatement[] psArray = builder.buildStatements(conn, list);
			try {
				long startTime = System.nanoTime() / 1000000L;
				PgStatement.executeBatchInsert(psArray);
				MetricRegistry registry = Metrics.registry();
				long endTime = System.nanoTime() / 1000000L;
				String tableName = list.get(0).getSchema().getTableNameObj().getFullName();
				//registry.meter(Metrics.METRICS_WRITE_QPS + tableName).mark();
				//registry.meter(Metrics.METRICS_WRITE_RPS + tableName).mark(list.size());
				//registry.histogram(Metrics.METRICS_WRITE_LATENCY + tableName).update(endTime - startTime);
				registry.meter(Metrics.METRICS_WRITE_QPS).mark();
				registry.histogram(Metrics.METRICS_WRITE_LATENCY).update(endTime - startTime);
				registry.meter(Metrics.METRICS_WRITE_RPS).mark(list.size());
			} finally {
				for (PreparedStatement ps : psArray) {
					ps.close();
				}
			}
			return null;
		});
	}

	private void handleScanAction(ScanAction scanAction) throws HoloClientException {
		try {
			long startTime = System.currentTimeMillis();
			Scan scan = scanAction.getScan();
			TableSchema schema = scan.getSchema();
			BitSet columnMask = new BitSet(schema.getColumnSchema().length);
			String tableName = schema.getTableNameObj().getFullName();
			if (scan.getSelectedColumns() != null) {
				columnMask.or(scan.getSelectedColumns());
			} else {
				columnMask.set(0, schema.getColumnSchema().length);
			}
			boolean first = true;
			StringBuilder sb = new StringBuilder();
			sb.append("select ");
			for (PrimitiveIterator.OfInt it = columnMask.stream().iterator(); it.hasNext(); ) {
				if (!first) {
					sb.append(",");
				}
				first = false;
				sb.append(IdentifierUtil.quoteIdentifier(schema.getColumn(it.next()).getName(), true));
			}
			sb.append(" from ").append(schema.getTableNameObj().getFullName());
			if (scan.getFilterList() != null) {
				sb.append(" where ");
				first = true;
				for (Filter filter : scan.getFilterList()) {
					if (!first) {
						sb.append(" and ");
					}

					if (filter instanceof EqualsFilter) {
						sb.append(IdentifierUtil.quoteIdentifier(schema.getColumn(((EqualsFilter) filter).getIndex()).getName(), true)).append("=?");
					} else if (filter instanceof RangeFilter) {
						RangeFilter rf = (RangeFilter) filter;
						if (rf.getStart() != null) {
							sb.append(IdentifierUtil.quoteIdentifier(schema.getColumn(rf.getIndex()).getName(), true)).append(">=?");
							first = false;
						}
						if (rf.getStop() != null) {
							if (!first) {
								sb.append(" and ");
							}
							sb.append(IdentifierUtil.quoteIdentifier(schema.getColumn((rf).getIndex()).getName(), true)).append("<?");
							first = false;
						}
					}
					first = false;

				}
			}
			String[] sortKeyNames = null;
			switch (scan.getSortKeys()) {
				case PRIMARY_KEY:
					if (schema.getPrimaryKeys() != null && schema.getPrimaryKeys().length > 0) {
						sortKeyNames = schema.getPrimaryKeys();
					}
					break;
				case CLUSTERING_KEY:
					if (schema.getClusteringKey() != null && schema.getClusteringKey().length > 0) {
						sortKeyNames = schema.getClusteringKey();
					}
					break;
				case NONE:
				default:

			}
			if (sortKeyNames != null && sortKeyNames.length > 0) {
				sb.append(" order by ");
				first = true;
				for (String name : schema.getPrimaryKeys()) {
					if (!first) {
						sb.append(",");
					}
					first = false;
					sb.append(IdentifierUtil.quoteIdentifier(name, true));
				}
			}
			String sql = sb.toString();
			LOGGER.debug("Scan sql:{}", sql);
			connectionHolder.retryExecute((conn) -> {
				Map<RecordKey, Record> resultMap = new HashMap<>();
				try {
					conn.setAutoCommit(false);
					try (PreparedStatement ps = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
						int paramIndex = 0;
						if (scan.getFilterList() != null) {
							for (Filter filter : scan.getFilterList()) {
								if (filter instanceof EqualsFilter) {
									ps.setObject(++paramIndex, ((EqualsFilter) filter).getObj(), schema.getColumn(((EqualsFilter) filter).getIndex()).getType());
								} else if (filter instanceof RangeFilter) {
									RangeFilter rf = (RangeFilter) filter;
									if (rf.getStart() != null) {
										ps.setObject(++paramIndex, rf.getStart(), schema.getColumn(rf.getIndex()).getType());
									}
									if (rf.getStop() != null) {
										ps.setObject(++paramIndex, rf.getStop(), schema.getColumn(rf.getIndex()).getType());
									}
								}
							}
						}
						ps.setFetchSize(scan.getFetchSize() > 0 ? scan.getFetchSize() : config.getScanFetchSize());
						ps.setQueryTimeout(scan.getTimeout() > 0 ? scan.getTimeout() : config.getScanTimeoutSeconds());
						ResultSet rs = null;
						try {
							rs = ps.executeQuery();
						} catch (SQLException e) {
							if (rs != null) {
								try {
									rs.close();
								} catch (SQLException ignore) {
								} finally {
									rs = null;
								}
							}
							scanAction.getFuture().completeExceptionally(HoloClientException.fromSqlException(e));
						}
						if (rs != null) {
							byte[] lock = new byte[]{};
							RecordScanner recordScanner = new RecordScanner(rs, lock, schema, scan.getSelectedColumns());
							scanAction.getFuture().complete(recordScanner);
							synchronized (lock) {
								while (!recordScanner.isDone()) {
									try {
										lock.wait(5000L);
									} catch (InterruptedException e) {
										throw new RuntimeException(e);
									}
								}
							}
							long endTime = System.currentTimeMillis();
							MetricRegistry registry = Metrics.registry();
							registry.histogram(Metrics.METRICS_SCAN_LATENCY).update(endTime - startTime);
							registry.meter(Metrics.METRICS_SCAN_QPS).mark();
						}
					}
				} catch (SQLException e) {
					if (!scanAction.getFuture().isDone()) {
						scanAction.getFuture().completeExceptionally(e);
					}
					throw e;
				} finally {
					conn.setAutoCommit(true);
				}
				return null;
			}, 1);
		} catch (Exception e) {
			if (!scanAction.getFuture().isDone()) {
				scanAction.getFuture().completeExceptionally(e);
			}
		}
	}

	private void handleGetAction(GetAction getAction) throws HoloClientException {
		if (getAction.getGetList().size() > 0) {
			doHandleGetAction(getAction.getGetList().get(0).getRecord().getSchema(), getAction.getGetList());
		}
	}

	private void doHandleGetAction(TableSchema schema, List<Get> recordList) throws HoloClientException {
		long startTime = System.currentTimeMillis();
		BitSet columnMask = new BitSet(schema.getColumnSchema().length);
		String tableName = schema.getTableNameObj().getFullName();
		for (Get get : recordList) {
			columnMask.or(get.getRecord().getBitSet());
		}
		boolean first = true;
		StringBuilder sb = new StringBuilder();
		sb.append("select ");
		for (PrimitiveIterator.OfInt it = columnMask.stream().iterator(); it.hasNext(); ) {
			if (!first) {
				sb.append(",");
			}
			first = false;
			sb.append(IdentifierUtil.quoteIdentifier(schema.getColumn(it.next()).getName(), true));
		}
		sb.append(" from ").append(schema.getTableNameObj().getFullName()).append(" where ");
		for (int i = 0; i < recordList.size(); ++i) {
			if (i > 0) {
				sb.append(" or ");
			}
			first = true;

			sb.append("( ");
			for (String key : schema.getPrimaryKeys()) {
				if (!first) {
					sb.append(" and ");
				}
				first = false;
				sb.append(IdentifierUtil.quoteIdentifier(key, true)).append("=?");

			}
			sb.append(" ) ");

		}
		String sql = sb.toString();
		try {
			Map<RecordKey, Record> resultRecordMap = (Map<RecordKey, Record>) connectionHolder.retryExecute((conn) -> {
				Map<RecordKey, Record> resultMap = new HashMap<>();
				try (PreparedStatement ps = conn.prepareStatement(sql)) {
					int paramIndex = 0;
					for (Get get : recordList) {
						Record record = get.getRecord();
						for (int keyIndex : record.getKeyIndex()) {
							ps.setObject(++paramIndex, record.getObject(keyIndex), record.getSchema().getColumn(keyIndex).getType());

						}
					}
					try (ResultSet rs = ps.executeQuery()) {
						while (rs.next()) {
							Record record = Record.build(schema);
							int index = 0;
							for (PrimitiveIterator.OfInt it = columnMask.stream().iterator(); it.hasNext(); ) {
								int recordColumnIndex = it.next();
								fillRecord(record, recordColumnIndex, rs, ++index, schema.getColumn(recordColumnIndex));
							}
							resultMap.put(new RecordKey(record), record);
						}
					}
					long endTime = System.currentTimeMillis();
					MetricRegistry registry = Metrics.registry();
					registry.meter(Metrics.METRICS_DIMLOOKUP_QPS + tableName).mark();
					registry.meter(Metrics.METRICS_DIMLOOKUP_RPS + tableName).mark(recordList.size());
					registry.histogram(Metrics.METRICS_DIMLOOKUP_LATENCY + tableName).update(endTime - startTime);
					registry.meter(Metrics.METRICS_DIMLOOKUP_RPS_ALL).mark(recordList.size());
				}
				return resultMap;
			}, 1);

			for (Get get : recordList) {
				Record record = get.getRecord();
				if (get.getFuture() != null) {
					RecordKey key = new RecordKey(convertRecordColumnType(record));
					Record result = resultRecordMap.get(key);
					get.getFuture().complete(result);
				}
			}
		} catch (Exception e) {
			for (Get get : recordList) {
				if (get.getFuture() != null && !get.getFuture().isDone()) {
					get.getFuture().completeExceptionally(e);
				}
			}
		}
	}

	public static void fillRecord(Record record, int recordIndex, ResultSet rs, int resultSetIndex, Column column) throws SQLException {
		switch (column.getType()) {
			case Types.SMALLINT:
				record.setObject(recordIndex, rs.getShort(resultSetIndex));
				break;
			default:
				record.setObject(recordIndex, rs.getObject(resultSetIndex));
		}
	}

	public static Record convertRecordColumnType(Record record) {
		Record ret = record;
		boolean needConvert = false;
		for (int keyIndex : record.getSchema().getKeyIndex()) {
			Object obj = record.getObject(keyIndex);
			int type = record.getSchema().getColumnSchema()[keyIndex].getType();
			switch (type) {
				case Types.INTEGER:
					if (!(obj instanceof Integer)) {
						needConvert = true;
					}
					break;
				case Types.BIGINT:
					if (!(obj instanceof Long)) {
						needConvert = true;
					}
					break;
				default:
					needConvert = false;
			}
			if (needConvert) {
				break;
			}
		}
		if (needConvert) {
			ret = Record.build(record.getSchema());
			for (int keyIndex : record.getSchema().getKeyIndex()) {
				Object obj = record.getObject(keyIndex);
				int type = record.getSchema().getColumnSchema()[keyIndex].getType();
				switch (type) {
					case Types.SMALLINT:
						if (obj instanceof Number) {
							obj = ((Number) obj).shortValue();
						} else if (obj instanceof String) {
							obj = Short.parseShort((String) obj);
						}
						break;
					case Types.INTEGER:
						if (obj instanceof Number) {
							obj = ((Number) obj).intValue();
						} else if (obj instanceof String) {
							obj = Integer.parseInt((String) obj);
						}
						break;
					case Types.BIGINT:
						if (obj instanceof Number) {
							obj = ((Number) obj).longValue();
						} else if (obj instanceof String) {
							obj = Long.parseLong((String) obj);
						}
						break;
					default:
				}
				ret.setObject(keyIndex, obj);
			}
		}

		return ret;
	}

	@Override
	public String toString() {
		return name;
	}
}
