/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler;

import com.alibaba.hologres.client.EqualsFilter;
import com.alibaba.hologres.client.Filter;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.RangeFilter;
import com.alibaba.hologres.client.Scan;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.ConnectionHolder;
import com.alibaba.hologres.client.impl.action.ScanAction;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.RecordKey;
import com.alibaba.hologres.client.model.RecordScanner;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.IdentifierUtil;
import com.alibaba.hologres.client.utils.Metrics;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.PrimitiveIterator;

/**
 * ScanAction处理类.
 */
public class ScanActionHandler extends ActionHandler<ScanAction> {

	public static final Logger LOGGER = LoggerFactory.getLogger(ScanActionHandler.class);

	private static final String NAME = "empty";

	private final HoloConfig config;
	private final ConnectionHolder connectionHolder;

	public ScanActionHandler(ConnectionHolder connectionHolder, HoloConfig config) {
		super(config);
		this.config = config;
		this.connectionHolder = connectionHolder;
	}

	@Override
	public void handle(ScanAction scanAction) {
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
					if (!config.isUseFixedFe()) {
						// AutoCommit set false will execute "begin" query, what fixed fe not support
						conn.setAutoCommit(false);
					}
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

	@Override
	public String getCostMsMetricName() {
		return NAME + METRIC_COST_MS;
	}
}
