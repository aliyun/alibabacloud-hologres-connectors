/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler;

import com.alibaba.hologres.client.Get;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.impl.ConnectionHolder;
import com.alibaba.hologres.client.impl.action.GetAction;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.RecordKey;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.IdentifierUtil;
import com.alibaba.hologres.client.utils.Metrics;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;

/**
 * GetAction处理类.
 */
public class GetActionHandler extends ActionHandler<GetAction> {

	public static final Logger LOGGER = LoggerFactory.getLogger(GetActionHandler.class);

	private static final String NAME = "get";

	private final HoloConfig config;
	private final ConnectionHolder connectionHolder;

	public GetActionHandler(ConnectionHolder connectionHolder, HoloConfig config) {
		super(config);
		this.config = config;
		this.connectionHolder = connectionHolder;
	}

	@Override
	public void handle(GetAction getAction) {
		if (getAction.getGetList().size() > 0) {
			doHandleGetAction(getAction.getGetList().get(0).getRecord().getSchema(), getAction.getGetList().get(0).getRecord().getTableName(), getAction.getGetList());
		}
	}

	private void doHandleGetAction(TableSchema schema, TableName tableName, List<Get> recordList) {
		long startTime = System.currentTimeMillis();
		BitSet columnMask = new BitSet(schema.getColumnSchema().length);
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
		sb.append(" from ").append(tableName.getFullName()).append(" where ");
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
		LOGGER.debug("Get sql:{}", sql);
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
					ps.setQueryTimeout(config.getReadTimeoutMilliseconds() > 0 ? Math.max(config.getReadTimeoutMilliseconds() / 1000, 1) : 0);
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
			}, config.getReadRetryCount());

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

	@Override
	public String getCostMsMetricName() {
		return NAME + METRIC_COST_MS;
	}
}
