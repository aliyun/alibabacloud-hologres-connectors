/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.ConnectionHolder;
import com.alibaba.hologres.client.impl.action.MetaAction;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.TableSchema;

import java.sql.SQLException;

/**
 * MetaAction处理类.
 */
public class MetaActionHandler extends ActionHandler<MetaAction> {

	private static final String NAME = "meta";
	private final HoloConfig config;
	private final ConnectionHolder connectionHolder;

	public MetaActionHandler(ConnectionHolder connectionHolder, HoloConfig config) {
		super(config);
		this.config = config;
		this.connectionHolder = connectionHolder;
	}

	@Override
	public void handle(MetaAction action) {
		try {
			action.getFuture().complete((TableSchema) connectionHolder.retryExecuteWithVersion((connWithVersion) -> {
				if (config.isRefreshMetaBeforeGetTableSchema() && config.getRefreshMetaTimeout() > 0) {
					String fullTableName = action.getTableName().getFullName();
					ConnectionUtil.CheckMetaResult result = ConnectionUtil.checkMeta(connWithVersion.getConn(), connWithVersion.getVersion(), fullTableName, config.getRefreshMetaTimeout());
					if (!result.isUpdated()) {
						if (result.getMsg().contains("table is lock by other query which request a AccessExclusiveLock")) {
							throw new SQLException("mismatches the version of the table [" + fullTableName + "]:" + result.getMsg()); //HoloClientException会识别这种特殊的异常message
						} else {
							throw new SQLException("truncate table [" + fullTableName + "], but replay not finished yet:" + result.getMsg()); //HoloClientException会识别这种特殊的异常message
						}
					}
				}
				return ConnectionUtil.getTableSchema(connWithVersion.getConn(), action.getTableName());
			}));
		} catch (HoloClientException e) {
			action.getFuture().completeExceptionally(e);
		}
	}

	@Override
	public String getCostMsMetricName() {
		return NAME + METRIC_COST_MS;
	}
}
