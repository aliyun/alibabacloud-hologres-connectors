/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.ConnectionHolder;
import com.alibaba.hologres.client.impl.action.SqlAction;

/** SqlAction处理类. */
public class SqlActionHandler extends ActionHandler<SqlAction> {

    private static final String NAME = "sql";

    private final ConnectionHolder connectionHolder;

    public SqlActionHandler(ConnectionHolder connectionHolder, HoloConfig config) {
        super(config);
        this.connectionHolder = connectionHolder;
    }

    @Override
    public void handle(SqlAction action) {
        try {
            action.getFuture()
                    .complete(
                            connectionHolder.retryExecute(
                                    (conn) -> action.getHandler().apply(conn)));
        } catch (HoloClientException e) {
            action.getFuture().completeExceptionally(e);
        }
    }

    @Override
    public String getCostMsMetricName() {
        return NAME + METRIC_COST_MS;
    }
}
