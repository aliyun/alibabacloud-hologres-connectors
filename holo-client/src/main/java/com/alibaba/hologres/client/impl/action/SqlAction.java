/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.action;

import com.alibaba.hologres.client.function.FunctionWithSQLException;

import java.sql.Connection;

/** ga. */
public class SqlAction<T> extends AbstractAction<T> {

    FunctionWithSQLException<Connection, T> handler;

    public SqlAction(FunctionWithSQLException<Connection, T> handler) {
        this.handler = handler;
    }

    public FunctionWithSQLException<Connection, T> getHandler() {
        return handler;
    }
}
