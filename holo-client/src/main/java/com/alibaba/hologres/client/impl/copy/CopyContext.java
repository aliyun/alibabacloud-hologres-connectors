/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.copy;

import org.postgresql.copy.CopyOperation;
import org.postgresql.jdbc.PgConnection;

import java.sql.SQLException;

/** copy需要的相关对象. */
public class CopyContext {

    /** 为了拿到conn.getTimestampUtils()对象，用于CopyIn时的timestamp相关字段序列化. */
    private PgConnection conn;

    /** 为了可以在调用侧根据情况执行cancel copy. */
    private CopyOperation copyOperation;

    public CopyContext(PgConnection conn, CopyOperation copyOperation) {
        this.conn = conn;
        this.copyOperation = copyOperation;
    }

    public PgConnection getConn() {
        return conn;
    }

    public CopyOperation getCopyOperation() {
        return copyOperation;
    }

    public synchronized void cancel() throws SQLException {
        if (copyOperation.isActive()) {
            copyOperation.cancelCopy();
        }
    }
}
