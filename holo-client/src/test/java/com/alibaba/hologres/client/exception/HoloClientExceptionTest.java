/*
 * Copyright (c) 2024. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.exception;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.SQLException;

/** HoloClientExceptionTest. */
public class HoloClientExceptionTest {
    @Test
    void testErrorCode() {
        {
            SQLException exception =
                    new SQLException("Get rundown is not allowed in recovering state", "XX000");
            HoloClientException holoException = HoloClientException.fromSqlException(exception);
            Assert.assertEquals(holoException.getCode(), ExceptionCode.BUSY);
        }
        {
            SQLException exception =
                    new SQLException(
                            "The shards are incomplete for query[50243444819093247], the workers or shards are unhealthy, please retry later",
                            "XX000");
            HoloClientException holoException = HoloClientException.fromSqlException(exception);
            Assert.assertEquals(holoException.getCode(), ExceptionCode.BUSY);
        }
        // 08P01 协议错乱（Hologres 缩容后 socket 被 server 关闭场景）必须归为 CONNECTION_ERROR，
        // 这样 ConnectionHolder.testConnection / needRetry 会丢弃并重建 PgConnection。
        {
            SQLException exception =
                    new SQLException("Expected command status BEGIN, got .", "08P01");
            HoloClientException holoException = HoloClientException.fromSqlException(exception);
            Assert.assertEquals(holoException.getCode(), ExceptionCode.CONNECTION_ERROR);
        }
        // 08P02 IDLE_SESSION_TIMEOUT 应继续归为 TIMEOUT，不被上面的 class 08 兜底提前拦截。
        {
            SQLException exception =
                    new SQLException("terminating connection due to idle", "08P02");
            HoloClientException holoException = HoloClientException.fromSqlException(exception);
            Assert.assertEquals(holoException.getCode(), ExceptionCode.TIMEOUT);
        }
        // PSQLState.isConnectionError 已覆盖的 08006 仍应为 CONNECTION_ERROR（回归保护）。
        {
            SQLException exception =
                    new SQLException(
                            "An I/O error occurred while sending to the backend.", "08006");
            HoloClientException holoException = HoloClientException.fromSqlException(exception);
            Assert.assertEquals(holoException.getCode(), ExceptionCode.CONNECTION_ERROR);
        }
    }
}
