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
    }
}
