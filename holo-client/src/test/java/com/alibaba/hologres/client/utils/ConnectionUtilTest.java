/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.utils;

import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * ConnectionUtil单元测试用例.
 */
public class ConnectionUtilTest {

	@Test
	public void testReplaceJdbcUrlEndpoint() {
		String originalJdbcUrl = "jdbc:postgresql://{ENDPOINT}:{PORT}/{DBNAME}?ApplicationName={APPLICATION_NAME}&reWriteBatchedInserts=true";
		String newEndpoint = "127.0.0.1:80";
		String expect = "jdbc:postgresql://127.0.0.1:80/{DBNAME}?ApplicationName={APPLICATION_NAME}&reWriteBatchedInserts=true";
		String newJdbcUrl = ConnectionUtil.replaceJdbcUrlEndpoint(originalJdbcUrl, newEndpoint);
		Assert.assertEquals(newJdbcUrl, expect);
	}
}
