/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * connection工具类.
 */
public class ConnectionUtil {

	public static void refreshMeta(Connection conn, int timeout) throws SQLException {
		try (Statement stat = conn.createStatement()) {
			stat.execute("select hologres.hg_internal_refresh_meta(" + timeout + ")");
		}
	}
}
