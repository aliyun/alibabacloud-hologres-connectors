/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.impl.ExecutionPool;
import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.hologres.client.utils.Metrics;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.model.TableSchema;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * UT for ExecutionPool.
 */
public class ExecutionPoolTest extends HoloClientTestBase {
	/**
	 * ExecutionPool
	 * Method: put(Put put).
	 */
	@Test
	public void testExecutionPool001() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		config.setAppName("executionPool001");
		try (Connection conn = buildConnection(); ExecutionPool pool = ExecutionPool.buildOrGet("hello", config, false)) {
			String tableName = "test_schema.holo_execution_pool_001";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,c int not null,d text,e text, primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				HoloClient client0 = new HoloClient(config);
				client0.setPool(pool);
				HoloClient client1 = new HoloClient(config);
				TableSchema schema = client0.getTableSchema(tableName, true);
				client1.setPool(pool);
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 0L);
					put2.setObject("c", 12);
					put2.setObject("d", "aaa");
					put2.setObject("e", "123");
					client0.put(put2);
				}
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 1L);
					put2.setObject("c", 12);
					put2.setObject("d", "aaa");
					put2.setObject("e", "123");
					client1.put(put2);
				}
				client0.close();
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 2L);
					put2.setObject("c", 12);
					put2.setObject("d", "aaa");
					put2.setObject("e", "123");
					client1.put(put2);
				}
				client1.close();
				client0 = new HoloClient(config);
				client0.setPool(pool);
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 3L);
					put2.setObject("c", 12);
					put2.setObject("d", "aaa");
					put2.setObject("e", "123");
					client0.put(put2);
				}
				client0.flush();

				int count = 0;
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
						if (rs.next()) {
							Assert.assertEquals(4, rs.getInt(1));
							count = 1;
						}
					}
				}
				Assert.assertEquals(1, count);
				Metrics.reporter().report();
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
		synchronized (this) {
			this.wait(5000L);
		}
	}
}
