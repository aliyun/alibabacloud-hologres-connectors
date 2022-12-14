/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloClientTestBase;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.hologres.client.utils.Metrics;
import org.testng.Assert;
import org.testng.annotations.Test;

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
		try (Connection conn = buildConnection(); ExecutionPool pool = ExecutionPool.buildOrGet("executionPool001", config, false)) {
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

	/**
	 * ExecutionPool
	 * Method: put(Put put).
	 */
	@Test
	public void testExecutionPool002() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config0 = buildConfig();
		config0.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config0.setWriteBatchSize(1);
		config0.setAppName("executionPool000");
		HoloConfig config1 = buildConfig();
		config1.setWriteMode(WriteMode.INSERT_OR_IGNORE);
		config1.setWriteBatchSize(1);
		config1.setAppName("executionPool001");
		try (Connection conn = buildConnection(); ExecutionPool pool = ExecutionPool.buildOrGet("testExecutionPool002", config0,
				false)) {
			String tableName0 = "holo_execution_pool_000";
			String tableName1 = "holo_execution_pool_001";
			String dropSql0 = "drop table if exists " + tableName0;
			String dropSql1 = "drop table if exists " + tableName1;
			String createSql0 = "create table " + tableName0
					+ "(id int not null,c int not null,d text,e text, primary key(id)); call set_table_property('"
					+ tableName0 + "','binlog.level','replica')";
			String createSql1 = "create table " + tableName1
					+ "(id int not null,c int not null,d text,e text, primary key(id)); call set_table_property('"
					+ tableName1 + "','binlog.level','replica')";

			execute(conn, new String[]{dropSql0, dropSql1, createSql0, createSql1});

			try {
				HoloClient client0 = new HoloClient(config0);
				client0.setPool(pool);
				TableSchema schema0 = client0.getTableSchema(tableName0, true);

				HoloClient client1 = new HoloClient(config1);
				client1.setPool(pool);
				TableSchema schema1 = client0.getTableSchema(tableName1, true);
				for (int i = 0; i < 10; i++) {
					Put put = new Put(schema0);
					put.setObject("id", 0L);
					put.setObject("c", i);
					put.setObject("d", "aaa");
					put.setObject("e", "123");
					client0.put(put);
				}
				client0.flush();

				for (int i = 0; i < 10; i++) {
					Put put = new Put(schema1);
					put.setObject("id", 0L);
					put.setObject("c", i);
					put.setObject("d", "aaa");
					put.setObject("e", "123");
					client1.put(put);
				}
				client1.flush();

				Thread.sleep(1000);
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(hg_binlog_lsn) from " + tableName0)) {
						if (rs.next()) {
							// 1 insert + 9 update: 1 + 2 * 9 = 19
							Assert.assertEquals(19, rs.getInt(1));
						}
					}
					try (ResultSet rs = stat.executeQuery("select count(hg_binlog_lsn) from " + tableName1)) {
						if (rs.next()) {
							// 1 insert
							Assert.assertEquals(1, rs.getInt(1));
						}
					}
				}
				Metrics.reporter().report();
			} finally {
				execute(conn, new String[]{dropSql0, dropSql1});
			}
		}
		synchronized (this) {
			this.wait(5000L);
		}
	}
}
