/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.HoloClientWithDetailsException;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.hologres.client.utils.Tuple;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 分区表测试用例.
 */
public class HoloClientPartitionTest extends HoloClientTestBase {
	@DataProvider(name = "partitionColumnCase")
	public Object[] createData() {
		Tuple<String, Object>[] ret = new Tuple[4];
		ret[0] = new Tuple<>("date", Date.valueOf("2023-07-14"));
		ret[1] = new Tuple<>("int", 1234);
		ret[2] = new Tuple<>("text", "2023-07-14");
		ret[3] = new Tuple<>("varchar(20)", "20230714_1");
		return ret;
	}

	@Test
	public void testPartition001() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"partition_001\"";
			String childTableName = "test_schema.\"partition_001_a\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text,ds text not null,primary key(id,ds)) partition by list(ds)";
			String createChildSql = "create table " + childTableName + " partition of " + tableName + " for values in ('a')";
			String addColumn = "alter table " + tableName + " add column age int";
			execute(conn, new String[]{createSchema, dropSql, createSql, createChildSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				Assert.assertEquals(3, schema.getColumnSchema().length);
				Put put = new Put(schema);
				put.setObject(0, 0);
				put.setObject(1, "name");
				put.setObject(2, "a");

				execute(conn, new String[]{addColumn});

				client.put(put);

				client.flush();

				int count = 0;
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select * from " + tableName)) {
						while (rs.next()) {
							++count;
							Assert.assertEquals(0, rs.getInt(1));
							Assert.assertEquals("name", rs.getString(2));
							Assert.assertEquals("a", rs.getString(3));
							Assert.assertNull(rs.getObject(4));
						}
					}
				}
				Assert.assertEquals(1, count);
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * delete/insert not exists partition table
	 * Method: put(Put put).
	 */
	@Test
	public void testPartition002() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(false); // child table is not exists
		config.setConnectionMaxIdleMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "\"holO_client_partition_002\"";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName
				+ "(iD int not null,id2 int not null, name text, primary key(id,id2)) partition by list(iD)";

			execute(conn, new String[] {dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				// delete not throw exception
				Put put1 = new Put(schema);
				put1.setObject("id", 10);
				put1.setObject("id2", 1);
				put1.getRecord().setType(Put.MutationType.DELETE);
				client.put(put1);
				client.flush();

				// insert throw exception
				Assert.assertThrows(HoloClientWithDetailsException.class, () -> {
					try {
						Put put2 = new Put(schema);
						put2.setObject("id", 10);
						put2.setObject("id2", 1);
						client.put(put2);
						client.flush();
					} catch (HoloClientWithDetailsException e) {
						Assert.assertTrue(e.getMessage().contains("child table is not found"));
						throw e;
					}
				});
			} finally {
				execute(conn, new String[] {dropSql});
			}
		}
	}

	/**
	 * Partition column type test
	 * Method: put(Put put).
	 */
	@Test(dataProvider = "partitionColumnCase")
	public void testPartition003(Tuple<String, Object> partitionColumnCase) throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "\"holO_client_partition_003_" + partitionColumnCase.l + "\"";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName
					+ "(iD " + partitionColumnCase.l + " not null,id2 int not null, name text, primary key(id,id2)) partition by list(iD)";

			execute(conn, new String[] {dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				Put put2 = new Put(schema);
				put2.setObject("id", partitionColumnCase.r);
				put2.setObject("id2", 1);
				client.put(put2);
				client.flush();

				int count = 0;
				try (Statement stat = conn.createStatement()) {
					// 分区子表没同步的话，select结果可能不对
					ConnectionUtil.refreshMeta(conn, 1000);
					try (ResultSet rs = stat.executeQuery("select * from " + tableName)) {
						while (rs.next()) {
							++count;
							Assert.assertEquals(partitionColumnCase.r, rs.getObject(1));
							Assert.assertEquals(1, rs.getInt(2));
							Assert.assertNull(rs.getObject(3));
						}
					}
				}
				Assert.assertEquals(1, count);
			} finally {
				execute(conn, new String[] {dropSql});
			}
		}
	}

	/**
	 * DynamicPartition with drop column table
	 *
	 * Method: put(Put put).
	 */
	@Test
	public void testPartition004() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "\"test_SCHema\".\"holO_client_partition_004\"";
			String dropSql = "drop table if exists " + tableName;
			String createSchema = "create schema if not exists \"test_SCHema\"";
			String createSql = "create table " + tableName
					+ "(a text not null,b text not null, ds text, primary key(a,ds)) partition by list(ds)";

			execute(conn, new String[] {createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				Put put1 = new Put(schema);
				put1.setObject("a", "test");
				put1.setObject("b", "test");
				put1.setObject("ds", "20230920");
				client.put(put1);
				client.flush();

				String dropColumn = "set hg_experimental_enable_drop_column = on;"
						+ "ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN b;";
				execute(conn, new String[] {dropColumn});

				schema = client.getTableSchema(tableName, true);
				Put put2 = new Put(schema);
				put2.setObject("a", "test");
				put2.setObject("ds", "20230921");
				client.put(put2);
				client.flush();

				int count = 0;
				try (Statement stat = conn.createStatement()) {
					// 分区子表没同步的话，select结果可能不对
					ConnectionUtil.refreshMeta(conn, 1000);
					try (ResultSet rs = stat.executeQuery("select * from " + tableName + " order by ds")) {
						while (rs.next()) {
							++count;
							Assert.assertEquals("test", rs.getObject(1));
							if (count == 1) {
								Assert.assertEquals("20230920", rs.getString(2));
							} else {
								Assert.assertEquals("20230921", rs.getString(2));
							}
						}
					}
				}
				Assert.assertEquals(2, count);
			} finally {
				execute(conn, new String[] {dropSql});
			}
		}
	}

}
