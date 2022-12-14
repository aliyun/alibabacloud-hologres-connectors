/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.impl.collector.shard.DistributionKeyShardPolicy;
import com.alibaba.hologres.client.impl.collector.shard.ShardPolicy;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;

/**
 * shard单元测试.
 */
public class HoloClientShardTest extends HoloClientTestBase {

	/**
	 * shard 单pk.
	 * Method: put(Put put).
	 */
	@Test
	public void testShard001() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_shard_001";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text,primary key(id))";

			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);

				int shardCount = getShardCount(client, schema);

				ShardPolicy policy = new DistributionKeyShardPolicy();

				policy.init(shardCount);

				final int count = 1000;
				int[] actualValues = new int[count];
				int[] shardRecordCount = new int[shardCount];
				for (int i = 0; i < count; ++i) {
					Put put = new Put(schema);
					put.setObject(0, i);
					put.setObject(1, "name0");
					client.put(put);
					actualValues[i] = policy.locate(put.getRecord());
					++shardRecordCount[actualValues[i]];
				}
				client.flush();

				int[] expectValues = client.sql(c -> {
					int[] ret = new int[count];
					int actualCount = 0;
					try (Statement stat = conn.createStatement()) {
						try (ResultSet rs = stat.executeQuery("select hg_shard_id,id from " + tableName)) {
							while (rs.next()) {
								ret[rs.getInt(2)] = rs.getInt(1);
								++actualCount;
							}
						}
					}
					if (count == actualCount) {
						return ret;
					} else {
						return null;
					}
				}).get();
				Assert.assertEquals(expectValues, actualValues);
				System.out.println(Arrays.toString(shardRecordCount));
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * shard 双pk,pk=dk.
	 * Method: put(Put put).
	 */
	@Test
	public void testShard002() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_shard_002";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text not null,address text, primary key(id,name))";

			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);

				int shardCount = getShardCount(client, schema);

				ShardPolicy policy = new DistributionKeyShardPolicy();

				policy.init(shardCount);

				final int count = 1000;
				int[] actualValues = new int[count];
				int[] shardRecordCount = new int[shardCount];
				for (int i = 0; i < count; ++i) {
					Put put = new Put(schema);
					put.setObject(0, i);
					put.setObject(1, "name0");
					put.setObject(2, "address0");
					client.put(put);
					actualValues[i] = policy.locate(put.getRecord());
					++shardRecordCount[actualValues[i]];
				}
				client.flush();

				int[] expectValues = client.sql(c -> {
					int[] ret = new int[count];
					int actualCount = 0;
					try (Statement stat = conn.createStatement()) {
						try (ResultSet rs = stat.executeQuery("select hg_shard_id,id from " + tableName)) {
							while (rs.next()) {
								ret[rs.getInt(2)] = rs.getInt(1);
								++actualCount;
							}
						}
					}
					if (count == actualCount) {
						return ret;
					} else {
						return null;
					}
				}).get();
				Assert.assertEquals(expectValues, actualValues);
				System.out.println(Arrays.toString(shardRecordCount));
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * shard 双pk,pk!=dk.
	 * Method: put(Put put).
	 */
	@Test
	public void testShard003() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_shard_003";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "begin;create table " + tableName + "(id int not null,name text not null,address text, primary key(id,name));call set_table_property('" + tableName + "','distribution_key','id');end;";

			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);

				int shardCount = getShardCount(client, schema);
				Assert.assertTrue(shardCount > 1);
				ShardPolicy policy = new DistributionKeyShardPolicy();

				policy.init(shardCount);

				final int count = 1000;
				int[] actualValues = new int[count];
				int[] shardRecordCount = new int[shardCount];
				for (int i = 0; i < count; ++i) {
					Put put = new Put(schema);
					put.setObject(0, i);
					put.setObject(1, "name0");
					put.setObject(2, "address0");
					client.put(put);
					actualValues[i] = policy.locate(put.getRecord());
					++shardRecordCount[actualValues[i]];
				}
				client.flush();

				int[] expectValues = client.sql(c -> {
					int[] ret = new int[count];
					int actualCount = 0;
					try (Statement stat = conn.createStatement()) {
						try (ResultSet rs = stat.executeQuery("select hg_shard_id,id from " + tableName)) {
							while (rs.next()) {
								ret[rs.getInt(2)] = rs.getInt(1);
								++actualCount;
							}
						}
					}
					if (count == actualCount) {
						return ret;
					} else {
						return null;
					}
				}).get();
				Assert.assertEquals(expectValues, actualValues);
				System.out.println(Arrays.toString(shardRecordCount));
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * shard no_pk
	 * Method: put(Put put).
	 */
	@Test
	public void testShard004() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_shard_004";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text not null,address text)";

			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);

				int shardCount = getShardCount(client, schema);

				Assert.assertTrue(shardCount > 1);
				ShardPolicy policy = new DistributionKeyShardPolicy();

				policy.init(shardCount);

				final int count = 10000;
				int[] actualValues = new int[count];
				int[] shardRecordCount = new int[shardCount];
				for (int i = 0; i < count; ++i) {
					Put put = new Put(schema);
					put.setObject(0, i);
					put.setObject(1, "name0");
					put.setObject(2, "address0");
					client.put(put);
					actualValues[i] = policy.locate(put.getRecord());
					++shardRecordCount[actualValues[i]];
				}
				client.flush();

				int avg = count / shardCount;
				int diff = avg * 2 / 10;
				for (int c : shardRecordCount) {
					Assert.assertTrue(Math.abs(c - avg) < diff);
				}
				System.out.println(Arrays.toString(shardRecordCount));
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * shard no_pk，has dk
	 * Method: put(Put put).
	 */
	@Test
	public void testShard005() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_shard_005";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "begin;create table " + tableName + "(id int not null,name text not null,address text);call set_table_property('" + tableName + "','distribution_key','name');end;";

			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);

				int shardCount = getShardCount(client, schema);

				Assert.assertTrue(shardCount > 1);
				ShardPolicy policy = new DistributionKeyShardPolicy();

				policy.init(shardCount);

				final int count = 10000;
				int[] actualValues = new int[count];
				int[] shardRecordCount = new int[shardCount];
				for (int i = 0; i < count; ++i) {
					Put put = new Put(schema);
					put.setObject(0, i);
					put.setObject(1, i % 2 == 1 ? "name0" : null);
					put.setObject(2, "address0");
					client.put(put);
					actualValues[i] = policy.locate(put.getRecord());
					++shardRecordCount[actualValues[i]];
				}
				client.flush();

				int[] expectValues = client.sql(c -> {
					int[] ret = new int[count];
					int actualCount = 0;
					try (Statement stat = conn.createStatement()) {
						try (ResultSet rs = stat.executeQuery("select hg_shard_id,id from " + tableName)) {
							while (rs.next()) {
								ret[rs.getInt(2)] = rs.getInt(1);
								++actualCount;
							}
						}
					}
					if (count == actualCount) {
						return ret;
					} else {
						return null;
					}
				}).get();
				Assert.assertEquals(expectValues, actualValues);
				System.out.println(Arrays.toString(shardRecordCount));
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}
}
