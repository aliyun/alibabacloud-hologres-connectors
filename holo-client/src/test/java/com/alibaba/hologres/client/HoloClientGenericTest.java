package com.alibaba.hologres.client;

import com.alibaba.hologres.client.model.TableSchema;
import org.testng.Assert;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * HoloClient Tester.
 * HoloClientTest的行数超过了style-check的上限4000，之后的通用测试可以在本文件中实现.
 */
public class HoloClientGenericTest extends HoloClientTestBase {
	/**
	 * HoloConfig 的enableDeduplication参数设置测试, 设置为false表示不进行去重.
	 */
	@Test
	public void testDeduplicationWhenInsert() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();

		try (Connection conn = buildConnection()) {
			String tableName = "\"holO_client_put_de_dup\"";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,id2 int not null, name text, primary key(id,id2))";
			String enableBinlog = "call set_table_property('" + tableName + "', 'binlog.level', 'replica')";
			execute(conn, new String[]{dropSql, createSql, enableBinlog});

			// default true, 默认会去重
			Assert.assertTrue(config.isEnableDeduplication());
			try (HoloClient client = new HoloClient(config)) {
				TableSchema schema = client.getTableSchema(tableName);
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 0);
					put2.setObject("id2", 1);
					put2.setObject("name", "aaa");
					client.put(put2);
				}
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 0);
					put2.setObject("id2", 1);
					put2.setObject("name", "bbb");
					client.put(put2);
				}
				client.flush();
				{
					Put put2 = new Put(schema);
					put2.getRecord().setType(Put.MutationType.DELETE);
					put2.setObject("id", 0);
					put2.setObject("id2", 1);
					client.put(put2);
				}
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 0);
					put2.setObject("id2", 1);
					put2.setObject("name", "ccc");
					client.put(put2);
				}
				{
					Put put2 = new Put(schema);
					put2.getRecord().setType(Put.MutationType.DELETE);
					put2.setObject("id", 0);
					put2.setObject("id2", 1);
					client.put(put2);
				}
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 0);
					put2.setObject("id2", 1);
					put2.setObject("name", "ddd");
					client.put(put2);
				}
				client.flush();

				/*
				 hg_binlog_event_type | id | id2 | name
				----------------------+----+-----+------
									5 |  0 |   1 | bbb
									3 |  0 |   1 | bbb
									7 |  0 |   1 | ddd
				*/
				int count = 0;
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select hg_binlog_event_type,* from " + tableName + " order by hg_binlog_lsn")) {
						while (rs.next()) {
							++count;
							Assert.assertEquals(0, rs.getInt(2));
							Assert.assertEquals(1, rs.getInt(3));
							if (count == 1) {
								Assert.assertEquals(5, rs.getInt(1));
								Assert.assertEquals("bbb", rs.getString(4));
							} else if (count == 2) {
								Assert.assertEquals(3, rs.getInt(1));
								Assert.assertEquals("bbb", rs.getString(4));
							} else if (count == 3) {
								Assert.assertEquals(7, rs.getInt(1));
								Assert.assertEquals("ddd", rs.getString(4));
							} else {
								throw new RuntimeException("count should not greater than 3");
							}
						}
					}
				}
				Assert.assertEquals(3, count);
			}

			execute(conn, new String[]{dropSql, createSql, enableBinlog});
			// 设为false，不去重
			config.setEnableDeduplication(false);
			try (HoloClient client = new HoloClient(config)) {
				TableSchema schema = client.getTableSchema(tableName);
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 0);
					put2.setObject("id2", 1);
					put2.setObject("name", "aaa");
					client.put(put2);
				}
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 0);
					put2.setObject("id2", 1);
					put2.setObject("name", "bbb");
					client.put(put2);
				}
				client.flush();
				{
					Put put2 = new Put(schema);
					put2.getRecord().setType(Put.MutationType.DELETE);
					put2.setObject("id", 0);
					put2.setObject("id2", 1);
					client.put(put2);
				}
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 0);
					put2.setObject("id2", 1);
					put2.setObject("name", "ccc");
					client.put(put2);
				}
				{
					Put put2 = new Put(schema);
					put2.getRecord().setType(Put.MutationType.DELETE);
					put2.setObject("id", 0);
					put2.setObject("id2", 1);
					client.put(put2);
				}
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 0);
					put2.setObject("id2", 1);
					put2.setObject("name", "ddd");
					client.put(put2);
				}
				client.flush();

				/*
				 hg_binlog_event_type | id | id2 | name
				----------------------+----+-----+------
									5 |  0 |   1 | aaa
									3 |  0 |   1 | aaa
									7 |  0 |   1 | bbb
									2 |  0 |   1 | bbb
									5 |  0 |   1 | ccc
									2 |  0 |   1 | ccc
									5 |  0 |   1 | ddd
				*/
				int count = 0;
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select hg_binlog_event_type,* from " + tableName + " order by hg_binlog_lsn")) {
						while (rs.next()) {
							++count;
							Assert.assertEquals(0, rs.getInt(2));
							Assert.assertEquals(1, rs.getInt(3));
							if (count == 1) {
								Assert.assertEquals(5, rs.getInt(1));
								Assert.assertEquals("aaa", rs.getString(4));
							} else if (count == 2) {
								Assert.assertEquals(3, rs.getInt(1));
								Assert.assertEquals("aaa", rs.getString(4));
							} else if (count == 3) {
								Assert.assertEquals(7, rs.getInt(1));
								Assert.assertEquals("bbb", rs.getString(4));
							} else if (count == 4) {
								Assert.assertEquals(2, rs.getInt(1));
								Assert.assertEquals("bbb", rs.getString(4));
							} else if (count == 5) {
								Assert.assertEquals(5, rs.getInt(1));
								Assert.assertEquals("ccc", rs.getString(4));
							} else if (count == 6) {
								Assert.assertEquals(2, rs.getInt(1));
								Assert.assertEquals("ccc", rs.getString(4));
							} else if (count == 7) {
								Assert.assertEquals(5, rs.getInt(1));
								Assert.assertEquals("ddd", rs.getString(4));
							} else {
								throw new RuntimeException("count should not greater than 7");
							}
						}
					}
				}
				Assert.assertEquals(count, 7);
			}
			execute(conn, new String[]{dropSql});
		}
	}

	/**
	 * HoloConfig 的enableAggressive参数设置测试, 设置为true表示激进模式写入,期望数据量较小时可以有效减小延迟.
	 */
	@Test
	public void testAggressiveInsert() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteBatchSize(256);
		config.setWriteThreadSize(1);
		config.setWriteMaxIntervalMs(10000);

		try (Connection conn = buildConnection()) {
			String tableName = "\"holO_client_put_aggressive\"";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null, name text, primary key(id));" +
			"call set_table_property('" + tableName + "', 'shard_count', '1');\n";
			String enableBinlog = "call set_table_property('" + tableName + "', 'binlog.level', 'replica')";
			execute(conn, new String[]{dropSql, "begin;", createSql, "commit;", enableBinlog});

			long interval1 = 0;
			long interval2 = 0;

			// default false, 默认不走激进模式
			Assert.assertFalse(config.isEnableAggressive());
			try (HoloClient client = new HoloClient(config)) {
				TableSchema schema = client.getTableSchema(tableName);
				{
					for (int i = 0; i < 50; i++) {
						Put put2 = new Put(schema);
						put2.setObject("id", i);
						put2.setObject("name", "aaa");
						client.put(put2);
						Thread.sleep(100);
					}
				}
				client.flush();
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery(String.format("select max(hg_binlog_timestamp_us) - min(hg_binlog_timestamp_us) from  %s;", tableName))) {
						while (rs.next()) {
							// 非激进模式,数据会攒批提交,写入时间相差很小
							interval1 = rs.getLong(1);
						}
					}
				}
			}

			execute(conn, new String[]{dropSql, "begin;", createSql, "commit;", enableBinlog});
			// 设为true，激进模式写入
			config.setEnableAggressive(true);
			try (HoloClient client = new HoloClient(config)) {
				TableSchema schema = client.getTableSchema(tableName);
				{
					for (int i = 0; i < 50; i++) {
						Put put2 = new Put(schema);
						put2.setObject("id", i);
						put2.setObject("name", "aaa");
						client.put(put2);
						Thread.sleep(100);
					}
				}
				client.flush();
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery(String.format("select max(hg_binlog_timestamp_us) - min(hg_binlog_timestamp_us) from  %s;", tableName))) {
						while (rs.next()) {
							// 激进模式,连接空闲即提交,写入时间相差接近5秒钟
							interval2 = rs.getLong(1);
						}
					}
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
			Assert.assertTrue(interval2 >= interval1);
		}
	}

	@Ignore
	@Test
	public void testTableVersionChangeWhenInsert() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteBatchSize(256);
		config.setWriteThreadSize(1);
		config.setWriteMaxIntervalMs(3000);

		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "\"holO_client_table_version_change\"";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null, name text, primary key(id));";
			execute(conn, new String[]{dropSql, createSql});

			AtomicBoolean running = new AtomicBoolean(true);
			AtomicReference<Exception> failed = new AtomicReference(null);
			ExecutorService es = new ThreadPoolExecutor(20, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(2), r -> {
				Thread t = new Thread(r);
				return t;
			}, new ThreadPoolExecutor.AbortPolicy());

			Runnable insert = () -> {
				try {
					int i = 0;
					while (running.get()) {
						TableSchema schema = client.getTableSchema(tableName, true);
						{
							Put put2 = new Put(schema);
							put2.setObject("id", i++);
							put2.setObject("name", "aaa");
							client.put(put2);
						}
					}
				} catch (Exception e) {
					failed.set(e);
				}
			};
			Runnable alter = () -> {
				try {
					int i = 0;
					while (running.get()) {
						Thread.sleep(100);
						execute(conn, new String[]{"alter table " + tableName + "add column c_" + i++ + " int;"});
					}
				} catch (Exception e) {
					failed.set(e);
				}
			};

			es.execute(insert);
			es.execute(alter);

			Thread.sleep(10000);
			running.set(false);

			es.shutdown();
			while (!es.awaitTermination(5000L, TimeUnit.MILLISECONDS)) {

			}
			client.flush();
			if (failed.get() != null) {
				Assert.fail("fail", failed.get());
			}
			execute(conn, new String[]{dropSql});
		}
	}
}
