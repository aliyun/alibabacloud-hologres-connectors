package com.alibaba.hologres.client;

import com.alibaba.hologres.client.model.TableSchema;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

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
}
