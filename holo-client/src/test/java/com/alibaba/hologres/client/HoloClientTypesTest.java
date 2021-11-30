/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.WriteMode;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.model.TableSchema;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 特殊类型的支持测试.
 */
public class HoloClientTypesTest extends HoloClientTestBase {

	/**
	 * jsonb.
	 * Method: put(Put put).
	 */
	@Test
	public void testJsonb() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_jsonb";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,b jsonb,primary key(id))";

			execute(conn, new String[]{dropSql, createSql});

			TableSchema schema = client.getTableSchema(tableName);
			System.out.println(schema.getColumn(1).getTypeName());
			Put put = new Put(schema);
			put.setObject(0, 0);
			put.setObject(1, "{\"a\":\"cccc\"}");
			client.put(put);

			put = new Put(schema);
			put.setObject(0, 1);
			client.put(put);

			client.flush();

			Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
			Assert.assertEquals("{\"a\": \"cccc\"}", r.getObject("b").toString());

			r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
			Assert.assertNull(r.getObject("b"));

			execute(conn, new String[]{dropSql});
		}
	}

	int convert(char c) {
		if ('0' <= c && c <= '9') {
			return c - '0';
		} else if ('A' <= c && c <= 'F') {
			return c - 'A';
		} else if ('a' <= c && c <= 'f') {
			return c - 'a';
		} else {
			throw new RuntimeException("unknow " + c);
		}

	}

	byte[] convert(String text) {
		byte[] b = new byte[text.length() / 2];
		for (int i = 0; i < text.length() / 2; ++i) {
			int c = convert(text.charAt(i * 2));
			int c1 = convert(text.charAt(i * 2 + 1));
			b[i] = (byte) (c * 16 + c1);

		}
		return b;
	}

	/**
	 * GIS.
	 * Method: put(Put put).
	 */
	@Test
	public void testGIS() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_gis";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int PRIMARY KEY, b geography(POINT,4326))";

			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);
				System.out.println(schema.getColumn(1).getTypeName());
				Put put = new Put(schema);
				put.setObject(0, 0);
				put.setObject(1, "POINT(-110 90)");
				client.put(put);

				client.flush();

				String ret = client.sql((con) -> {
					try (Statement stat = con.createStatement()) {
						try (ResultSet rs = stat.executeQuery("select ST_AsText(b) from " + tableName + " where id=0")) {
							if (rs.next()) {
								return rs.getString(1);
							}
						}
					}
					return null;
				}).get();
				Assert.assertEquals("POINT(-110 90)", ret);
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * roaringbitmap.
	 * Method: put(Put put).
	 */
	@Test
	public void testRB() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_rb";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int PRIMARY KEY, b roaringbitmap)";

			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);
				System.out.println(schema.getColumn(1).getTypeName());
				Put put = new Put(schema);
				put.setObject(0, 0);
				put.setObject(1, new byte[]{58, 48, 0, 0, 1, 0, 0, 0, 0, 0, 2, 0, 16, 0, 0, 0, 1, 0, 4, 0, 5, 0}); //{1,4,5}
				client.put(put);

				client.flush();

				Record record = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
				Assert.assertNotNull(record);
				Object rb = record.getObject(1);
				System.out.println("className:" + rb.getClass().getName());
				Assert.assertNotNull(rb);
				long ret = client.sql((con) -> {
					try (Statement stat = con.createStatement()) {
						try (ResultSet rs = stat.executeQuery("select rb_cardinality(b) from " + tableName + " where id=0")) {
							if (rs.next()) {
								return rs.getLong(1);
							}
						}
					}
					return null;
				}).get();
				Assert.assertEquals(3L, ret);
			} finally {
				//execute(conn, new String[]{dropSql});
			}
		}
	}
}
