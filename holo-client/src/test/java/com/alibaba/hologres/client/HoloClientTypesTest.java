/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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

			execute(conn, new String[]{"CREATE EXTENSION if not exists roaringbitmap", dropSql, createSql});

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
				put = new Put(schema);
				put.setObject(0, 1);
				put.setObject(1, rb); //{1,4,5}
				client.put(put);

				client.flush();
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

				ret = client.sql((con) -> {
					try (Statement stat = con.createStatement()) {
						try (ResultSet rs = stat.executeQuery("select rb_cardinality(b) from " + tableName + " where id=1")) {
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

	private static final String[] ALL_TYPE = new String[]{"INT"
			, "BIGINT"
			, "BOOL"
			, "FLOAT4"
			, "FLOAT8"
			, "TEXT"
			, "TIMESTAMPTZ"
			, "DATE"
			, "TIMESTAMP"
			, "SMALLINT"
			, "JSON"
			, "JSONB"
			, "BYTEA"
			, "INTERVAL"
			, "TIMETZ"
			, "TIME"
			, "INET"
			// , "MONEY" // 难搞，不支持了
			, "OID"
			, "UUID"
			, "SERIAL"
			, "DECIMAL"
			, "CHAR"
			, "VARCHAR"
			, "BIT"
			, "VARBIT"
			, "roaringbitmap"};

	/**
	 * allType.
	 * Method: put(Put put).
	 */
	@Test
	public void testALLTypeInsert() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		//config.setUseLegacyPutHandler(true);
		try (HoloClient client = new HoloClient(config)) {
			client.sql(conn -> {
				execute(conn, new String[]{"CREATE EXTENSION if not exists roaringbitmap"});
				return null;
			}).get();
			for (String typeName : ALL_TYPE) {
				String tableName = "holo_client_type_" + typeName;
				String dropSql = "drop table if exists " + tableName;
				String createSql = null;
				switch (typeName) {
					case "SMALLINT":
					case "INT":
					case "BIGINT":
					case "FLOAT4":
					case "FLOAT8":
					case "TEXT":
					case "MONEY":
					case "OID":
					case "SERIAL":
					case "TIMESTAMPTZ":
					case "TIMESTAMP":
					case "DATE":
					case "TIMETZ":
					case "TIME":
					case "BOOL":
					case "JSON":
					case "JSONB":
					case "BYTEA":
					case "INTERVAL":
					case "INET":
					case "UUID":
					case "roaringbitmap":
						createSql = "create table " + tableName + "(id " + typeName + ", pk int primary key)";
						break;
					case "DECIMAL":
						createSql = "create table " + tableName + "(id " + typeName + "(18,5), pk int primary key)";
						break;
					case "CHAR":
					case "VARCHAR":
					case "BIT":
					case "VARBIT":
						createSql = "create table " + tableName + "(id " + typeName + "(5), pk int primary key)";
						break;
					default:
						Assert.assertTrue(false, "unsupported type " + typeName);
				}

				String finalCreateSql = createSql;
				client.sql(conn -> {
					execute(conn, new String[]{dropSql, finalCreateSql});
					return null;
				}).get();

				try {
					TableSchema schema = client.getTableSchema(tableName, true);
					for (int i = 0; i < 10; ++i) {
						Put put = new Put(schema);
						if (i != 5) {
							switch (typeName) {
								case "SMALLINT":
								case "INT":
								case "BIGINT":
								case "FLOAT4":
								case "FLOAT8":
								case "TEXT":
								case "OID":
								case "SERIAL":
								case "DECIMAL":
								case "CHAR":
								case "VARCHAR":
									put.setObject(0, i);
									break;
								case "BOOL":
									put.setObject(0, i % 2 == 0);
									break;
								case "TIMESTAMPTZ":
								case "TIMESTAMP":
									put.setObject(0, new Timestamp(i * 1000L + 123L));
									break;
								case "DATE":
									put.setObject(0, "2021-01-02");
									break;
								case "TIMETZ":
								case "TIME":
									put.setObject(0, "15:12:14.123+08");
									break;
								case "JSON":
								case "JSONB":
									put.setObject(0, "{\"a\":\"b\"}");
									break;
								case "BYTEA":
									put.setObject(0, new byte[]{(byte) i, (byte) (i + 1)});
									break;
								case "INTERVAL":
									put.setObject(0, i + " hours");
									break;
								case "INET":
									put.setObject(0, "127.0.0.1");
									break;
								case "UUID":
									put.setObject(0, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
									break;
								case "BIT":
									put.setObject(0, "0");
									break;
								case "VARBIT":
									put.setObject(0, "101");
									break;
								case "roaringbitmap":
									put.setObject(0, new byte[]{58, 48, 0, 0, 1, 0, 0, 0, 0, 0, 2, 0, 16, 0, 0, 0, 1, 0, 4, 0, 5, 0}); //{1,4,5}
									break;
								default:
									Assert.assertTrue(false, "unsupported type " + typeName);
							}
						} else {
							switch (typeName) {
								case "SERIAL":
									put.setObject(0, i);
									break;
								default:
							}
						}
						put.setObject(1, i);
						client.put(put);
					}
					client.flush();

					int ret = client.sql((con) -> {
						int count = 0;
						try (Statement stat = con.createStatement()) {
							LOG.info("current type:{}", typeName);
							if ("roaringbitmap".equals(typeName)) {
								try (ResultSet rs = stat.executeQuery("select rb_cardinality(id), pk from " + tableName)) {
									while (rs.next()) {
										int i = rs.getInt(2);
										if (5 == i) {
											Assert.assertNull(rs.getObject(1));
										} else {
											Assert.assertEquals(3L, rs.getLong(1));
										}
										++count;
									}
								}
							} else {
								try (ResultSet rs = stat.executeQuery("select * from " + tableName)) {
									DateFormat intervalDF = new SimpleDateFormat("HH:mm:ss");
									long intervalBase = 0L;
									try {
										intervalBase = intervalDF.parse("00:00:00").getTime();
									} catch (ParseException e) {
										throw new SQLException(e);
									}
									while (rs.next()) {
										int i = rs.getInt(2);
										if (5 == i) {
											if ("SERIAL".equals(typeName)) {
												Assert.assertEquals(String.valueOf(i), rs.getString(1));
											} else {
												Assert.assertNull(rs.getObject(1));
											}
										} else {
											switch (typeName) {
												case "SMALLINT":
												case "INT":
												case "BIGINT":
												case "FLOAT4":
												case "FLOAT8":
												case "TEXT":
												case "OID":
												case "SERIAL":
												case "VARCHAR":
													Assert.assertEquals(String.valueOf(i), rs.getString(1));
													break;
												case "CHAR":
													Assert.assertEquals((i + "         ").substring(0, 5), rs.getString(1));
													break;
												case "DECIMAL":
													Assert.assertEquals(i + ".00000", rs.getString(1));
													break;
												case "BOOL":
													Assert.assertEquals(i % 2 == 0, rs.getBoolean(1));
													break;
												case "TIMESTAMPTZ":
												case "TIMESTAMP":
													Assert.assertEquals(i * 1000L + 123L, rs.getTimestamp(1).getTime(), rs.getString(1));
													break;
												case "DATE":
													Assert.assertEquals("2021-01-02", rs.getString(1));
													break;
												case "TIMETZ":
													Assert.assertEquals("15:12:14.123+08", rs.getString(1));
													break;
												case "TIME":
													Assert.assertEquals("15:12:14.123", rs.getString(1));
													break;
												case "JSON":
													Assert.assertEquals("{\"a\":\"b\"}", rs.getString(1));
													break;
												case "JSONB":
													Assert.assertEquals("{\"a\": \"b\"}", rs.getString(1));
													break;
												case "BYTEA":
													Assert.assertEquals(new byte[]{(byte) i, (byte) (i + 1)}, rs.getBytes(1));
													break;
												case "INTERVAL":
													Assert.assertEquals(intervalDF.format(new Date(intervalBase + i * 3600000L)), rs.getString(1));
													break;
												case "INET":
													Assert.assertEquals("127.0.0.1", rs.getString(1));
													break;
												case "UUID":
													Assert.assertEquals("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", rs.getString(1));
													break;
												case "BIT":
													Assert.assertEquals("00000", rs.getString(1));
													break;
												case "VARBIT":
													Assert.assertEquals("101", rs.getString(1));
													break;
												default:
													Assert.assertTrue(false, "unsupported type " + typeName);
											}
										}
										++count;
									}
								}
							}
						}
						return count;
					}).get();
					Assert.assertEquals(10, ret);
				} finally {
					//execute(conn, new String[]{dropSql});
				}
			}
		}
	}
}
