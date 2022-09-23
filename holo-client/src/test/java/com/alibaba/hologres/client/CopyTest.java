/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.copy.CopyInOutputStream;
import com.alibaba.hologres.client.copy.CopyUtil;
import com.alibaba.hologres.client.copy.RecordBinaryOutputStream;
import com.alibaba.hologres.client.copy.RecordOutputStream;
import com.alibaba.hologres.client.copy.RecordTextOutputStream;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;

import java.io.OutputStream;
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
 * Fixed Copy测试用例.
 */
public class CopyTest extends HoloClientTestBase {

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
			// , "INTERVAL"
			// , "TIMETZ"
			// , "TIME"
			//, "INET"
			// , "MONEY" // 难搞，不支持了
			//, "OID"
			//, "UUID"
			, "SERIAL"
			, "DECIMAL"
			, "CHAR"
			, "VARCHAR"
			//, "BIT"
			//, "VARBIT"
			, "roaringbitmap"};

	private String buildCreateTableSql(String tableName, String typeName) {
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
				Assert.assertTrue("unsupported type " + typeName, false);
		}
		return createSql;
	}

	private Record buildRecord(int i, TableSchema schema, String typeName) {
		Record put = new Record(schema);
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
					Assert.assertTrue("unsupported type " + typeName, false);
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
		return put;
	}

	@Test
	public void testCopy001() throws Exception {
		if (properties == null) {
			return;
		}
		boolean[] binaryList = new boolean[]{false, true};
		try (Connection conn = buildConnection()) {

			for (boolean binary : binaryList) {
				for (String typeName : ALL_TYPE) {
					String tableName = "\"holo_client_copy_type_" + typeName + "\"";
					String dropSql = "drop table if exists " + tableName;
					String createSql = buildCreateTableSql(tableName, typeName);
					try {
						LOG.info("current type {}, binary {}", typeName, binary);

						execute(conn, new String[]{"CREATE EXTENSION if not exists roaringbitmap", dropSql, createSql});

						PgConnection pgConn = conn.unwrap(PgConnection.class);
						TableSchema schema = ConnectionUtil.getTableSchema(conn, TableName.valueOf(tableName));
						CopyManager copyManager = new CopyManager(conn.unwrap(PgConnection.class));
						String copySql = CopyUtil.buildCopyInSql(schema, binary, WriteMode.INSERT_OR_REPLACE);
						LOG.info("copySql : {}", copySql);
						try (OutputStream os = new CopyInOutputStream(copyManager.copyIn(copySql))) {
							RecordOutputStream ros = binary ?
									new RecordBinaryOutputStream(os, schema, pgConn.getTimestampUtils(), 1024 * 1024 * 10) :
									new RecordTextOutputStream(os, schema, pgConn.getTimestampUtils(), 1024 * 1024 * 10);
							for (int i = 0; i < 10; ++i) {
								ros.putRecord(buildRecord(i, schema, typeName));
							}
						}

						int count = 0;
						try (Statement stat = conn.createStatement()) {
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
													Assert.assertEquals(rs.getString(1), i * 1000L + 123L, rs.getTimestamp(1).getTime());
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
													Assert.assertArrayEquals(new byte[]{(byte) i, (byte) (i + 1)}, rs.getBytes(1));
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
													Assert.assertTrue("unsupported type " + typeName, false);
											}
										}
										++count;
									}

								}
								Assert.assertEquals(10, count);
							}
						}
					} finally {
						execute(conn, new String[]{dropSql});
					}
				}
			}
		}
	}
}
