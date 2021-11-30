/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.WriteMode;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.model.TableSchema;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Timestamp;

/**
 * HoloClient defaultValue Tester.
 */
public class HoloClientDefaultValueTest extends HoloClientTestBase {

	/**
	 * INSERT_IGNORE.
	 * Method: put(Put put).
	 */
	@Test
	public void testDefaultValue01() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_IGNORE);
		config.setInputNumberAsEpochMsForDatetimeColumn(true);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_default_001";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(\n" +
					"    \"iD\" int not null,\n" +
					"    c_str_nullable text ,\n" +
					"    c_str_d text not null default '-',\n" +
					"    c_str text not null,\n" +
					"\n" +
					"    c_int8_nullable int8 ,\n" +
					"    c_int8_d int8 not null default 6,\n" +
					"    c_int8 int8 not null,\n" +
					"\n" +
					"    c_numeric_nullable numeric(10,5) ,\n" +
					"    c_numeric_d numeric(10,5) not null default 3.141,\n" +
					"    c_numeric numeric(10,5) not null,\n" +
					"\n" +
					"\n" +
					"    c_timestamp_nullable timestamptz ,\n" +
					"    c_timestamp_d timestamptz not null default now(),\n" +
					"    c_timestamp timestamptz not null,\n" +
					"\n" +
					"    c_timestamp1_nullable timestamptz ,\n" +
					"    c_timestamp1_d timestamptz not null default '1970-01-01 00:00:00',\n" +
					"    c_timestamp1 timestamptz not null,\n" +
					"\n" +
					"    c_bool_nullable boolean,\n" +
					"    c_bool_d boolean not null default true,\n" +
					"    c_bool boolean not null,\n" +
					"    primary key(\"iD\")\n" +
					")";

			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);

				long minTs = System.currentTimeMillis();
				int index = -1;
				Put put = new Put(schema);
				put.setObject(++index, 0);
				put.setObject(++index, "name0");
				put.setObject(++index, "name1");
				put.setObject(++index, "name2");
				put.setObject(++index, 1);
				put.setObject(++index, 2);
				put.setObject(++index, 3);
				put.setObject(++index, "10.2");
				put.setObject(++index, "10.3");
				put.setObject(++index, "10.4");
				put.setObject(++index, 100L);
				put.setObject(++index, 200L);
				put.setObject(++index, 300L);
				put.setObject(++index, 400L);
				put.setObject(++index, 500L);
				put.setObject(++index, 600L);
				put.setObject(++index, true);
				put.setObject(++index, true);
				put.setObject(++index, true);
				client.put(put);

				put = new Put(schema);
				put.setObject(0, 1);
				client.put(put);

				client.flush();

				long maxTs = System.currentTimeMillis();

				Record r = client.get(Get.newBuilder(schema).setPrimaryKey("iD", 0).build()).get();
				index = 0;
				Assert.assertEquals("name0", r.getObject(++index));
				Assert.assertEquals("name1", r.getObject(++index));
				Assert.assertEquals("name2", r.getObject(++index));
				Assert.assertEquals(1L, r.getObject(++index));
				Assert.assertEquals(2L, r.getObject(++index));
				Assert.assertEquals(3L, r.getObject(++index));
				Assert.assertEquals(new BigDecimal("10.20000"), r.getObject(++index));
				Assert.assertEquals(new BigDecimal("10.30000"), r.getObject(++index));
				Assert.assertEquals(new BigDecimal("10.40000"), r.getObject(++index));
				Assert.assertEquals(100L, ((Timestamp) r.getObject(++index)).getTime());
				Assert.assertEquals(200L, ((Timestamp) r.getObject(++index)).getTime());
				Assert.assertEquals(300L, ((Timestamp) r.getObject(++index)).getTime());
				Assert.assertEquals(400L, ((Timestamp) r.getObject(++index)).getTime());
				Assert.assertEquals(500L, ((Timestamp) r.getObject(++index)).getTime());
				Assert.assertEquals(600L, ((Timestamp) r.getObject(++index)).getTime());
				Assert.assertEquals(true, r.getObject(++index));
				Assert.assertEquals(true, r.getObject(++index));
				Assert.assertEquals(true, r.getObject(++index));

				r = client.get(Get.newBuilder(schema).setPrimaryKey("iD", 1).build()).get();
				index = 0;
				Assert.assertNull(r.getObject(++index));
				Assert.assertEquals("-", r.getObject(++index));
				Assert.assertEquals("", r.getObject(++index));
				Assert.assertNull(r.getObject(++index));
				Assert.assertEquals(6L, r.getObject(++index));
				Assert.assertEquals(0L, r.getObject(++index));
				Assert.assertNull(r.getObject(++index));
				Assert.assertEquals(new BigDecimal("3.14100"), r.getObject(++index));
				Assert.assertEquals(new BigDecimal("0.00000"), r.getObject(++index));
				Assert.assertNull(r.getObject(++index));
				long temp = ((Timestamp) r.getObject(++index)).getTime();
				Assert.assertTrue(minTs <= temp && temp <= maxTs);
				Assert.assertEquals(0L, ((Timestamp) r.getObject(++index)).getTime());
				Assert.assertNull(r.getObject(++index));
				Assert.assertEquals(-8 * 3600L * 1000L, ((Timestamp) r.getObject(++index)).getTime());
				Assert.assertEquals(0L, ((Timestamp) r.getObject(++index)).getTime());
				Assert.assertNull(r.getObject(++index));
				Assert.assertEquals(true, r.getObject(++index));
				Assert.assertEquals(false, r.getObject(++index));
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * INSERT_UPDATE.
	 * Method: put(Put put).
	 */
	@Test
	public void testDefaultValue02() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setInputNumberAsEpochMsForDatetimeColumn(true);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_default_002";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(\n" +
					"    \"iD\" int not null,\n" +
					"    c_str_nullable text ,\n" +
					"    c_str_d text not null default '-',\n" +
					"    c_str text not null,\n" +
					"\n" +
					"    c_int8_nullable int8 ,\n" +
					"    c_int8_d int8 not null default 6,\n" +
					"    c_int8 int8 not null,\n" +
					"\n" +
					"    c_numeric_nullable numeric(10,5) ,\n" +
					"    c_numeric_d numeric(10,5) not null default 3.141,\n" +
					"    c_numeric numeric(10,5) not null,\n" +
					"\n" +
					"\n" +
					"    c_timestamp_nullable timestamptz ,\n" +
					"    c_timestamp_d timestamptz not null default now(),\n" +
					"    c_timestamp timestamptz not null,\n" +
					"\n" +
					"    c_timestamp1_nullable timestamptz ,\n" +
					"    c_timestamp1_d timestamptz not null default '1970-01-01 00:00:00',\n" +
					"    c_timestamp1 timestamptz not null,\n" +
					"\n" +
					"    c_bool_nullable boolean,\n" +
					"    c_bool_d boolean not null default true,\n" +
					"    c_bool boolean not null,\n" +
					"    primary key(\"iD\")\n" +
					")";

			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);

				long minTs = System.currentTimeMillis();
				int index = -1;
				Put put = new Put(schema);
				put.setObject(++index, 0);
				put.setObject(++index, "name0");
				put.setObject(++index, "name1");
				put.setObject(++index, "name2");
				put.setObject(++index, 1);
				put.setObject(++index, 2);
				put.setObject(++index, 3);
				put.setObject(++index, "10.2");
				put.setObject(++index, "10.3");
				put.setObject(++index, "10.4");
				put.setObject(++index, 100L);
				put.setObject(++index, 200L);
				put.setObject(++index, 300L);
				put.setObject(++index, 400L);
				put.setObject(++index, 500L);
				put.setObject(++index, 600L);
				put.setObject(++index, true);
				put.setObject(++index, true);
				put.setObject(++index, true);
				client.put(put);

				client.flush();
				put = new Put(schema);
				put.setObject(0, 0);
				put.setObject(3, "new_name");
				client.put(put);

				client.flush();

				Record r = client.get(Get.newBuilder(schema).setPrimaryKey("iD", 0).build()).get();
				index = 0;
				Assert.assertEquals("name0", r.getObject(++index));
				Assert.assertEquals("name1", r.getObject(++index));
				Assert.assertEquals("new_name", r.getObject(++index));
				Assert.assertEquals(1L, r.getObject(++index));
				Assert.assertEquals(2L, r.getObject(++index));
				Assert.assertEquals(3L, r.getObject(++index));
				Assert.assertEquals(new BigDecimal("10.20000"), r.getObject(++index));
				Assert.assertEquals(new BigDecimal("10.30000"), r.getObject(++index));
				Assert.assertEquals(new BigDecimal("10.40000"), r.getObject(++index));
				Assert.assertEquals(100L, ((Timestamp) r.getObject(++index)).getTime());
				Assert.assertEquals(200L, ((Timestamp) r.getObject(++index)).getTime());
				Assert.assertEquals(300L, ((Timestamp) r.getObject(++index)).getTime());
				Assert.assertEquals(400L, ((Timestamp) r.getObject(++index)).getTime());
				Assert.assertEquals(500L, ((Timestamp) r.getObject(++index)).getTime());
				Assert.assertEquals(600L, ((Timestamp) r.getObject(++index)).getTime());
				Assert.assertEquals(true, r.getObject(++index));
				Assert.assertEquals(true, r.getObject(++index));
				Assert.assertEquals(true, r.getObject(++index));

			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * serial.
	 * Method: put(Put put).
	 */
	@Test
	public void testDefaultValue03() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_IGNORE);
		config.setInputNumberAsEpochMsForDatetimeColumn(true);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_default_003";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id serial,name text,primary key (id));";

			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);

				Put put = new Put(schema);
				put.setObject(1, "a");
				client.put(put);

				put = new Put(schema);
				put.setObject(1, "b");
				client.put(put);

				client.flush();

				Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
				Assert.assertEquals("a", r.getObject(1));

				r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).build()).get();
				Assert.assertEquals("b", r.getObject(1));

			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}
}
