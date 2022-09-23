/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.TimeZone;

/**
 * 类型转换.
 */
public class HoloClientTypeConvertTest extends HoloClientTestBase {
	public static Logger logger = LoggerFactory.getLogger(HoloClientTypeConvertTest.class);

	@Override
	protected void doBefore() throws Exception {
		TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.ofHours(8)));
	}

	/**
	 * String -> Int.
	 */
	@Test
	public void test001() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			client.setAsyncCommit(false);
			String tableName = "test_schema.\"holO_client_type_001_02\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,\"aaa\" int,\"address\" text,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});
			try {
				TableSchema schema = client.getTableSchema(tableName);

				for (int i = 0; i < 1; ++i) {
					Put put2 = new Put(schema);
					put2.setObject(0, 1);
					put2.setObject(1, "234");
					put2.setObject(2, "address");
					client.put(put2);
				}

				Get get = new Get(schema, new Object[]{1});
				get.addSelectColumns(new int[]{0, 1, 2});

				Record r = client.get(get).get();
				Assert.assertEquals(234, r.getObject(1));
				Assert.assertEquals("address", r.getObject(2));
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * String -> Timestamp.
	 */
	@Test
	public void test002() throws Exception {
		if (properties == null) {
			return;
		}

		String pattern = "yyyy-MM-dd HH:mm:ss.SSS";
		DateFormat formatter = new SimpleDateFormat(pattern);
		logger.info("=----------------------{}", formatter.format(formatter.parse("0000-00-00 00:00:00.000")));
		String[][] inputAndExpectList = new String[][]{
				{"2020-12-13 14:15:00", "2020-12-13 14:15:00.000"},
				{"2020-12-13 14:15:00.123", "2020-12-13 14:15:00.123"},
				{"2020-12-13 14:15:00.123+07:00", "2020-12-13 15:15:00.123"},
				{"20201213141500", "2020-12-13 14:15:00.000"},
				{"20201213141500123", "2020-12-13 14:15:00.123"},
				{"20201213141500123+07:00", "2020-12-13 15:15:00.123"},
				{"19651213141500", "1965-12-13 14:15:00.000"},
				{"19651213141500123", "1965-12-13 14:15:00.123"},
				{"19651213141500123+07:00", "1965-12-13 15:15:00.123"},
				{"0000-00-00 00:00:00", "1970-01-01 08:00:00.000"},
				{"1965121314150012", "1965-12-13 14:15:00.120"},
				{"1965121314150012+07:00", "1965-12-13 15:15:00.120"},
				{"19651213141500123000", "1965-12-13 14:15:00.123"},
				{"19651213141500123000+07:00", "1965-12-13 15:15:00.123"},
				//{"1965121314150002345", "1965-12-13 14:15:00.024"},
				//{"1965121314150002345+07:00", "1965-12-13 15:15:00.024"},
				//{"1965121314150012345", "1965-12-13 14:15:00.124"},
				//{"1965121314150012345+07:00", "1965-12-13 15:15:00.124"},
		};
		HoloConfig config = buildConfig();
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_type_002\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int ,ts timestamp, tstz timestamptz,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});
			try {
				TableSchema schema = client.getTableSchema(tableName);

				for (int i = 0; i < inputAndExpectList.length; ++i) {
					String[] pair = inputAndExpectList[i];
					Put put2 = new Put(schema);
					put2.setObject(0, i);
					put2.setObject(1, pair[0]);
					put2.setObject(2, pair[0]);
					client.put(put2);
				}
				client.flush();

				for (int i = 0; i < inputAndExpectList.length; ++i) {
					String[] pair = inputAndExpectList[i];
					Get get = new Get(schema, new Object[]{i});
					get.addSelectColumns(new int[]{0, 1, 2});

					Record r = client.get(get).get();
					Assert.assertEquals(pair[0], pair[1], formatter.format(new java.util.Date(((Timestamp) r.getObject(1)).getTime())));
					Assert.assertEquals(pair[0], pair[1], formatter.format(new java.util.Date(((Timestamp) r.getObject(2)).getTime())));
				}

			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * String -> Date.
	 */
	@Test
	public void test003() throws Exception {
		if (properties == null) {
			return;
		}

		String pattern = "yyyy-MM-dd";
		DateFormat formatter = new SimpleDateFormat(pattern);
		String[][] inputAndExpectList = new String[][]{
				{"2020-12-13 14:15:00", "2020-12-13"},
				{"2020-12-13 05:15:00.123", "2020-12-13"},
				{"2020-12-13 02:15:00.123+11:00", "2020-12-12"},
				{"20201213141500", "2020-12-13"},
				{"20201213051500123", "2020-12-13"},
				{"20201213021500123+11:00", "2020-12-12"},
				{"19651213141500", "1965-12-13"},
				{"19651213051500123", "1965-12-13"},
				{"19651213021500123+11:00", "1965-12-12"},
		};
		HoloConfig config = buildConfig();
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_type_003\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int ,ts date,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});
			try {
				TableSchema schema = client.getTableSchema(tableName);

				for (int i = 0; i < inputAndExpectList.length; ++i) {
					String[] pair = inputAndExpectList[i];
					Put put2 = new Put(schema);
					put2.setObject(0, i);
					put2.setObject(1, pair[0]);
					client.put(put2);
				}
				client.flush();

				for (int i = 0; i < inputAndExpectList.length; ++i) {
					String[] pair = inputAndExpectList[i];
					Get get = new Get(schema, new Object[]{i});

					Record r = client.get(get).get();
					Assert.assertEquals(pair[0], pair[1], formatter.format((Date) r.getObject(1)));
				}

			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * Long -> Timestamp.
	 */
	@Test
	public void test004() throws Exception {
		if (properties == null) {
			return;
		}

		String pattern = "yyyy-MM-dd HH:mm:ss.SSS";
		DateFormat formatter = new SimpleDateFormat(pattern);
		Long[] inputList = new Long[]{
				0L,
				-100L,
				1010101010L,
				20201213100000L
		};
		String[] expectList = new String[inputList.length];
		for (int i = 0; i < inputList.length; ++i) {
			expectList[i] = formatter.format(new Date(inputList[i]));
		}
		HoloConfig config = buildConfig();
		config.setInputNumberAsEpochMsForDatetimeColumn(true);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_type_004\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int ,ts timestamp, tstz timestamptz,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});
			try {
				TableSchema schema = client.getTableSchema(tableName);

				for (int i = 0; i < inputList.length; ++i) {
					Put put2 = new Put(schema);
					put2.setObject(0, i);
					put2.setObject(1, inputList[i]);
					put2.setObject(2, inputList[i]);
					client.put(put2);
				}
				client.flush();

				for (int i = 0; i < inputList.length; ++i) {
					Get get = new Get(schema, new Object[]{i});
					get.addSelectColumns(new int[]{0, 1, 2});

					Record r = client.get(get).get();
					Assert.assertEquals("" + inputList[i], expectList[i], formatter.format(new java.util.Date(((Timestamp) r.getObject(1)).getTime())));
					Assert.assertEquals("" + inputList[i], expectList[i], formatter.format(new java.util.Date(((Timestamp) r.getObject(2)).getTime())));
				}

			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * Long -> Timestamp.
	 */
	@Test
	public void test005() throws Exception {
		if (properties == null) {
			return;
		}

		String pattern = "yyyy-MM-dd HH:mm:ss.SSS";
		DateFormat formatter = new SimpleDateFormat(pattern);
		Long[] inputList = new Long[]{
				20201213100000L,
				20201213100000123L
		};
		String[] expectList = new String[]{
				"2020-12-13 10:00:00.000",
				"2020-12-13 10:00:00.123"
		};
		HoloConfig config = buildConfig();
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_type_005\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int ,ts timestamp, tstz timestamptz,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});
			try {
				TableSchema schema = client.getTableSchema(tableName);

				for (int i = 0; i < inputList.length; ++i) {
					Put put2 = new Put(schema);
					put2.setObject(0, i);
					put2.setObject(1, inputList[i]);
					put2.setObject(2, inputList[i]);
					client.put(put2);
				}
				client.flush();

				for (int i = 0; i < inputList.length; ++i) {
					Get get = new Get(schema, new Object[]{i});
					get.addSelectColumns(new int[]{0, 1, 2});

					Record r = client.get(get).get();
					Assert.assertEquals("" + inputList[i], expectList[i], formatter.format(new java.util.Date(((Timestamp) r.getObject(1)).getTime())));
					Assert.assertEquals("" + inputList[i], expectList[i], formatter.format(new java.util.Date(((Timestamp) r.getObject(2)).getTime())));
				}

			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * Long -> Date.
	 */
	@Test
	public void test006() throws Exception {
		if (properties == null) {
			return;
		}

		String pattern = "yyyy-MM-dd";
		DateFormat formatter = new SimpleDateFormat(pattern);
		Long[] inputList = new Long[]{
				0L,
				-100L,
				1010101010L,
				20201213100000L
		};
		String[] expectList = new String[inputList.length];
		for (int i = 0; i < inputList.length; ++i) {
			expectList[i] = formatter.format(new Date(inputList[i]));
		}
		HoloConfig config = buildConfig();
		config.setInputNumberAsEpochMsForDatetimeColumn(true);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_type_006\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int ,ts date,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});
			try {
				TableSchema schema = client.getTableSchema(tableName);

				for (int i = 0; i < inputList.length; ++i) {
					Put put2 = new Put(schema);
					put2.setObject(0, i);
					put2.setObject(1, inputList[i]);
					client.put(put2);
				}
				client.flush();

				for (int i = 0; i < inputList.length; ++i) {
					Get get = new Get(schema, new Object[]{i});

					Record r = client.get(get).get();
					Assert.assertEquals("" + inputList[i], expectList[i], formatter.format((Date) r.getObject(1)));
				}

			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * Long -> Date.
	 */
	@Test
	public void test007() throws Exception {
		if (properties == null) {
			return;
		}

		String pattern = "yyyy-MM-dd";
		DateFormat formatter = new SimpleDateFormat(pattern);
		Long[] inputList = new Long[]{
				20201213100000L,
				20201213L
		};
		String[] expectList = new String[]{
				"2020-12-13",
				"2020-12-13"
		};
		HoloConfig config = buildConfig();
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_type_007\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int ,ts date,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});
			try {
				TableSchema schema = client.getTableSchema(tableName);

				for (int i = 0; i < inputList.length; ++i) {
					Put put2 = new Put(schema);
					put2.setObject(0, i);
					put2.setObject(1, inputList[i]);
					client.put(put2);
				}
				client.flush();

				for (int i = 0; i < inputList.length; ++i) {
					Get get = new Get(schema, new Object[]{i});

					Record r = client.get(get).get();
					Assert.assertEquals("" + inputList[0], expectList[1], formatter.format((Date) r.getObject(1)));
				}

			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * String(long) -> Timestamp.
	 */
	@Test
	public void test008() throws Exception {
		if (properties == null) {
			return;
		}

		String pattern = "yyyy-MM-dd HH:mm:ss.SSS";
		DateFormat formatter = new SimpleDateFormat(pattern);
		String[] inputList = new String[]{
				"0",
				"-100",
				"1010101010",
				"20201213100000"
		};
		String[] expectList = new String[inputList.length];
		for (int i = 0; i < inputList.length; ++i) {
			expectList[i] = formatter.format(new Date(Long.parseLong(inputList[i])));
		}
		HoloConfig config = buildConfig();
		config.setInputStringAsEpochMsForDatetimeColumn(true);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_type_004\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int ,ts timestamp, tstz timestamptz,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});
			try {
				TableSchema schema = client.getTableSchema(tableName);

				for (int i = 0; i < inputList.length; ++i) {
					Put put2 = new Put(schema);
					put2.setObject(0, i);
					put2.setObject(1, inputList[i]);
					put2.setObject(2, inputList[i]);
					client.put(put2);
				}
				client.flush();

				for (int i = 0; i < inputList.length; ++i) {
					Get get = new Get(schema, new Object[]{i});
					get.addSelectColumns(new int[]{0, 1, 2});

					Record r = client.get(get).get();
					Assert.assertEquals("" + inputList[i], expectList[i], formatter.format(new java.util.Date(((Timestamp) r.getObject(1)).getTime())));
					Assert.assertEquals("" + inputList[i], expectList[i], formatter.format(new java.util.Date(((Timestamp) r.getObject(2)).getTime())));
				}

			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * String(long) -> Timestamp 即使这样，不能转Long的也应该正常工作.
	 */
	@Test
	public void test009() throws Exception {
		if (properties == null) {
			return;
		}

		String pattern = "yyyy-MM-dd HH:mm:ss.SSS";
		DateFormat formatter = new SimpleDateFormat(pattern);
		String[] inputList = new String[]{
				"2021-12-12 05:12:15.456",
				"2021-12-12 05:12:15.456+07"
		};
		String[] expectList = new String[]{
				"2021-12-12 05:12:15.456",
				"2021-12-12 06:12:15.456"
		};
		HoloConfig config = buildConfig();
		config.setInputStringAsEpochMsForDatetimeColumn(true);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_type_009\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int ,ts timestamp, tstz timestamptz,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});
			try {
				TableSchema schema = client.getTableSchema(tableName);

				for (int i = 0; i < inputList.length; ++i) {
					Put put2 = new Put(schema);
					put2.setObject(0, i);
					put2.setObject(1, inputList[i]);
					put2.setObject(2, inputList[i]);
					client.put(put2);
				}
				client.flush();

				for (int i = 0; i < inputList.length; ++i) {
					Get get = new Get(schema, new Object[]{i});
					get.addSelectColumns(new int[]{0, 1, 2});

					Record r = client.get(get).get();
					Assert.assertEquals("" + inputList[i], expectList[i], formatter.format(new java.util.Date(((Timestamp) r.getObject(1)).getTime())));
					Assert.assertEquals("" + inputList[i], expectList[i], formatter.format(new java.util.Date(((Timestamp) r.getObject(2)).getTime())));
				}

			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}
}
