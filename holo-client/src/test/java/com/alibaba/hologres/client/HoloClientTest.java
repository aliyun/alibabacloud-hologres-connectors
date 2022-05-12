package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.exception.HoloClientWithDetailsException;
import com.alibaba.hologres.client.impl.ExecutionPool;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.RecordScanner;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.hologres.client.utils.Metrics;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HoloClient Tester.
 *
 * @version 1.0
 * @since <pre>12月 2, 2020</pre>
 */
public class HoloClientTest extends HoloClientTestBase {

	/**
	 *
	 */
	@Test
	public void testSts001() throws Exception {
		if (properties == null) {
			return;
		}
		String url = properties.getProperty("url") + "?conf:stsCode=123";
		Assert.assertThrows(SQLException.class, () -> {
			try (Connection conn = DriverManager.getConnection(url, properties)) {

			} catch (SQLException s) {
				Assert.assertEquals("FATAL: unrecognized configuration parameter \"stsCode\"", s.getMessage());
				throw s;
			}
		});
	}

	/**
	 *
	 */
	@Test
	public void testSts002() throws Exception {
		if (properties == null) {
			return;
		}
		String url = properties.getProperty("url") + "?stsCode=123";

		try (Connection conn = DriverManager.getConnection(url, properties)) {
			Assert.assertEquals("a", "a");
		}

	}

	/**
	 * INSERT_REPLACE.
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut001() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_put_001";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text,address text,primary key(id))";

			execute(conn, new String[]{dropSql, createSql});

			TableSchema schema = client.getTableSchema(tableName);

			Put put = new Put(schema);
			put.setObject(0, 0);
			put.setObject(1, "name0");
			put.setObject(2, "address");
			client.put(put);

			put = new Put(schema);
			put.setObject(0, 1);
			put.setObject(1, "name1");
			client.put(put);

			put = new Put(schema);
			put.setObject(0, 0);
			put.setObject(1, "name3");
			client.put(put);

			client.flush();

			Record r = client.get(new Get(schema, new Object[]{0})).get();
			Assert.assertEquals("name3", r.getObject(1));
			Assert.assertNull(r.getObject(2));

			r = client.get(new Get(schema, new Object[]{1})).get();
			Assert.assertEquals("name1", r.getObject(1));
			Assert.assertNull(r.getObject(2));

			execute(conn, new String[]{dropSql});
		}
	}

	/**
	 * INSERT_IGNORE.
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut002() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_IGNORE);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_put_002";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text,primary key(id))";

			execute(conn, new String[]{dropSql, createSql});

			TableSchema schema = client.getTableSchema(tableName);

			Put put = new Put(schema);
			put.setObject(0, 0);
			put.setObject(1, "name0");
			client.put(put);

			put = new Put(schema);
			put.setObject(0, 1);
			put.setObject(1, "name1");
			client.put(put);

			put = new Put(schema);
			put.setObject(0, 0);
			put.setObject(1, "name3");
			client.put(put);

			put = new Put(schema);
			put.setObject(0, 2);
			client.put(put);

			client.flush();

			Record r = client.get(new Get(schema, new Object[]{0})).get();
			Assert.assertEquals("name0", r.getObject(1));

			r = client.get(new Get(schema, new Object[]{1})).get();
			Assert.assertEquals("name1", r.getObject(1));

			r = client.get(new Get(schema, new Object[]{2})).get();
			Assert.assertNull(r.getObject(1));

			execute(conn, new String[]{dropSql});
		}
	}

	/**
	 * INSERT_UPDATE.
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut003() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_put_003";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text,address text,primary key(id))";

			execute(conn, new String[]{dropSql, createSql});

			TableSchema schema = client.getTableSchema(tableName);

			Put put = new Put(schema);
			put.setObject(0, 0);
			put.setObject(1, "name0");
			client.put(put);

			put = new Put(schema);
			put.setObject(0, 1);
			put.setObject(1, "name1");
			put.setObject(2, "address2");
			client.put(put);

			put = new Put(schema);
			put.setObject(0, 1);
			put.setObject(2, "address3");
			client.put(put);

			client.flush();

			Record r = client.get(new Get(schema, new Object[]{0})).get();
			Assert.assertEquals("name0", r.getObject(1));
			Assert.assertNull(r.getObject(2));

			r = client.get(new Get(schema, new Object[]{1})).get();
			Assert.assertEquals("name1", r.getObject(1));
			Assert.assertEquals("address3", r.getObject(2));

			execute(conn, new String[]{dropSql});
		}
	}

	/**
	 * CUSTOM_SCHEMA，INSERT_OR_UPDATE.
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut004() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.holo_client_put_004";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text,address text,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			TableSchema schema = client.getTableSchema(tableName);

			Put put = new Put(schema);
			put.setObject(0, 0);
			put.setObject(1, "name0");
			client.put(put);

			put = new Put(schema);
			put.setObject(0, 1);
			put.setObject(1, "name1");
			put.setObject(2, "address2");
			client.put(put);

			put = new Put(schema);
			put.setObject(0, 1);
			put.setObject(2, "address3");
			client.put(put);

			client.flush();

			Record r = client.get(new Get(schema, new Object[]{0})).get();
			Assert.assertEquals("name0", r.getObject(1));
			Assert.assertNull(r.getObject(2));

			r = client.get(new Get(schema, new Object[]{1})).get();
			Assert.assertEquals("name1", r.getObject(1));
			Assert.assertEquals("address3", r.getObject(2));

			execute(conn, new String[]{dropSql});
		}
	}

	/**
	 * CUSTOM_SCHEMA,success.
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut005() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_IGNORE);
		config.setDynamicPartition(true);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.holo_client_put_005";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text,address text,primary key(id,name)) partition by list(name)";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			TableSchema schema = client.getTableSchema(tableName);

			Put put = new Put(schema);
			put.setObject(0, 0);
			put.setObject(1, "name0");
			put.setObject(2, "address1");
			client.put(put);

			client.flush();

			Record r = client.get(new Get(schema, new Object[]{0, "name0"})).get();
			Assert.assertEquals("address1", r.getObject(2));

			execute(conn, new String[]{dropSql});
		}
	}

	/**
	 * CUSTOM_SCHEMA,fail.
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut006() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_IGNORE);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.holo_client_put_006";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text,address text,primary key(id,name)) partition by list(name)";
			String createSql1 = "create table " + tableName + "(id int not null,name text,address text,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			final TableSchema schema = client.getTableSchema(tableName);

			Assert.assertThrows(HoloClientWithDetailsException.class, () -> {
				try {
					Put put = new Put(schema);
					put.setObject(0, 0);
					put.setObject(1, "name0");
					put.setObject(2, "address1");
					client.put(put);
				} catch (HoloClientWithDetailsException e) {
					Assert.assertEquals(1, e.size());
					throw e;
				}
			});
			Assert.assertThrows(HoloClientWithDetailsException.class, () -> {
				try {
					Put put = new Put(schema);
					put.setObject(0, 1);
					put.setObject(1, "name0");
					put.setObject(2, "address1");
					client.put(put);
					client.put(put);
				} catch (HoloClientWithDetailsException e) {
					Assert.assertEquals(1, e.size());
					throw e;
				}
			});
			execute(conn, new String[]{dropSql, createSql1});
			TableSchema schema1 = client.getTableSchema(tableName, true);

			Put put = new Put(schema1);
			put.setObject(0, 0);
			put.setObject(1, "name0");
			put.setObject(2, "address1");
			client.put(put);
			client.flush();
			Record r = client.get(new Get(schema1, new Object[]{0})).get();
			Assert.assertEquals("address1", r.getObject(2));

			execute(conn, new String[]{dropSql});
		}
	}

	/**
	 * CUSTOM_SCHEMA,tableName，columName.
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut007() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_IGNORE);
		config.setDynamicPartition(true);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_007\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,\"nAme\" text,address text,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			TableSchema schema = client.getTableSchema(tableName);

			Put put = new Put(schema);
			put.setObject(0, 0);
			put.setObject(1, "name0");
			put.setObject(2, "address1");
			client.put(put);
			put = new Put(schema);
			put.setObject(0, 1);
			put.setObject(1, "name1");
			put.setObject(2, "address2");
			client.put(put);

			client.flush();

			Record r = client.get(new Get(schema, new Object[]{0})).get();
			Assert.assertEquals("name0", r.getObject(1));
			Assert.assertEquals("address1", r.getObject(2));

			r = client.get(new Get(schema, new Object[]{1})).get();
			Assert.assertEquals("name1", r.getObject(1));
			Assert.assertEquals("address2", r.getObject(2));
			execute(conn, new String[]{dropSql});
		}
	}

	/**
	 * CUSTOM_SCHEMA,tableName，columName,syncCommit.
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut008() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_IGNORE);
		config.setDynamicPartition(true);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			client.setAsyncCommit(false);
			String tableName = "test_schema.\"holO_client_put_007\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,\"nAme\" text,\"address\" text not null,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			TableSchema schema = client.getTableSchema(tableName);

			final Put put = new Put(schema);
			put.setObject(0, 0);
			put.setObject(1, "name0");
			// 此处不应该报错
			Put put2 = new Put(schema);
			put2.setObject(0, 1);
			put2.setObject(1, "name1");
			put2.setObject(2, "address2");
			client.put(put2);

			Record r = client.get(new Get(schema, new Object[]{0})).get();
			Assert.assertNull(r);

			r = client.get(new Get(schema, new Object[]{1})).get();
			Assert.assertEquals("name1", r.getObject(1));
			Assert.assertEquals("address2", r.getObject(2));
			execute(conn, new String[]{dropSql});
		}
	}

	/**
	 * issue:32157525.
	 * 已设置主键的情况下，调用HoloClient.get报Get primary key cannot be null
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut009() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_IGNORE);
		config.setDynamicPartition(true);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			client.setAsyncCommit(false);
			String tableName = "test_schema.\"holO_client_put_009\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,\"nAme\" text,\"address\" text not null,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);

				Put put2 = new Put(schema);
				put2.setObject(0, 1);
				put2.setObject(1, "name1");
				put2.setObject(2, "address2");
				client.put(put2);
				Get get = new Get(schema, new Object[]{1});
				get.addSelectColumns(new int[]{0, 1, 2});

				Record r = client.get(get).get();
				Assert.assertEquals("name1", r.getObject(1));
				Assert.assertEquals("address2", r.getObject(2));

				get = new Get(schema, new Object[]{1});
				get.addSelectColumn(0);
				get.addSelectColumn(1);
				get.addSelectColumn(2);

				r = client.get(get).get();
				Assert.assertEquals("name1", r.getObject(1));
				Assert.assertEquals("address2", r.getObject(2));
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * column name with char '"'
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut010() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_IGNORE);
		config.setDynamicPartition(true);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			client.setAsyncCommit(false);
			String tableName = "test_schema.\"holO_client_put_009\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,\"a\"\"b\" text,\"address\" text not null,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				Assert.assertEquals("a\"b", schema.getColumnSchema()[1].getName());

				Put put2 = new Put(schema);
				put2.setObject(0, 1);
				put2.setObject(1, "name1");
				put2.setObject(2, "address2");
				client.put(put2);
				Get get = new Get(schema, new Object[]{1});
				get.addSelectColumns(new int[]{0, 1, 2});

				Record r = client.get(get).get();
				Assert.assertEquals("name1", r.getObject(1));
				Assert.assertEquals("address2", r.getObject(2));

				get = new Get(schema, new Object[]{1});
				get.addSelectColumn(0);
				get.addSelectColumn(1);
				get.addSelectColumn(2);

				r = client.get(get).get();
				Assert.assertEquals("name1", r.getObject(1));
				Assert.assertEquals("address2", r.getObject(2));
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * test WriteFailStrategy.TRY_ONE_BY_ONE.
	 */
	@Test
	public void testPutPut011() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_IGNORE);
		config.setDynamicPartition(true);
		config.setEnableDefaultForNotNullColumn(false);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_011\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null default 0,\"a\"\"b\" text,\"address\" text not null,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql, "comment on column " + tableName + ".id is 'hello'"});

			try {
				TableSchema schema = client.getTableSchema(tableName);

				Assert.assertEquals("a\"b", schema.getColumnSchema()[1].getName());

				Put put = new Put(schema);
				put.setObject(0, 1);
				put.setObject(1, "name1");
				put.setObject(2, "address1");
				client.put(put);
				put = new Put(schema);
				put.setObject(0, 2);
				put.setObject(1, "name2");
				put.setObject(2, null);
				client.put(put);
				put = new Put(schema);
				put.setObject(0, 3);
				put.setObject(1, "name3");
				put.setObject(2, "address3");
				client.put(put);

				Assert.assertThrows(HoloClientWithDetailsException.class, () -> {
					try {
						client.flush();
					} catch (HoloClientWithDetailsException e) {
						Assert.assertEquals(1, e.size());
						throw e;
					}
				});

				Get get = new Get(schema, new Object[]{1});
				get.addSelectColumns(new int[]{0, 1, 2});
				Record r = client.get(get).get();
				Assert.assertEquals("name1", r.getObject(1));
				Assert.assertEquals("address1", r.getObject(2));
				get = new Get(schema, new Object[]{2});
				get.addSelectColumn(0);
				get.addSelectColumn(1);
				get.addSelectColumn(2);
				r = client.get(get).get();
				Assert.assertNull(r);
				get = new Get(schema, new Object[]{3});
				get.addSelectColumns(new int[]{0, 1, 2});
				r = client.get(get).get();
				Assert.assertEquals("name3", r.getObject(1));
				Assert.assertEquals("address3", r.getObject(2));
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * Get/Put by columName
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut012() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_IGNORE);
		config.setDynamicPartition(true);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			client.setAsyncCommit(false);
			String tableName = "test_schema.\"holO_client_put_009\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,\"a\"\"b\" text,\"address\" text not null,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				Assert.assertEquals("a\"b", schema.getColumnSchema()[1].getName());

				Put put2 = new Put(schema);
				put2.setObject("id", 1);
				put2.setObject("a\"b", "name1");
				put2.setObject("address", "address2");
				client.put(put2);
				Get get = new Get(schema, new Object[]{1});
				get.addSelectColumn("id");
				get.addSelectColumn("a\"b");
				get.addSelectColumn("address");
				Record r = client.get(get).get();
				Assert.assertEquals("name1", r.getObject("a\"b"));
				Assert.assertEquals("address2", r.getObject("address"));

				get = new Get(schema, new Object[]{1});
				get.addSelectColumn("id");
				get.addSelectColumn("address");

				r = client.get(get).get();
				Assert.assertEquals(1, r.getObject("id"));
				Assert.assertEquals("address2", r.getObject("address"));
				Assert.assertNull(r.getObject("a\"b"));
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * Put no pk
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut013() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_IGNORE);
		config.setDynamicPartition(true);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			client.setAsyncCommit(false);
			String tableName = "test_schema.\"holO_client_put_013\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,\"a\"\"b\" text,\"address\" text not null)";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				Assert.assertEquals("a\"b", schema.getColumnSchema()[1].getName());

				Put put2 = new Put(schema);
				put2.setObject("id", 1);
				put2.setObject("a\"b", "name1");
				put2.setObject("address", "address2");
				client.put(put2);
				put2 = new Put(schema);
				put2.setObject("id", 1);
				put2.setObject("a\"b", "name1");
				put2.setObject("address", "address2");
				client.put(put2);
				put2 = new Put(schema);
				put2.setObject("id", 1);
				put2.setObject("a\"b", "name1");
				put2.setObject("address", "address2");
				client.put(put2);
				put2 = new Put(schema);
				put2.setObject("id", 1);
				put2.setObject("a\"b", "name1");
				put2.setObject("address", "address2");
				client.put(put2);

				client.flush();

				int count = 0;
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
						if (rs.next()) {
							count = rs.getInt(1);
						}
					}
				}
				Assert.assertEquals(4, count);
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * connection max idle time
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut014() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_IGNORE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			client.setAsyncCommit(false);
			String tableName = "test_schema.\"holO_client_put_014\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,\"a\"\"b\" text,\"address\" text not null)";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				Put put2 = new Put(schema);
				put2.setObject("id", 1);
				put2.setObject("a\"b", "name1");
				put2.setObject("address", "address2");
				client.put(put2);
				put2 = new Put(schema);
				put2.setObject("id", 1);
				put2.setObject("a\"b", "name1");
				put2.setObject("address", "address2");
				client.put(put2);
				put2 = new Put(schema);
				put2.setObject("id", 1);
				put2.setObject("a\"b", "name1");
				put2.setObject("address", "address2");
				client.put(put2);
				put2 = new Put(schema);
				put2.setObject("id", 1);
				put2.setObject("a\"b", "name1");
				put2.setObject("address", "address2");
				client.put(put2);
				client.flush();

				Thread.sleep(10000L);
				put2 = new Put(schema);
				put2.setObject("id", 1);
				put2.setObject("a\"b", "name1");
				put2.setObject("address", "address2");
				client.put(put2);
				client.flush();
				Thread.sleep(5000L);
				put2 = new Put(schema);
				put2.setObject("id", 1);
				put2.setObject("a\"b", "name1");
				put2.setObject("address", "address2");
				client.put(put2);
				client.flush();
				Thread.sleep(5000L);
				put2 = new Put(schema);
				put2.setObject("id", 1);
				put2.setObject("a\"b", "name1");
				put2.setObject("address", "address2");
				client.put(put2);
				client.flush();
				int count = 0;
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
						if (rs.next()) {
							count = rs.getInt(1);
						}
					}
				}
				Assert.assertEquals(7, count);
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * Put then delete
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut015() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_015\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,\"a\"\"b\" text,\"address\" text not null,primary key(id,\"a\"\"b\"))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				Put put2 = new Put(schema);
				put2.setObject("id", 1);
				put2.setObject("a\"b", "name1");
				put2.setObject("address", "address2");
				client.put(put2);
				put2 = new Put(schema);
				put2.setObject("id", 2);
				put2.setObject("a\"b", "name2");
				put2.setObject("address", "address2");
				client.put(put2);
				put2 = new Put(schema);
				put2.setObject("id", 3);
				put2.setObject("a\"b", "name3");
				put2.setObject("address", "address2");
				client.put(put2);
				put2 = new Put(schema);
				put2.setObject("id", 4);
				put2.setObject("a\"b", "name1");
				put2.setObject("address", "address2");
				client.put(put2);
				//client.flush();
				put2 = new Put(schema);
				put2.getRecord().setType(Put.MutationType.DELETE);
				put2.setObject("id", 1);
				put2.setObject("a\"b", "name1");
				put2.setObject("address", "ccc");
				client.put(put2);

				client.flush();
				int count = 0;
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
						if (rs.next()) {
							count = rs.getInt(1);
						}
					}
				}
				Assert.assertEquals(3, count);
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * CUSTOM_SCHEMA，INSERT_OR_UPDATE.
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut017() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setAppName("testPutPut017");
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.holo_client_put_017";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text,address text,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			TableSchema schema = client.getTableSchema(tableName);

			Put put = new Put(schema);
			put.setObject(0, 0);
			put.setObject(1, "name0");
			put.setObject(2, "address0");
			client.put(put);

			put = new Put(schema);
			put.setObject(0, 1);
			put.setObject(1, "name1");
			put.setObject(2, "address1");
			client.put(put);

			client.flush();

			put = new Put(schema);
			put.getRecord().setType(Put.MutationType.DELETE);
			put.setObject(0, 0);
			client.put(put);

			put = new Put(schema);
			put.setObject(0, 0);
			put.setObject(1, "name0_new");
			put.setObject(2, "address0_new");
			client.put(put);

			put = new Put(schema);
			put.getRecord().setType(Put.MutationType.DELETE);
			put.setObject(0, 1);
			client.put(put);

			put = new Put(schema);
			put.setObject(0, 2);
			put.setObject(1, "name1");
			put.setObject(2, "address1");
			client.put(put);

			client.flush();

			Record r = client.get(new Get(schema, new Object[]{0})).get();
			Assert.assertEquals("name0_new", r.getObject(1));
			Assert.assertEquals("address0_new", r.getObject(2));

			r = client.get(new Get(schema, new Object[]{2})).get();
			Assert.assertEquals("name1", r.getObject(1));
			Assert.assertEquals("address1", r.getObject(2));

			r = client.get(new Get(schema, new Object[]{1})).get();
			Assert.assertNull(r);
			execute(conn, new String[]{dropSql});
		}
	}

	/**
	 * Put 数据1、数据2（写入失败），等待内部flush，Put 数据3（抛异常），主动flush，应该能查到数据1和数据3.
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut018() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteMaxIntervalMs(3000L);
		config.setEnableDefaultForNotNullColumn(false);
		config.setAppName("testPutPut018");
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.holo_client_put_018";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text not null,address text,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);

				Put put = new Put(schema);
				put.setObject(0, 0);
				put.setObject(1, "name0");
				put.setObject(2, "address0");
				client.put(put);

				put = new Put(schema);
				put.setObject(0, 1);
				put.setObject(1, null);
				put.setObject(2, "address1");
				client.put(put);

				//等待超过WriteMaxIntervalMs时间后自动提交
				Thread.sleep(5000L);

				Assert.assertThrows(HoloClientWithDetailsException.class, () -> {
					Put put2 = new Put(schema);
					put2.setObject(0, 2);
					put2.setObject(1, "name2");
					put2.setObject(2, "address2");
					client.put(put2);
				});

				client.flush();

				Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
				Assert.assertEquals("name0", r.getObject("name"));
				Assert.assertEquals("address0", r.getObject("address"));

				r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
				Assert.assertNull(r);

				r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).build()).get();
				Assert.assertEquals("name2", r.getObject("name"));
				Assert.assertEquals("address2", r.getObject("address"));

			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * Get时支持类型自动转换，例如String->Int
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut019() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteMaxIntervalMs(3000L);
		config.setAppName("testPutPut019");
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.holo_client_put_019";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text not null,address text,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);

				Put put = new Put(schema);
				put.setObject(0, 0);
				put.setObject(1, "name0");
				put.setObject(2, "address0");
				client.put(put);

				client.flush();

				Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", "0").build()).get();
				Assert.assertEquals("name0", r.getObject("name"));
				Assert.assertEquals("address0", r.getObject("address"));

				r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0L).build()).get();
				Assert.assertEquals("name0", r.getObject("name"));
				Assert.assertEquals("address0", r.getObject("address"));

				r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
				Assert.assertEquals("name0", r.getObject("name"));
				Assert.assertEquals("address0", r.getObject("address"));
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * Get时支持类型自动转换，例如String->Int8
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut020() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteMaxIntervalMs(3000L);
		config.setAppName("testPutPut020");
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.holo_client_put_020";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int8 not null,name text not null,address text,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);

				Put put = new Put(schema);
				put.setObject(0, "0");
				put.setObject(1, "name0");
				put.setObject(2, "address0");
				client.put(put);

				client.flush();

				Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", "0").build()).get();
				Assert.assertEquals("name0", r.getObject("name"));
				Assert.assertEquals("address0", r.getObject("address"));

				r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0L).build()).get();
				Assert.assertEquals("name0", r.getObject("name"));
				Assert.assertEquals("address0", r.getObject("address"));

				r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
				Assert.assertEquals("name0", r.getObject("name"));
				Assert.assertEquals("address0", r.getObject("address"));
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * 加列后正常工作
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut021() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setMetaCacheTTL(5000L);
		config.setAppName("testPutPut021");
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.holo_client_put_021";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int8 not null,name text not null,address text,primary key(id))";

			String addColumnSql = "alter table " + tableName + " add column new_c text";
			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);

				Put put = new Put(schema);
				put.setObject(0, "0");
				put.setObject(1, "name0");
				put.setObject(2, "address0");
				client.put(put);

				Thread.sleep(5000L);
				{
					TableSchema temp = client.getTableSchema(tableName);
					Assert.assertTrue(temp.equals(schema));
				}

				execute(conn, new String[]{addColumnSql});
				put = new Put(schema);
				put.setObject(0, "1");
				put.setObject(1, "name1");
				put.setObject(2, "address1");
				client.put(put);

				Thread.sleep(5000L);
				TableSchema oldSchema = schema;
				schema = client.getTableSchema(tableName);
				put = new Put(schema);
				put.setObject(0, "2");
				put.setObject(1, "name2");
				put.setObject(2, "address2");
				put.setObject(3, "new");
				client.put(put);

				client.flush();

				Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", "0").build()).get();
				Assert.assertEquals("name0", r.getObject("name"));
				Assert.assertEquals("address0", r.getObject("address"));
				Assert.assertNull(r.getObject("new_c"));

				r = client.get(Get.newBuilder(oldSchema).setPrimaryKey("id", "0").build()).get();
				Assert.assertEquals("name0", r.getObject("name"));
				Assert.assertEquals("address0", r.getObject("address"));

				r = client.get(Get.newBuilder(schema).setPrimaryKey("id", "1").build()).get();
				Assert.assertEquals("name1", r.getObject("name"));
				Assert.assertEquals("address1", r.getObject("address"));
				Assert.assertNull(r.getObject("new_c"));

				r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).build()).get();
				Assert.assertEquals("name2", r.getObject("name"));
				Assert.assertEquals("address2", r.getObject("address"));
				Assert.assertEquals("new", r.getObject("new_c"));
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}


	/**
	 * preparedStatement.setObject throw exception
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut022() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		config.setAppName("testPutPut022");
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_022\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName + ";drop table if exists holo_client_put_022_1";
			String createSql = "create table " + tableName + "(id int not null,ts timestamp,primary key(id));" +
					"create table holo_client_put_022_1 (id int not null,ts timestamp,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);
				TableSchema schema1 = client.getTableSchema("holo_client_put_022_1", true);

				try {
					Put put2 = new Put(schema);
					put2.setObject("id", 1);
					put2.setObject("ts", "2017-05-01 12:10:55");
					client.put(put2);
					put2 = new Put(schema1);
					put2.setObject("id", 1);
					put2.setObject("ts", "20170501 12:10:55");
					client.put(put2);
					client.flush();
				} catch (HoloClientWithDetailsException e) {
					System.out.println("exception:" + e.size());
					e.printStackTrace();
				}
				int count = 0;
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
						if (rs.next()) {
							count = rs.getInt(1);
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
	 * array
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut023() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		config.setAppName("testPutPut023");
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_023\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,a text[],b int[],primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				{
					Put put2 = new Put(schema);
					put2.setObject("id", 0);

					List<Object> textArray = new ArrayList<>();
					textArray.add("a");
					textArray.add("b");
					put2.setObject("a", textArray);
					List<Object> intArray = new ArrayList<>();
					intArray.add(1);
					intArray.add(2);
					put2.setObject("b", intArray);
					client.put(put2);
				}
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 1);

					List<Object> textArray = new ArrayList<>();
					put2.setObject("a", new Object[]{"c", "d"});
					put2.setObject("b", new Object[]{"3", "4"});
					client.put(put2);
				}
				client.flush();

				Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", "0").build()).get();
				Assert.assertEquals("{a,b}", r.getObject("a").toString());
				Assert.assertEquals("{1,2}", r.getObject("b").toString());

				r = client.get(Get.newBuilder(schema).setPrimaryKey("id", "1").build()).get();
				Assert.assertEquals("{c,d}", r.getObject("a").toString());
				Assert.assertEquals("{3,4}", r.getObject("b").toString());
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * mult primary key ,ingore
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut024() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		config.setAppName("testPutPut024");
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_024\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text not null,address text ,primary key(id,name))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				{
					Put put2 = new Put(schema);
					put2.setObject("id", 0);
					put2.setObject("name", "a");
					put2.setObject("address", "b");
					client.put(put2);
				}
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 0);
					put2.setObject("name", "b");
					put2.setObject("address", "c");
					client.put(put2);
				}
				client.flush();

				Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", "0").setPrimaryKey("name", "a").build()).get();
				Assert.assertEquals("b", r.getObject("address"));

				r = client.get(Get.newBuilder(schema).setPrimaryKey("id", "0").setPrimaryKey("name", "b").build()).get();
				Assert.assertEquals("c", r.getObject("address"));
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * partition int
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut0245() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		config.setAppName("testPutPut0245");
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_025\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,c int not null,d text,primary key(id,c)) partition by list(c)";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				{
					Put put2 = new Put(schema);
					put2.setObject("id", 0);
					put2.setObject("c", 12);
					put2.setObject("d", "aaa");
					client.put(put2);
				}
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 0);
					put2.setObject("c", 1);
					put2.setObject("d", "123");
					client.put(put2);
				}
				client.flush();

				Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", "0").setPrimaryKey("c", 12).build()).get();
				Assert.assertEquals("aaa", r.getObject("d").toString());

				r = client.get(Get.newBuilder(schema).setPrimaryKey("id", "0").setPrimaryKey("c", 1).build()).get();
				Assert.assertEquals("123", r.getObject("d").toString());
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * partition int
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut026() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		config.setAppName("testPutPut026");
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_026\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,c int not null,d text,primary key(id,d)) partition by list(d)";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				{
					Put put2 = new Put(schema);
					put2.setObject("id", 0);
					put2.setObject("c", 12);
					put2.setObject("d", "aaa");
					client.put(put2);
				}
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 0);
					put2.setObject("c", 1);
					put2.setObject("d", "123");
					client.put(put2);
				}
				client.flush();

				Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", "0").setPrimaryKey("d", "aaa").build()).get();
				Assert.assertEquals(12, r.getObject("c"));

				r = client.get(Get.newBuilder(schema).setPrimaryKey("id", "0").setPrimaryKey("d", "123").build()).get();
				Assert.assertEquals(1, r.getObject("c"));
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * 局部更新
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut027() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		config.setAppName("testPutPut027");
		//config.setUseLegacyPutHandler(true);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_027\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,c int not null,d text,e text, primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				{
					Put put2 = new Put(schema);
					put2.setObject("id", 0);
					put2.setObject("c", 12);
					put2.setObject("d", "aaa");
					put2.setObject("e", "123");
					client.put(put2);
				}
				client.flush();
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 0);
					put2.setObject("c", 1);
					put2.setObject("d", "456", true);
					client.put(put2);
				}
				client.flush();

				{
					Put put2 = new Put(schema);
					put2.setObject("id", 1);
					put2.setObject("c", 12);
					put2.setObject("d", "aaa");
					put2.setObject("e", "123");
					client.put(put2);
				}
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 1);
					put2.setObject("c", 1);
					put2.setObject("d", "456", true);
					client.put(put2);
				}
				client.flush();

				{
					Put put2 = new Put(schema);
					put2.setObject("id", 2);
					put2.setObject("c", 12);
					put2.setObject("d", "aaa");
					put2.setObject("e", "123");
					client.put(put2);
				}
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 2);
					put2.setObject("c", 1);
					put2.setObject("d", "456");
					client.put(put2);
				}
				client.flush();
				Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", "0").build()).get();
				Assert.assertEquals(1, r.getObject("c"));
				Assert.assertEquals("aaa", r.getObject("d"));
				Assert.assertEquals("123", r.getObject("e"));

				r = client.get(Get.newBuilder(schema).setPrimaryKey("id", "1").build()).get();
				Assert.assertEquals(1, r.getObject("c"));
				Assert.assertEquals("aaa", r.getObject("d"));
				Assert.assertEquals("123", r.getObject("e"));

				r = client.get(Get.newBuilder(schema).setPrimaryKey("id", "2").build()).get();
				Assert.assertEquals(1, r.getObject("c"));
				Assert.assertEquals("456", r.getObject("d"));
				Assert.assertEquals("123", r.getObject("e"));

			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * ExecutionPool
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut028() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		config.setAppName("testPutPut028");
		try (Connection conn = buildConnection(); ExecutionPool pool = ExecutionPool.buildOrGet("hello", config, false)) {
			String tableName = "test_schema.\"holO_client_put_027\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,c int not null,d text,e text, primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				final AtomicLong l = new AtomicLong(0L);
				Runnable runnable = () -> {
					try {
						HoloClient client = new HoloClient(config);
						client.setPool(pool);
						TableSchema schema = client.getTableSchema(tableName, true);
						for (int i = 0; i < 100; ++i) {
							{
								Put put2 = new Put(schema);
								put2.setObject("id", l.getAndIncrement());
								put2.setObject("c", 12);
								put2.setObject("d", "aaa");
								put2.setObject("e", "123");
								client.put(put2);
							}
						}
						client.flush();
					} catch (HoloClientException e) {
						e.printStackTrace();
						Assert.assertEquals(e.getMessage(), 1, 2);
					}
				};
				ExecutorService es = new ThreadPoolExecutor(20, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(20), r -> {
					Thread t = new Thread(r);
					return t;
				}, new ThreadPoolExecutor.AbortPolicy());
				for (int i = 0; i < 5; ++i) {
					es.execute(runnable);
				}
				es.shutdown();
				while (!es.awaitTermination(5000L, TimeUnit.MILLISECONDS)) {

				}
				int count = 0;
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
						if (rs.next()) {
							Assert.assertEquals(100 * 5, rs.getInt(1));
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
	 * scan
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut029() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		config.setAppName("testPutPut029");
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_029\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text not null,address text ,primary key(id,name))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < 10000; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("name", i % 10);
					put2.setObject("address", "b");
					client.put(put2);
				}
				client.flush();

				//scan all
				{
					Scan scan = Scan.newBuilder(schema).build();
					int size = 0;
					try (RecordScanner rs = client.scan(scan)) {
						while (rs.next()) {
							Record record = rs.getRecord();
							++size;
							Assert.assertEquals("b", record.getObject("address"));
							Assert.assertNotNull(record.getObject("name"));
							Assert.assertNotNull(record.getObject("id"));
						}
					}
					Assert.assertEquals(10000, size);
				}
				//scan range
				{
					Scan scan = Scan.newBuilder(schema).addRangeFilter("id", null, 10).build();
					int size = 0;
					try (RecordScanner rs = client.scan(scan)) {
						while (rs.next()) {
							Record record = rs.getRecord();
							++size;
							Assert.assertEquals("b", record.getObject("address"));
							Assert.assertNotNull(record.getObject("name"));
							Assert.assertTrue((Integer) record.getObject("id") < 10);
						}
					}
					Assert.assertEquals(10, size);
				}
				//scan range
				{
					Scan scan = Scan.newBuilder(schema).addEqualFilter("id", 102).addRangeFilter("name", "3", null).build();
					int size = 0;
					try (RecordScanner rs = client.scan(scan)) {
						while (rs.next()) {
							Record record = rs.getRecord();
							System.out.println(record);
							++size;
						}
					}
					Assert.assertEquals(0, size);
				}
				//scan range
				{
					Scan scan = Scan.newBuilder(schema).addEqualFilter("id", 102).addRangeFilter("name", "00", null).build();
					int size = 0;
					try (RecordScanner rs = client.scan(scan)) {
						while (rs.next()) {
							Record record = rs.getRecord();
							++size;
							Assert.assertEquals("b", record.getObject("address"));
							Assert.assertEquals("2", record.getObject("name"));
							Assert.assertEquals(102, record.getObject("id"));
						}
					}
					Assert.assertEquals(1, size);
				}
				//scan with select column
				{
					Scan scan = Scan.newBuilder(schema).addEqualFilter("id", 102).addRangeFilter("name", "00", null).withSelectedColumn("address").build();
					int size = 0;
					try (RecordScanner rs = client.scan(scan)) {
						while (rs.next()) {
							Record record = rs.getRecord();
							++size;
							Assert.assertEquals("b", record.getObject("address"));
						}
					}
					Assert.assertEquals(1, size);
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * 2min
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut030() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setDynamicPartition(true);
		config.setMetaCacheTTL(10000L);
		config.setConnectionMaxIdleMs(10000L);
		config.setWriteThreadSize(3);
		config.setAppName("testPutPut030");
		try (HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_030\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,a text[],b int[],primary key(id))";

			client.sql(conn -> {
				execute(conn, new String[]{createSchema, dropSql, createSql});
				return null;
			}).get();

			Metrics.startSlf4jReporter(30L, TimeUnit.SECONDS);
			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				int count = 1000000;
				for (int i = 0; i < count; ++i) {
					Put put2 = new Put(schema);
					schema = client.getTableSchema(tableName);
					put2.setObject("id", i);

					List<Object> textArray = new ArrayList<>();
					textArray.add("a");
					textArray.add("b");
					put2.setObject("a", textArray);
					List<Object> intArray = new ArrayList<>();
					intArray.add(1);
					intArray.add(2);
					put2.setObject("b", intArray);
					client.put(put2);
					if (i % 10000 == 0) {
						System.out.println("put:" + i);
					}
				}
				client.flush();

				Optional<Integer> result = (Optional<Integer>) client.sql(conn -> {
					Optional<Integer> ret = Optional.empty();
					try (Statement stat = conn.createStatement()) {
						try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
							rs.next();
							ret = Optional.of(rs.getInt(1));
						}
					}
					return ret;
				}).get();
				Assert.assertEquals(java.util.Optional.of(count), result);
			} finally {
				client.sql(conn -> {
					execute(conn, new String[]{dropSql});
					return null;
				}).get();
			}
		}
	}

	/**
	 * put records to same partition.
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut031() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setDynamicPartition(true);
		config.setAppName("testPutPut031");
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.holo_client_put_031";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text,address text,primary key(id,name)) partition by list(name)";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			TableSchema schema = client.getTableSchema(tableName);

			Put put = new Put(schema);
			put.setObject(0, 0);
			put.setObject(1, "2019-01-02");
			put.setObject(2, "address1");
			client.put(put);

			client.flush();

			Record r = client.get(new Get(schema, new Object[]{0, "2019-01-02"})).get();
			Assert.assertEquals("address1", r.getObject(2));

			//TableSchema schema1 = client.getTableSchema("test_schema.\"holo_client_put_005_3-2\"");
			//System.out.println(schema1);
			{
				Put put1 = new Put(schema);
				put1.setObject(0, 0);
				put1.setObject(1, "2019-01-02");
				put1.setObject(2, "abc");
				client.put(put1);
				client.flush();
			}
			r = client.get(new Get(schema, new Object[]{0, "2019-01-02"})).get();
			Assert.assertEquals("abc", r.getObject(2));

			execute(conn, new String[]{dropSql});
		}
	}

	/**
	 * enable MetaAutoRefreshFactor
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut032() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setDynamicPartition(true);
		config.setMetaCacheTTL(10000L);
		config.setAppName("testPutPut032");
		//config.setMetaAutoRefreshFactor(Integer.MAX_VALUE);
		try (HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.holo_client_put_032";
			String tableName2 = "test_schema.holo_client_put_032_2";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName + ";drop table if exists " + tableName2;
			String createSql = "create table " + tableName + "(id int not null,name text,address text,primary key(id,name)) partition by list(name);" +
					"create table " + tableName2 + "(id int not null,name text,address text,primary key(id,name)) partition by list(name)";

			client.sql(conn -> {
				execute(conn, new String[]{createSchema, dropSql, createSql});
				return null;
			}).get();
			try {
				TableSchema schema = client.getTableSchema(tableName);
				TableSchema schema2 = client.getTableSchema(tableName2);
				schema = client.getTableSchema(tableName);

				Thread.sleep(12000L);

				{
					long start = System.nanoTime();
					schema2 = client.getTableSchema(tableName2);
					long end = System.nanoTime();
					long costMs = (end - start) / 1000000;
					Assert.assertTrue("costMs=" + costMs, costMs > 10L);
				}
				{
					long start = System.nanoTime();
					schema = client.getTableSchema(tableName);
					long end = System.nanoTime();
					long costMs = (end - start) / 1000000;
					Assert.assertTrue("costMs=" + costMs, costMs < 1L);
				}
			} finally {
				client.sql(conn -> {
					execute(conn, new String[]{dropSql});
					return null;
				}).get();
			}
		}
	}

	/**
	 * disable MetaAutoRefreshFactor
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut033() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setDynamicPartition(true);
		config.setMetaCacheTTL(10000L);
		config.setMetaAutoRefreshFactor(Integer.MAX_VALUE);
		config.setAppName("testPutPut033");
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.holo_client_put_032";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text,address text,primary key(id,name)) partition by list(name)";

			execute(conn, new String[]{createSchema, dropSql, createSql});
			try {
				TableSchema schema = client.getTableSchema(tableName);
				schema = client.getTableSchema(tableName);

				Thread.sleep(12000L);

				long start = System.nanoTime();
				schema = client.getTableSchema(tableName);
				long end = System.nanoTime();
				long costMs = (end - start) / 1000000;
				Assert.assertTrue("costMs=" + costMs, costMs > 10L);
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * resize
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut034() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		config.setAppName("testPutPut034");
		config.setWriteThreadSize(10);
		try (Connection conn = buildConnection(); ExecutionPool pool = ExecutionPool.buildOrGet("hello", config, false)) {
			Metrics.startSlf4jReporter(60, TimeUnit.SECONDS);
			int tableCount = 20;
			String[] tableNames = new String[tableCount];
			String createSql = "";
			String dropSql = "";
			AtomicInteger[] count = new AtomicInteger[tableCount];
			for (int i = 0; i < tableCount; ++i) {
				tableNames[i] = "test_schema.\"holO_client_put_034_" + i + "\"";
				count[i] = new AtomicInteger(0);
				createSql += "create table " + tableNames[i] + "(id int not null,c int not null,d text,e text, primary key(id))" + ";";
				dropSql += "drop table if exists " + tableNames[i] + ";";
			}
			String createSchema = "create schema if not exists test_schema";
			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				final AtomicLong l = new AtomicLong(0L);
				Runnable runnable = () -> {
					try {
						long currentNano = System.nanoTime();
						long targetNano = currentNano + 180 * 1000 * 1000000L;
						Random rand = new Random();
						HoloClient client = new HoloClient(config);
						client.setPool(pool);
						long tempNano;
						while ((tempNano = System.nanoTime()) < targetNano) {
							{
								int tableIndex = rand.nextInt(tableCount * 10);
								if (tableIndex >= tableCount) {
									tableIndex = 0;
								}
								count[tableIndex].incrementAndGet();
								Put put2 = new Put(client.getTableSchema(tableNames[tableIndex]));
								long id = l.getAndIncrement();
								put2.setObject("id", id);
								put2.setObject("c", 12);
								put2.setObject("d", "aaa");
								put2.setObject("e", "123");
								client.put(put2);
								if (id % 100000 == 0) {
									System.out.println("current id:" + id);
								}
							}

							if (tempNano - currentNano > 10000 * 1000000L) {
								currentNano = tempNano;
								client.flush();
							}
						}
						client.flush();
					} catch (HoloClientException e) {
						e.printStackTrace();
						Assert.assertEquals(e.getMessage(), 1, 2);
					}
				};
				ExecutorService es = new ThreadPoolExecutor(20, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(20), r -> {
					Thread t = new Thread(r);
					return t;
				}, new ThreadPoolExecutor.AbortPolicy());
				for (int i = 0; i < 1; ++i) {
					es.execute(runnable);
				}
				es.shutdown();
				while (!es.awaitTermination(5000L, TimeUnit.MILLISECONDS)) {

				}
				for (int i = 0; i < tableCount; ++i) {
					try (Statement stat = conn.createStatement()) {
						try (ResultSet rs = stat.executeQuery("select count(*) from " + tableNames[i])) {
							if (rs.next()) {
								Assert.assertEquals((int) count[i].get(), rs.getInt(1));
							}
						}
					}
				}
			} finally {
				execute(conn, new String[]{dropSql});
				Metrics.closeReporter();
			}
		}
		synchronized (this) {
			this.wait(5000L);
		}
	}

	/**
	 * Put then delete
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut035() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_035\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,\"a\"\"b\" text,name text, \"address\" bytea,primary key(id,\"a\"\"b\"))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				Put put2 = new Put(schema);
				put2.setObject("id", 1);
				put2.setObject("a\"b", "name1");
				put2.setObject("name", "no0000");
				put2.setObject("address", new byte[]{0, 1, 2, 3});
				client.put(put2);
				put2 = new Put(schema);
				put2.setObject("id", 2);
				put2.setObject("a\"b", "name1");
				put2.setObject("name", "\u0000123");
				client.put(put2);
				put2 = new Put(schema);
				put2.setObject("id", 3);
				put2.setObject("a\"b", "name1");
				put2.setObject("name", "123\u0000");
				client.put(put2);
				put2 = new Put(schema);
				put2.setObject("id", 4);
				put2.setObject("a\"b", "name1");
				put2.setObject("name", "\u0000123\u0000");
				client.put(put2);
				put2 = new Put(schema);
				put2.setObject("id", 5);
				put2.setObject("a\"b", "name1");
				put2.setObject("name", "\u00001\u000023\u0000");
				client.put(put2);
				put2 = new Put(schema);
				put2.setObject("id", 6);
				put2.setObject("a\"b", "name1");
				put2.setObject("name", "1\u000023");
				client.put(put2);
				put2 = new Put(schema);
				put2.setObject("id", 7);
				put2.setObject("a\"b", "name1");
				put2.setObject("name", "1\u0000\u000023");
				client.put(put2);
				put2 = new Put(schema);
				put2.setObject("id", 8);
				put2.setObject("a\"b", "name1");
				put2.setObject("name", "1\u00002\u00003");
				client.put(put2);

				client.flush();
				{
					Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).setPrimaryKey("a\"b", "name1").build()).get();
					Assert.assertEquals("no0000", r.getObject("name"));
					Assert.assertArrayEquals(new byte[]{0, 1, 2, 3}, (byte[]) r.getObject("address"));
				}
				for (int i = 2; i < 9; ++i) {
					Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", i).setPrimaryKey("a\"b", "name1").build()).get();
					Assert.assertEquals(String.valueOf(i), "123", r.getObject("name"));
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * delete then put,insert_or_update
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut036() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"depot_waring_product\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				Put put2 = new Put(schema);
				put2.setObject("id", -1);
				put2.setObject("name", "name-1");
				client.put(put2);
				for (int i = 0; i < 500; ++i) {
					put2 = new Put(schema);
					put2.setObject("id", i);
					put2.getRecord().setType(Put.MutationType.DELETE);
					client.put(put2);
					put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("name", "name" + i);
					client.put(put2);
				}
				client.flush();

				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*),max(id),min(id) from " + tableName)) {
						rs.next();
						Assert.assertEquals(501, rs.getInt(1));
						Assert.assertEquals(499, rs.getInt(2));
						Assert.assertEquals(-1, rs.getInt(3));
					}
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * delete then put,insert_or_ignore
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut037() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_IGNORE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"depot_waring_product\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				Put put2 = new Put(schema);
				put2.setObject("id", -1);
				put2.setObject("name", "name-1");
				client.put(put2);
				for (int i = 0; i < 500; ++i) {
					put2 = new Put(schema);
					put2.setObject("id", i);
					put2.getRecord().setType(Put.MutationType.DELETE);
					client.put(put2);
					put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("name", "name" + i);
					client.put(put2);
				}
				client.flush();

				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*),max(id),min(id) from " + tableName)) {
						rs.next();
						Assert.assertEquals(501, rs.getInt(1));
						Assert.assertEquals(499, rs.getInt(2));
						Assert.assertEquals(-1, rs.getInt(3));
					}
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * delete then put,insert_or_replace
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut038() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"depot_waring_product\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				Put put2 = new Put(schema);
				put2.setObject("id", -1);
				put2.setObject("name", "name-1");
				client.put(put2);
				for (int i = 0; i < 500; ++i) {
					put2 = new Put(schema);
					put2.setObject("id", i);
					put2.getRecord().setType(Put.MutationType.DELETE);
					client.put(put2);
					put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("name", "name" + i);
					client.put(put2);
				}
				client.flush();

				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*),max(id),min(id) from " + tableName)) {
						rs.next();
						Assert.assertEquals(501, rs.getInt(1));
						Assert.assertEquals(499, rs.getInt(2));
						Assert.assertEquals(-1, rs.getInt(3));
					}
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * delete
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut039() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_039\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				int count = 100000;
				for (int i = 0; i < count; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("name", "name-1");
					client.put(put2);
				}
				client.flush();

				for (int i = 0; i < count - 2; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.getRecord().setType(Put.MutationType.DELETE);
					client.put(put2);
				}
				client.flush();
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
						rs.next();
						Assert.assertEquals(2, rs.getInt(1));
					}
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * delete
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut040() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_040\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,id2 int not null, name text,primary key(id,id2))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				int count = 100000;
				for (int i = 0; i < count; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("id2", i + 1);
					put2.setObject("name", "name-1");
					client.put(put2);
				}
				client.flush();

				for (int i = 0; i < count - 2; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("id2", i + 1);
					put2.getRecord().setType(Put.MutationType.DELETE);
					client.put(put2);
				}
				client.flush();
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
						rs.next();
						Assert.assertEquals(2, rs.getInt(1));
					}
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * async put
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut041() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_041\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,\"a\"\"b\" text,name text, \"address\" bytea,primary key(id,\"a\"\"b\"))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				AtomicInteger c = new AtomicInteger(0);
				TableSchema schema = client.getTableSchema(tableName, true);

				Put put2 = new Put(schema);
				put2.setObject("id", 1);
				put2.setObject("a\"b", "name1");
				put2.setObject("name", "no0000");
				put2.setObject("address", new byte[]{0, 1, 2, 3});
				client.putAsync(put2).thenRun(() -> {
					System.out.println("done:" + System.currentTimeMillis());
					c.incrementAndGet();
				});

				put2 = new Put(schema);
				put2.setObject("id", 1);
				put2.setObject("a\"b", "name1");
				put2.setObject("name", "no0000");
				put2.setObject("address", new byte[]{0, 1, 2, 3, 4});
				client.putAsync(put2).thenRun(() -> {
					System.out.println("done:" + System.currentTimeMillis());
					c.incrementAndGet();
				});

				put2 = new Put(schema);
				put2.setObject("id", 2);
				put2.setObject("a\"b", "name1");
				put2.setObject("name", "\u0000123");
				client.putAsync(put2).thenRun(() -> {
					System.out.println("done:" + System.currentTimeMillis());
					c.incrementAndGet();
				});
				put2 = new Put(schema);
				put2.setObject("id", 3);
				put2.setObject("a\"b", "name1");
				put2.setObject("name", "123\u0000");
				client.putAsync(put2).thenRun(() -> {
					System.out.println("done:" + System.currentTimeMillis());
					c.incrementAndGet();
				});
				put2 = new Put(schema);
				put2.setObject("id", 4);
				put2.setObject("a\"b", "name1");
				put2.setObject("name", "\u0000123\u0000");
				client.putAsync(put2).thenRun(() -> {
					System.out.println("done:" + System.currentTimeMillis());
					c.incrementAndGet();
				});
				put2 = new Put(schema);
				put2.setObject("id", 5);
				put2.setObject("a\"b", "name1");
				put2.setObject("name", "\u00001\u000023\u0000");
				client.putAsync(put2).thenRun(() -> {
					System.out.println("done:" + System.currentTimeMillis());
					c.incrementAndGet();
				});
				put2 = new Put(schema);
				put2.setObject("id", 6);
				put2.setObject("a\"b", "name1");
				put2.setObject("name", "1\u000023");
				client.putAsync(put2).thenRun(() -> {
					System.out.println("done:" + System.currentTimeMillis());
					c.incrementAndGet();
				});
				put2 = new Put(schema);
				put2.setObject("id", 7);
				put2.setObject("a\"b", "name1");
				put2.setObject("name", "1\u0000\u000023");
				client.putAsync(put2).thenRun(() -> {
					System.out.println("done:" + System.currentTimeMillis());
					c.incrementAndGet();
				});
				put2 = new Put(schema);
				put2.setObject("id", 8);
				put2.setObject("a\"b", "name1");
				put2.setObject("name", "1\u00002\u00003");
				client.putAsync(put2).thenRun(() -> {
					System.out.println("done:" + System.currentTimeMillis());
					c.incrementAndGet();
				});

				client.flush();
				Assert.assertEquals(9, c.get());
				{
					Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).setPrimaryKey("a\"b", "name1").build()).get();
					Assert.assertEquals("no0000", r.getObject("name"));
					Assert.assertArrayEquals(new byte[]{0, 1, 2, 3, 4}, (byte[]) r.getObject("address"));
				}
				for (int i = 2; i < 9; ++i) {
					Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", i).setPrimaryKey("a\"b", "name1").build()).get();
					Assert.assertEquals(String.valueOf(i), "123", r.getObject("name"));
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * put decimal
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut042() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_042\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,amount decimal(12,2),primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				AtomicInteger c = new AtomicInteger(0);
				TableSchema schema = client.getTableSchema(tableName, true);

				Put put2 = new Put(schema);
				put2.setObject("id", 0);
				put2.setObject("amount", "16.211");
				client.put(put2);

				put2 = new Put(schema);
				put2.setObject("id", 1);
				client.put(put2);

				client.flush();
				{
					Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
					Assert.assertEquals(new BigDecimal("16.21"), r.getObject("amount"));
				}
				{
					Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
					Assert.assertNull(r.getObject("amount"));
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * insert delete insert
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut043() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		config.setWriteThreadSize(3);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_043\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,amount decimal(12,2),primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				AtomicInteger c = new AtomicInteger(0);
				TableSchema schema = client.getTableSchema(tableName, true);

				Put put2 = new Put(schema);
				put2.setObject("id", 0);
				put2.setObject("amount", "16.211");
				client.put(put2);
				client.flush();

				put2 = new Put(schema);
				put2.setObject("id", 0);
				put2.getRecord().setType(Put.MutationType.DELETE);
				client.put(put2);

				put2 = new Put(schema);
				put2.setObject("id", 0);
				put2.setObject("amount", "19.211");
				client.put(put2);

				put2 = new Put(schema);
				put2.setObject("id", 2);
				put2.getRecord().setType(Put.MutationType.DELETE);
				client.put(put2);

				client.flush();
				{
					Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
					Assert.assertEquals(new BigDecimal("19.21"), r.getObject("amount"));
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * insert replace
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut044() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		config.setWriteThreadSize(3);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_044\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,amount decimal(12,2),primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				{
					for (int i = 0; i < 100; ++i) {
						Put put2 = new Put(schema);
						put2.setObject("id", 0);
						put2.setObject("amount", i);
						client.put(put2);
					}
					client.flush();
					Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
					Assert.assertEquals(new BigDecimal("99.00"), r.getObject("amount"));
				}
				{
					for (int i = 0; i < 100; ++i) {
						Put put2 = new Put(schema);
						put2.setObject("id", 1);
						put2.setObject("amount", i);
						client.put(put2);
					}
					{
						Put put2 = new Put(schema);
						put2.setObject("id", 1);
						put2.getRecord().setType(Put.MutationType.DELETE);
						client.put(put2);
					}
					client.flush();
					Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
					Assert.assertNull(r);
				}

				{
					for (int i = 0; i < 100; ++i) {
						Put put2 = new Put(schema);
						put2.setObject("id", 2);
						put2.setObject("amount", i);
						if (i == 98) {
							put2.getRecord().setType(Put.MutationType.DELETE);
						}
						client.put(put2);
					}
					client.flush();
					Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).build()).get();
					Assert.assertEquals(new BigDecimal("99.00"), r.getObject("amount"));
				}

				{
					for (int i = 0; i < 100; ++i) {
						Put put2 = new Put(schema);
						put2.setObject("id", 2);
						put2.setObject("amount", i);
						if (i == 90) {
							put2.getRecord().setType(Put.MutationType.DELETE);
						}
						client.put(put2);
					}
					client.flush();
					Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).build()).get();
					Assert.assertEquals(new BigDecimal("99.00"), r.getObject("amount"));
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * 账号删除应该抛HoloClientException而不是HoloClientWithDetailsException.
	 * Method: put(Put put).
	 * HoloClientWithDetailsException一般指脏数据，又调用方决定跳过还是终止人工干预;
	 * HoloClientException一般是故障了，就应该停止
	 */
	@Test
	public void testPutPut045() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteThreadSize(5);
		config.setWriteBatchSize(4);
		config.setUsername("BASIC$ttqs");
		config.setPassword("ttqspw");
		config.setConnectionMaxIdleMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_put_045";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int8 not null,name text,primary key(id))";

			String addUser = "CREATE USER \"BASIC$ttqs\" WITH PASSWORD 'ttqspw';";
			String dropUser = "drop user if exists \"BASIC$ttqs\"";
			execute(conn, new String[]{dropSql, createSql, addUser});

			try {
				TableSchema schema = client.getTableSchema(tableName);
				execute(conn, new String[]{dropUser});
				Thread.sleep(12000L);
				Exception exception = null;
				try {
					for (int i = 0; i < 20; ++i) {
						Put put = new Put(schema);
						put.setObject(0, i);
						put.setObject(1, "aaaa");
						client.put(put);
					}
					client.flush();
				} catch (HoloClientWithDetailsException e) {
					Assert.assertTrue(e.getClass().getName() + ":" + e.getMessage(), false);
				} catch (HoloClientException e) {
					exception = e;
					e.printStackTrace();
				}
				Assert.assertEquals(HoloClientException.class.getName(), exception.getClass().getName());
			} finally {
				execute(conn, new String[]{dropSql, dropUser});
			}
		}
	}

	/**
	 * delete in simple mode
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut046() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setJdbcUrl(config.getJdbcUrl() + "?preferQueryMode=simple");
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_046\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,id2 int not null, name text,primary key(id,id2))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				int count = 100000;
				for (int i = 0; i < count; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("id2", i + 1);
					put2.setObject("name", "name-1");
					client.put(put2);
				}
				client.flush();

				for (int i = 0; i < count - 2; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("id2", i + 1);
					put2.getRecord().setType(Put.MutationType.DELETE);
					client.put(put2);
				}
				client.flush();
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
						rs.next();
						Assert.assertEquals(2, rs.getInt(1));
					}
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * delete partition table
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut047() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_047\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,id2 int not null, name text,primary key(id,id2)) partition by list(id)";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				int count = 10000;
				for (int i = 0; i < count; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i / 1000);
					put2.setObject("id2", i + 1);
					put2.setObject("name", "name-1");
					client.put(put2);
				}
				client.flush();

				for (int i = 0; i < count - 2; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i / 1000);
					put2.setObject("id2", i + 1);
					put2.getRecord().setType(Put.MutationType.DELETE);
					client.put(put2);
				}
				client.flush();
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
						rs.next();
						Assert.assertEquals(2, rs.getInt(1));
					}
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * delete partition table，parititon key case sensitive
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut048() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_put_048\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(iD int not null,id2 int not null, name text,primary key(id,id2)) partition by list(iD)";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				int count = 10000;
				for (int i = 0; i < count; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i / 1000);
					put2.setObject("id2", i + 1);
					put2.setObject("name", "name-1");
					client.put(put2);
				}
				client.flush();

				for (int i = 0; i < count - 2; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i / 1000);
					put2.setObject("id2", i + 1);
					put2.getRecord().setType(Put.MutationType.DELETE);
					client.put(put2);
				}
				client.flush();
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
						rs.next();
						Assert.assertEquals(2, rs.getInt(1));
					}
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * delete partition table，public schema
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut049() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "\"holO_client_put_049\"";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(iD int not null,id2 int not null, name text, primary key(id,id2)) partition by list(iD)";

			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				int count = 10000;
				for (int i = 0; i < count; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i / 1000);
					put2.setObject("id2", i + 1);
					put2.setObject("name", "name-1");
					client.put(put2);
				}
				client.flush();

				for (int i = 0; i < count - 2; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i / 1000);
					put2.setObject("id2", i + 1);
					put2.getRecord().setType(Put.MutationType.DELETE);
					client.put(put2);
				}
				client.flush();
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
						rs.next();
						Assert.assertEquals(2, rs.getInt(1));
					}
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * delete partition table，public schema. int
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut050() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		config.setWriteThreadSize(5);
		config.setReadThreadSize(4);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "\"holO_client_put_050\"";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(iD int not null,id2 int not null, name int, primary key(id,id2)) partition by list(iD)";

			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				int count = 100000;
				for (int i = 0; i < count; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i / 10000);
					put2.setObject("id2", i + 1);
					put2.setObject("name", i);
					client.put(put2);
				}
				client.flush();

				AtomicInteger tic = new AtomicInteger(count);
				AtomicInteger toc = new AtomicInteger(count);
				for (int i = 0; i < count; ++i) {
					client.get(Get.newBuilder(schema).setPrimaryKey("id", i / 10000).setPrimaryKey("id2", i + 1).build()).thenAccept(record -> {
						tic.decrementAndGet();
						Assert.assertNotNull(record);
						Assert.assertEquals((int) record.getObject(1) - 1, (int) record.getObject(2));
						toc.decrementAndGet();
					});
				}
				while (tic.get() != 0) {
					synchronized (tic) {
						tic.wait(100L);
					}
				}
				Assert.assertEquals(tic.get(), toc.get());
				Metrics.reporter().report();

			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * delete partition table，public schema. text
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut051() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		config.setWriteThreadSize(5);
		config.setReadThreadSize(4);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "\"holO_client_put_051\"";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id text not null,id2 int not null, name int, primary key(id,id2)) partition by list(id)";

			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				int count = 10000;
				for (int i = 0; i < count; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", String.valueOf(i / 1000));
					put2.setObject("id2", i + 1);
					put2.setObject("name", i);
					client.put(put2);
				}
				client.flush();

				AtomicInteger tic = new AtomicInteger(count);
				AtomicInteger toc = new AtomicInteger(count);
				for (int i = 0; i < count; ++i) {
					client.get(Get.newBuilder(schema).setPrimaryKey("id", String.valueOf(i / 1000)).setPrimaryKey("id2", i + 1).build()).thenAccept(record -> {
						tic.decrementAndGet();
						Assert.assertNotNull(record);
						Assert.assertEquals((int) record.getObject(1) - 1, (int) record.getObject(2));
						toc.decrementAndGet();
					});
				}
				while (tic.get() != 0) {
					synchronized (tic) {
						tic.wait(100L);
					}
				}
				Assert.assertEquals(tic.get(), toc.get());
				Metrics.reporter().report();

			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * 非常非常长的sql.
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut052() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_put_052";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null";
			for (int i = 0; i < 820; ++i) {
				createSql += ", name_name_name" + i + " text";
			}
			createSql += ",primary key(id))";

			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);

				for (int i = 0; i < 1; ++i) {
					Put put = new Put(schema);
					put.setObject(0, 0);
					put.setObject(1, "name0");
					put.setObject(2, "address");
					client.put(put);
				}
				client.flush();

				Record r = client.get(new Get(schema, new Object[]{0})).get();
				Assert.assertEquals("name0", r.getObject(1));
			} finally {
				execute(conn, new String[]{dropSql});
			}

		}
	}

	/**
	 * schema version 不一致时尝试重建连接.
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut053() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteThreadSize(10);
		config.setRetryCount(20);
		config.setRetrySleepStepMs(0L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_put_053";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null";
			for (int i = 0; i < 3; ++i) {
				createSql += ", name_name_name" + i + " text";
			}
			createSql += ",primary key(id))";
			ExecutorService es = new ThreadPoolExecutor(20, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(20), r -> {
				Thread t = new Thread(r);
				return t;
			}, new ThreadPoolExecutor.AbortPolicy());
			AtomicBoolean stop = new AtomicBoolean(false);
			execute(conn, new String[]{dropSql, createSql});
			new Thread(() -> {
				try {
					client.sql((c) -> {
						c.setAutoCommit(false);
						try (Statement stat = c.createStatement()) {
							try (ResultSet rs = stat.executeQuery("select * from " + tableName + " limit 1")) {
								System.out.println("LOCK");
							}
							Thread.sleep(50000L);
						} catch (InterruptedException e) {

						} finally {
							c.setAutoCommit(true);
						}
						System.out.println("LOCK RELEASE");
						return null;
					}).get();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					stop.set(true);
				}
			}).start();
			new Thread(() -> {
				try {
					Thread.sleep(2000L);
					while (true) {
						try {
							client.sql((c) -> {
								c.setAutoCommit(false);
								try (Statement stat = c.createStatement()) {
									stat.setQueryTimeout(2);
									System.out.println("TRY ALTER");
									stat.execute("alter table " + tableName + " add column ss text");
									System.out.println("ALTER DONE");
								} finally {
									c.setAutoCommit(true);
								}
								return null;
							}).get();
							break;
						} catch (Exception e) {

						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}).start();
			try {
				TableSchema schema = client.getTableSchema(tableName);
				int index = 0;
				while (!stop.get()) {
					Put put = new Put(schema);
					put.setObject(0, ++index);
					put.setObject(1, "name0");
					put.setObject(2, "address");
					client.put(put);
				}
				client.flush();

				int count = -1;
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
						if (rs.next()) {
							count = rs.getInt(1);
						}
					}
				}
				Assert.assertEquals(index, count);
			} finally {
				execute(conn, new String[]{dropSql});
			}

		}
	}

	/**
	 * boolean and bit(1)
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut054() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "\"holO_client_put_054\"";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(iD int not null,c_bool boolean,c_bit1 bit(1),c_bit5 bit(5),c_bit6v bit varying(6) ,b_a boolean[], primary key(id))";

			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				{
					Put put = new Put(schema);
					put.setObject("id", 0);
					put.setObject("c_bool", true);
					put.setObject("c_bit1", "0");
					put.setObject("c_bit5", "0");
					put.setObject("c_bit6v", "0");
					put.setObject("b_a", new boolean[]{true, false});
					client.put(put);
				}
				client.flush();
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
						if (rs.next()) {
							Assert.assertEquals(1, rs.getInt(1));
						}
					}
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * boolean and bit(1)
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut055() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(true);
		config.setWriteMaxIntervalMs(10000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "\"holO_client_put_055\"";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(iD int not null,c_bool boolean,c_bit1 bit(1),c_bit5 bit(5),c_bit6v bit varying(6) ,b_a boolean[], primary key(id))";
			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				{
					Put put = new Put(schema);
					put.setObject("id", 0);
					put.setObject("c_bool", true);
					put.setObject("c_bit1", "0");
					put.setObject("c_bit5", "0");
					put.setObject("c_bit6v", "0");
					put.setObject("b_a", new boolean[]{true, false});
					client.put(put);
				}
				execute(conn, new String[]{dropSql});
				Thread.sleep(15000L);
				try {
					Put put = new Put(schema);
					put.setObject("id", 0);
					put.setObject("c_bool", true);
					put.setObject("c_bit1", "0");
					put.setObject("c_bit5", "0");
					put.setObject("c_bit6v", "0");
					put.setObject("b_a", new boolean[]{true, false});
					client.put(put);
				} catch (HoloClientWithDetailsException detailException) {
					LOG.error("", detailException);
				} catch (HoloClientException e) {
					LOG.error("", e);
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * 大列.
	 * Method: put(Put put).
	 */
	@Test
	public void testPutPut056() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(true);
		config.setWriteMaxIntervalMs(10000L);
		config.setWriteBatchSize(255);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "\"holO_client_put_056\"";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(iD int not null";
			for (int i = 0; i < 500; ++i) {
				createSql += ",c" + i + " int";
			}
			createSql += ",primary key(id))";
			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < 1000; ++i) {
					Put put = new Put(schema);
					put.setObject("id", i);
					for (int j = 0; j < 500; ++j) {
						put.setObject(j + 1, i);
					}
					client.put(put);
				}
				client.flush();
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
						rs.next();
						Assert.assertEquals(1000, rs.getInt(1));
					}
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	@Test
	public void testCloseConnectionAtJVMShutdown() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_IGNORE);
		config.setDynamicPartition(true);
		config.setEnableDefaultForNotNullColumn(false);
		HoloClient client = new HoloClient(config);
		try (Connection conn = buildConnection()) {
			client.setAsyncCommit(false);
			String tableName = "test_schema.\"holO_client_put_007_jvmshutdown\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,\"nAme\" text,\"address\" text not null,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			TableSchema schema = client.getTableSchema(tableName);

			final Put put = new Put(schema);
			put.setObject(0, 0);
			put.setObject(1, "name0");
			Assert.assertThrows(HoloClientException.class, () -> {
				client.put(put);
			});
			Put put2 = new Put(schema);
			put2.setObject(0, 1);
			put2.setObject(1, "name1");
			put2.setObject(2, "address2");
			client.put(put2);

			Record r = client.get(new Get(schema, new Object[]{0})).get();
			Assert.assertNull(r);

			r = client.get(new Get(schema, new Object[]{1})).get();
			Assert.assertEquals("name1", r.getObject(1));
			Assert.assertEquals("address2", r.getObject(2));
			execute(conn, new String[]{dropSql});
		}
	}
}
