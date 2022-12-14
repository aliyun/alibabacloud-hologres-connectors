package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.ExecutionPool;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * HoloClient Tester.
 *
 * @version 1.0
 * @since <pre>12月 2, 2020</pre>
 */
public class HoloClientGetTest extends HoloClientTestBase {

	/**
	 * get bytea
	 * Method: put(Put put).
	 */
	@Test
	public void testGet001() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteMaxIntervalMs(3000L);
		config.setEnableDefaultForNotNullColumn(false);
		config.setAppName("testGet001");
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.holo_client_get_001";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id bytea not null,name text not null,address text,primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);

				Put put = new Put(schema);
				put.setObject(0, new byte[]{1, 2});
				put.setObject(1, "name0");
				put.setObject(2, "address0");
				client.put(put);

				client.flush();

				Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", new byte[]{1, 2}).build()).get();
				Assert.assertEquals("name0", r.getObject("name"));
				Assert.assertEquals("address0", r.getObject("address"));

			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * get long int short
	 * Method: put(Put put).
	 */
	@Test
	public void testGet002() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteMaxIntervalMs(3000L);
		config.setEnableDefaultForNotNullColumn(false);
		config.setAppName("testGet002");
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.holo_client_get_002";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text,i2 int2,i4 int4,i8 int8, primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);

				Put put = new Put(schema);
				put.setObject(0, 0);
				put.setObject(1, "name0");
				put.setObject(2, 5);
				put.setObject(3, 6);
				put.setObject(4, 7);
				client.put(put);

				client.flush();

				Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).withSelectedColumns(new String[]{"i8", "i4", "i2"}).build()).get();

				Assert.assertNull(r.getObject("name"));

				Object i2 = r.getObject("i2");
				Assert.assertEquals((short) 5, i2);

				Object i4 = r.getObject("i4");
				Assert.assertEquals(6, i4);

				Object i8 = r.getObject("i8");
				Assert.assertEquals(7L, i8);

			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * get when fail
	 * Method: put(Put put).
	 */
	@Test
	public void testGet003() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteMaxIntervalMs(3000L);
		config.setEnableDefaultForNotNullColumn(false);
		config.setAppName("testGet002");
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.holo_client_get_003";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text,i2 int2,i4 int4,i8 int8, primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);

				Put put = new Put(schema);
				put.setObject(0, 0);
				put.setObject(1, "name0");
				put.setObject(2, 5);
				put.setObject(3, 6);
				put.setObject(4, 7);
				client.put(put);

				client.flush();

				execute(conn, new String[]{dropSql});
				{

					Assert.assertThrows(HoloClientException.class, () -> {
						try {
							Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).withSelectedColumns(new String[]{"i8", "i4", "i2"}).build()).get();
						} catch (ExecutionException e) {
							throw e.getCause();
						}
					});
				}
				execute(conn, new String[]{createSql});

				put = new Put(schema);
				put.setObject(0, 0);
				put.setObject(1, "name0");
				put.setObject(2, 5);
				put.setObject(3, 6);
				put.setObject(4, 7);
				client.put(put);

				client.flush();
				{
					Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).withSelectedColumns(new String[]{"i8", "i4", "i2"}).build()).get();
					Assert.assertNull(r.getObject("name"));

					Object i2 = r.getObject("i2");
					Assert.assertEquals((short) 5, i2);

					Object i4 = r.getObject("i4");
					Assert.assertEquals(6, i4);

					Object i8 = r.getObject("i8");
					Assert.assertEquals(7L, i8);
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * get waiting timeout
	 * Method: put(Put put).
	 */
	@Test
	public void testGet004() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		// 1ms 便于测试
		config.setReadTimeoutMilliseconds(1);
		config.setAppName("testGet004");
		config.setReadRetryCount(3);
		config.setReadThreadSize(1);
		config.setWriteBatchSize(1);
		try (Connection conn = buildConnection(); ExecutionPool pool = ExecutionPool.buildOrGet("testGet004", config, false)) {
			String tableName = "test_schema.holo_client_get_004";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text, primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				AtomicBoolean failed = new AtomicBoolean(false);
				ExecutorService es = new ThreadPoolExecutor(20, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(20), r -> {
					Thread t = new Thread(r);
					return t;
				}, new ThreadPoolExecutor.AbortPolicy());
				// 多线程单连接执行get, 部分get的等待时间可能大于1ms
				Runnable runnable = () -> {
					try {
						HoloClient client = new HoloClient(config);
						client.setPool(pool);
						TableSchema schema = client.getTableSchema(tableName, true);
						Put put = new Put(schema);
						put.setObject(0, 0);
						put.setObject(1, "name0");
						client.put(put);
						client.flush();
						Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
						Assert.assertEquals("name0", r.getObject("name"));
					} catch (ExecutionException | HoloClientException | InterruptedException e) {
						e.printStackTrace();
						Assert.assertTrue(e.getMessage().contains("get waiting timeout before submit to holo"));
						// 其他error message则测试失败
						if (!e.getMessage().contains("get waiting timeout before submit to holo")) {
							failed.set(true);
						}
					}
				};
				for (int i = 0; i < 20; ++i) {
					es.execute(runnable);
				}
				es.shutdown();
				while (!es.awaitTermination(5000L, TimeUnit.MILLISECONDS)) {

				}
				if (failed.get()) {
					Assert.fail();
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
		synchronized (this) {
			this.wait(5000L);
		}
	}

	/**
	 * times of get > 5, to test statement name is saved
	 * Method: put(Put put).
	 */
	@Test
	public void testGet005() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteMaxIntervalMs(3000L);
		config.setEnableDefaultForNotNullColumn(false);
		config.setAppName("testGet005");
		config.setWriteBatchSize(128);
		config.setReadThreadSize(10);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.holo_client_get_005";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName
				+ "(id int not null,name text not null,address text,primary key(id));";
			execute(conn, new String[] {createSchema, dropSql, createSql});
			try {
				TableSchema schema = client.getTableSchema(tableName);
				for (int i = 0; i < 10000; i++) {
					Put put = new Put(schema);
					put.setObject(0, i);
					put.setObject(1, "name0");
					put.setObject(2, "address0");
					client.put(put);
				}
				client.flush();
				for (int i = 1; i < 100; i++) {
					Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", i).build()).get();
					Assert.assertEquals("name0", r.getObject("name"));
					Assert.assertEquals("address0", r.getObject("address"));
				}
			} finally {
				execute(conn, new String[] {dropSql});
			}
		}
	}
}
