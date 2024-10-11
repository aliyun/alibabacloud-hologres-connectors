/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloClientTestBase;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.impl.collector.ActionCollector;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.hologres.client.utils.Metrics;
import org.testng.Assert;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


/**
 * UT for ExecutionPool.
 */
public class ExecutionPoolTest extends HoloClientTestBase {
	/**
	 * ExecutionPool
	 * Method: put(Put put).
	 */
	@Test
	public void testExecutionPool001() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config.setDynamicPartition(true);
		config.setConnectionMaxIdleMs(10000L);
		config.setAppName("executionPool001");
		try (Connection conn = buildConnection(); ExecutionPool pool = ExecutionPool.buildOrGet("executionPool001", config, false)) {
			String tableName = "test_schema.holo_execution_pool_001";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,c int not null,d text,e text, primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				HoloClient client0 = new HoloClient(config);
				client0.setPool(pool);
				HoloClient client1 = new HoloClient(config);
				TableSchema schema = client0.getTableSchema(tableName, true);
				client1.setPool(pool);
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 0L);
					put2.setObject("c", 12);
					put2.setObject("d", "aaa");
					put2.setObject("e", "123");
					client0.put(put2);
				}
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 1L);
					put2.setObject("c", 12);
					put2.setObject("d", "aaa");
					put2.setObject("e", "123");
					client1.put(put2);
				}
				client0.close();
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 2L);
					put2.setObject("c", 12);
					put2.setObject("d", "aaa");
					put2.setObject("e", "123");
					client1.put(put2);
				}
				client1.close();
				client0 = new HoloClient(config);
				client0.setPool(pool);
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 3L);
					put2.setObject("c", 12);
					put2.setObject("d", "aaa");
					put2.setObject("e", "123");
					client0.put(put2);
				}
				client0.flush();

				int count = 0;
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
						if (rs.next()) {
							Assert.assertEquals(4, rs.getInt(1));
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
	 * ExecutionPool
	 * Method: put(Put put).
	 * different writeMode use one ExecutionPool
	 */
	@Test
	public void testExecutionPool002() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config0 = buildConfig();
		config0.setUseFixedFe(false);
		config0.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config0.setWriteBatchSize(1);
		config0.setWriteThreadSize(1);
		config0.setAppName("testExecutionPool002");
		HoloConfig config1 = buildConfig();
		config1.setUseFixedFe(false);
		config1.setWriteMode(WriteMode.INSERT_OR_IGNORE);
		config1.setWriteBatchSize(1);
		config1.setWriteThreadSize(3);
		try (Connection conn = buildConnection()) {
			String tableName0 = "holo_execution_pool_002_1";
			String tableName1 = "holo_execution_pool_002_2";
			String dropSql0 = "drop table if exists " + tableName0;
			String dropSql1 = "drop table if exists " + tableName1;
			String createSql0 = "create table " + tableName0
					+ "(id int not null,c int not null,d text,e text, primary key(id)); call set_table_property('"
					+ tableName0 + "','binlog.level','replica')";
			String createSql1 = "create table " + tableName1
					+ "(id int not null,c int not null,d text,e text, primary key(id)); call set_table_property('"
					+ tableName1 + "','binlog.level','replica')";
			execute(conn, new String[]{dropSql0, dropSql1, createSql0, createSql1});

			try (HoloClient client0 = new HoloClient(config0); HoloClient client1 = new HoloClient(config1)) {
				ExecutionPool pool = ExecutionPool.buildOrGet("testExecutionPool002", config0, false);
				Assert.assertEquals(pool.getWorkerCount(), 1);
				client0.setPool(pool);
				Assert.assertEquals(pool.getClientMapSize(), 1);
				TableSchema schema0 = client0.getTableSchema(tableName0, true);

				pool = ExecutionPool.buildOrGet("testExecutionPool002", config1, false);
				Assert.assertEquals(pool.getWorkerCount(), 3);
				client1.setPool(pool);
				Assert.assertEquals(pool.getClientMapSize(), 2);
				TableSchema schema1 = client1.getTableSchema(tableName1, true);
				for (int i = 0; i < 10; i++) {
					Put put = new Put(schema0);
					put.setObject("id", 0L);
					put.setObject("c", i);
					put.setObject("d", "aaa");
					put.setObject("e", "123");
					client0.put(put);
				}
				client0.flush();

				for (int i = 0; i < 10; i++) {
					Put put = new Put(schema1);
					put.setObject("id", 0L);
					put.setObject("c", i);
					put.setObject("d", "aaa");
					put.setObject("e", "123");
					client1.put(put);
				}
				client1.flush();

				Thread.sleep(1000);
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(hg_binlog_lsn) from " + tableName0)) {
						if (rs.next()) {
							// 1 insert + 9 update: 1 + 2 * 9 = 19
							Assert.assertEquals(19, rs.getInt(1));
						}
					}
					try (ResultSet rs = stat.executeQuery("select count(hg_binlog_lsn) from " + tableName1)) {
						if (rs.next()) {
							// 1 insert
							Assert.assertEquals(1, rs.getInt(1));
						}
					}
				}
				Metrics.reporter().report();
			} finally {
				execute(conn, new String[]{dropSql0, dropSql1});
			}
		}
		synchronized (this) {
			this.wait(5000L);
		}
	}

	/**
	 * ExecutionPool
	 * Method: put(Put put).
	 */
	@Test
	public void testExecutionPool003() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		HoloConfig fixedConfig = buildConfig();
		fixedConfig.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		fixedConfig.setDynamicPartition(true);
		fixedConfig.setConnectionMaxIdleMs(10000L);
		fixedConfig.setAppName("executionPool001");
		fixedConfig.setUseFixedFe(true);
		try (Connection conn = buildConnection(); ExecutionPool pool = ExecutionPool.buildOrGet("executionPool003", config, false, config.isUseFixedFe()); ExecutionPool fixedPool = ExecutionPool.buildOrGet("fixedExecutionPool003", fixedConfig, false, fixedConfig.isUseFixedFe())) {
			String tableName = "test_schema.holo_execution_pool_003";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,c int not null,d text,e text, primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				HoloClient client0 = new HoloClient(fixedConfig);
				client0.setPool(pool);
				client0.setFixedPool(fixedPool);
				HoloClient client1 = new HoloClient(fixedConfig);
				TableSchema schema = client0.getTableSchema(tableName, true);
				client1.setPool(pool);
				client1.setFixedPool(fixedPool);
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 0L);
					put2.setObject("c", 12);
					put2.setObject("d", "aaa");
					put2.setObject("e", "123");
					client0.put(put2);
				}
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 1L);
					put2.setObject("c", 12);
					put2.setObject("d", "aaa");
					put2.setObject("e", "123");
					client1.put(put2);
				}
				client0.close();
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 2L);
					put2.setObject("c", 12);
					put2.setObject("d", "aaa");
					put2.setObject("e", "123");
					client1.put(put2);
				}
				client1.close();
				client0 = new HoloClient(fixedConfig);
				client0.setPool(pool);
				client0.setFixedPool(fixedPool);
				{
					Put put2 = new Put(schema);
					put2.setObject("id", 3L);
					put2.setObject("c", 12);
					put2.setObject("d", "aaa");
					put2.setObject("e", "123");
					client0.put(put2);
				}
				client0.flush();

				int count = 0;
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
						if (rs.next()) {
							Assert.assertEquals(4, rs.getInt(1));
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
	 * ExecutionPool
	 * Method: setFixedPool.
	 */
	@Test
	public void testExecutionPool004() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		HoloConfig fixedConfig = buildConfig();
		fixedConfig.setUseFixedFe(true);

		//set FixedPool only
		try (ExecutionPool fixedPool = ExecutionPool.buildOrGet("fixedExecutionPool004", fixedConfig, false, fixedConfig.isUseFixedFe())) {
			Class<HoloClient> clientClass = HoloClient.class;
			Class<ActionCollector> collectorClass = ActionCollector.class;
			HoloClient client0 = new HoloClient(fixedConfig);
			client0.setFixedPool(fixedPool);

			Method ensurePoolOpen = clientClass.getDeclaredMethod("ensurePoolOpen");
			ensurePoolOpen.setAccessible(true);
			ensurePoolOpen.invoke(client0);

			Field collectorField = clientClass.getDeclaredField("collector");
			collectorField.setAccessible(true);
			ActionCollector collector = (ActionCollector) (collectorField.get(client0));

			Field poolField = collectorClass.getDeclaredField("pool");
			poolField.setAccessible(true);
			ExecutionPool curPool = (ExecutionPool) poolField.get(collector);

			Assert.assertEquals(curPool.isFixedPool(), true);
		}

		//set Pool only
		try (ExecutionPool pool = ExecutionPool.buildOrGet("ExecutionPool004", config, false, config.isUseFixedFe())) {
			Class<HoloClient> clientClass = HoloClient.class;
			Class<ActionCollector> collectorClass = ActionCollector.class;
			HoloClient client0 = new HoloClient(config);
			client0.setPool(pool);

			Method ensurePoolOpen = clientClass.getDeclaredMethod("ensurePoolOpen");
			ensurePoolOpen.setAccessible(true);
			ensurePoolOpen.invoke(client0);

			Field collectorField = clientClass.getDeclaredField("collector");
			collectorField.setAccessible(true);
			ActionCollector collector = (ActionCollector) (collectorField.get(client0));

			Field poolField = collectorClass.getDeclaredField("pool");
			poolField.setAccessible(true);
			ExecutionPool curPool = (ExecutionPool) poolField.get(collector);

			Assert.assertEquals(curPool.isFixedPool(), false);
		}

		//set Pool And FixedPool
		try (ExecutionPool pool = ExecutionPool.buildOrGet("ExecutionPool004", config, false, config.isUseFixedFe()); ExecutionPool fixedPool = ExecutionPool.buildOrGet("fixedExecutionPool004", fixedConfig, false, fixedConfig.isUseFixedFe())) {
			Class<HoloClient> clientClass = HoloClient.class;
			Class<ActionCollector> collectorClass = ActionCollector.class;
			HoloClient client0 = new HoloClient(fixedConfig);
			client0.setPool(pool);
			client0.setFixedPool(fixedPool);

			Method ensurePoolOpen = clientClass.getDeclaredMethod("ensurePoolOpen");
			ensurePoolOpen.setAccessible(true);
			ensurePoolOpen.invoke(client0);

			Field collectorField = clientClass.getDeclaredField("collector");
			collectorField.setAccessible(true);
			ActionCollector collector = (ActionCollector) (collectorField.get(client0));

			Field poolField = collectorClass.getDeclaredField("pool");
			poolField.setAccessible(true);
			ExecutionPool curPool = (ExecutionPool) poolField.get(collector);

			Assert.assertEquals(curPool.isFixedPool(), true);
		}

		synchronized (this) {
			this.wait(5000L);
		}
	}

	/**
	 * ExecutionPool
	 * Method: put(Put put).
	 * different writeMode use one ExecutionPool, use fixed fe
	 */
	@Test
	public void testExecutionPool005() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config0 = buildConfig();
		config0.setUseFixedFe(true);
		config0.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config0.setWriteBatchSize(1);
		config0.setWriteThreadSize(1);
		config0.setReadThreadSize(2);
		config0.setConnectionSizeWhenUseFixedFe(1);
		config0.setAppName("testExecutionPool005");
		HoloConfig config1 = buildConfig();
		config1.setUseFixedFe(true);
		config1.setWriteMode(WriteMode.INSERT_OR_IGNORE);
		config1.setWriteBatchSize(1);
		config1.setWriteThreadSize(3);
		config1.setReadThreadSize(1);
		config1.setConnectionSizeWhenUseFixedFe(2);
		try (Connection conn = buildConnection()) {
			String tableName0 = "holo_execution_pool_005_1";
			String tableName1 = "holo_execution_pool_005_2";
			String dropSql0 = "drop table if exists " + tableName0;
			String dropSql1 = "drop table if exists " + tableName1;
			String createSql0 = "create table " + tableName0
					+ "(id int not null,c int not null,d text,e text, primary key(id)); call set_table_property('"
					+ tableName0 + "','binlog.level','replica')";
			String createSql1 = "create table " + tableName1
					+ "(id int not null,c int not null,d text,e text, primary key(id)); call set_table_property('"
					+ tableName1 + "','binlog.level','replica')";
			execute(conn, new String[]{dropSql0, dropSql1, createSql0, createSql1});

			try (HoloClient client0 = new HoloClient(config0); HoloClient client1 = new HoloClient(config1)) {
				ExecutionPool pool = ExecutionPool.buildOrGet("testExecutionPool005", config0, false);
				ExecutionPool fixedPool = ExecutionPool.buildOrGet("fixed-testExecutionPool005", config0, false, true);
				Assert.assertEquals(pool.getWorkerCount(), 1); // connectionSizeWhenUseFixedFe = 1
				Assert.assertEquals(fixedPool.getWorkerCount(), 2); // readThreadSize = 2
				client0.setPool(pool);
				client0.setFixedPool(fixedPool);
				Assert.assertEquals(pool.getClientMapSize(), 1);
				Assert.assertEquals(fixedPool.getClientMapSize(), 1);
				pool = ExecutionPool.buildOrGet("testExecutionPool005", config1, false);
				fixedPool = ExecutionPool.buildOrGet("fixed-testExecutionPool005", config1, false, true);
				Assert.assertEquals(pool.getWorkerCount(), 2); // connectionSizeWhenUseFixedFe = 2
				Assert.assertEquals(fixedPool.getWorkerCount(), 3); // writeThreadSize = 3
				client1.setPool(pool);
				client1.setFixedPool(fixedPool);
				Assert.assertEquals(pool.getClientMapSize(), 2);
				Assert.assertEquals(fixedPool.getClientMapSize(), 2);
				TableSchema schema0 = client0.getTableSchema(tableName0, true);
				TableSchema schema1 = client1.getTableSchema(tableName1, true);
				for (int i = 0; i < 10; i++) {
					Put put = new Put(schema0);
					put.setObject("id", 0L);
					put.setObject("c", i);
					put.setObject("d", "aaa");
					put.setObject("e", "123");
					client0.put(put);
				}
				client0.flush();

				for (int i = 0; i < 10; i++) {
					Put put = new Put(schema1);
					put.setObject("id", 0L);
					put.setObject("c", i);
					put.setObject("d", "aaa");
					put.setObject("e", "123");
					client1.put(put);
				}
				client1.flush();

				Thread.sleep(1000);
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(hg_binlog_lsn) from " + tableName0)) {
						if (rs.next()) {
							// 1 insert + 9 update: 1 + 2 * 9 = 19
							Assert.assertEquals(19, rs.getInt(1));
						}
					}
					try (ResultSet rs = stat.executeQuery("select count(hg_binlog_lsn) from " + tableName1)) {
						if (rs.next()) {
							// 1 insert
							Assert.assertEquals(1, rs.getInt(1));
						}
					}
				}
				Metrics.reporter().report();
			} finally {
				execute(conn, new String[]{dropSql0, dropSql1});
			}
		}
		synchronized (this) {
			this.wait(5000L);
		}
	}

	/**
	 * ExecutionPool
	 * Method: put(Put put).
	 * buildOrGet use same name but different config, one use fixed fe, the other not
	 */
	@Test
	public void testExecutionPool006() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config0 = buildConfig();
		config0.setUseFixedFe(true);
		config0.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		config0.setWriteBatchSize(1);
		config0.setWriteThreadSize(1);
		config0.setReadThreadSize(2);
		config0.setConnectionSizeWhenUseFixedFe(1);
		config0.setAppName("testExecutionPool005");
		HoloConfig config1 = buildConfig();
		// gitconfig1.setUseFixedFe(true);
		config1.setWriteMode(WriteMode.INSERT_OR_IGNORE);
		config1.setWriteBatchSize(1);
		config1.setWriteThreadSize(3);
		config1.setReadThreadSize(1);
		try (Connection conn = buildConnection()) {
			String tableName0 = "holo_execution_pool_006_1";
			String tableName1 = "holo_execution_pool_006_2";
			String dropSql0 = "drop table if exists " + tableName0;
			String dropSql1 = "drop table if exists " + tableName1;
			String createSql0 = "create table " + tableName0
					+ "(id int not null,c int not null,d text,e text, primary key(id)); call set_table_property('"
					+ tableName0 + "','binlog.level','replica')";
			String createSql1 = "create table " + tableName1
					+ "(id int not null,c int not null,d text,e text, primary key(id)); call set_table_property('"
					+ tableName1 + "','binlog.level','replica')";
			execute(conn, new String[]{dropSql0, dropSql1, createSql0, createSql1});

			try (HoloClient client0 = new HoloClient(config0); HoloClient client1 = new HoloClient(config1)) {
				ExecutionPool pool0 = ExecutionPool.buildOrGet("testExecutionPool006", config0, false);
				Assert.assertEquals(pool0.getWorkerCount(), 1); // connectionSizeWhenUseFixedFe = 1
				client0.setPool(pool0);
				Assert.assertEquals(pool0.getClientMapSize(), 1);
				ExecutionPool pool1 = ExecutionPool.buildOrGet("testExecutionPool006", config1, false);
				Assert.assertEquals(pool1.getWorkerCount(), 3); // writeThreadSize = 3
				client1.setPool(pool1);
				Assert.assertEquals(pool1.getClientMapSize(), 1);
				Assert.assertNotEquals(pool0, pool1); // same name but different useFixedFe
				TableSchema schema0 = client0.getTableSchema(tableName0, true);
				TableSchema schema1 = client1.getTableSchema(tableName1, true);
				for (int i = 0; i < 10; i++) {
					Put put = new Put(schema0);
					put.setObject("id", 0L);
					put.setObject("c", i);
					put.setObject("d", "aaa");
					put.setObject("e", "123");
					client0.put(put);
				}
				client0.flush();

				for (int i = 0; i < 10; i++) {
					Put put = new Put(schema1);
					put.setObject("id", 0L);
					put.setObject("c", i);
					put.setObject("d", "aaa");
					put.setObject("e", "123");
					client1.put(put);
				}
				client1.flush();

				Thread.sleep(1000);
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(hg_binlog_lsn) from " + tableName0)) {
						if (rs.next()) {
							// 1 insert + 9 update: 1 + 2 * 9 = 19
							Assert.assertEquals(19, rs.getInt(1));
						}
					}
					try (ResultSet rs = stat.executeQuery("select count(hg_binlog_lsn) from " + tableName1)) {
						if (rs.next()) {
							// 1 insert
							Assert.assertEquals(1, rs.getInt(1));
						}
					}
				}
				Metrics.reporter().report();
			} finally {
				execute(conn, new String[]{dropSql0, dropSql1});
			}
		}
		synchronized (this) {
			this.wait(5000L);
		}
	}

	/**
	 * ExecutionPool
	 * Method: put(Put put).
	 * 复用pool多线程测试，主要用于验证是否存在可能的死锁.
	 * 测试时间比较久，默认不跑.
	 */
	@Ignore
	@Test
	public void testExecutionPool007() throws Exception {
		if (properties == null) {
			return;
		}
		long start = System.currentTimeMillis();
		try (Connection conn = buildConnection()) {
			for (int i = 0; i < 100; i++) {
				String tableName = "holO_client_execution_pool_007_" + i;
				String dropSql = "drop table if exists " + tableName;
				String createSql = "create table " + tableName + "(id int not null, name text, primary key(id));";
				execute(conn, new String[]{dropSql, createSql});
			}
		}
		for (int c = 0; c < 100; c++) {
			HoloConfig config = buildConfig();
			config.setWriteBatchSize(256);
			config.setWriteThreadSize(1);
			config.setWriteMaxIntervalMs(3000);

			try (ExecutionPool pool = ExecutionPool.buildOrGet("testExecutionPool007", config, false)) {
				AtomicBoolean running = new AtomicBoolean(true);
				AtomicReference<Exception> failed = new AtomicReference(null);
				ExecutorService es = new ThreadPoolExecutor(100, 100, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(2), r -> {
					Thread t = new Thread(r);
					return t;
				}, new ThreadPoolExecutor.AbortPolicy());
				AtomicInteger id = new AtomicInteger(0);
				Runnable insert = () -> {
					HoloConfig config1 = buildConfig();
					config1.setWriteThreadSize(new Random().nextInt(10) + 1);
					String tableName = "holO_client_execution_pool_007_" + id.getAndIncrement();
					try (HoloClient client = new HoloClient(config1)) {
						ExecutionPool pool0 = ExecutionPool.buildOrGet("testExecutionPool007", config1, false);
						client.setPool(pool0);
						TableSchema schema = client.getTableSchema(tableName, true);
						Thread.sleep(100 * new Random().nextInt(10));
						for (int i = 0; i < 1000; i++) {
							Put put = new Put(schema);
							put.setObject("id", i);
							put.setObject("name", "aaa");
							client.put(put);
						}
						client.flush();
					} catch (Exception e) {
						failed.set(e);
					}
				};

				for (int i = 0; i < 100; i++) {
					es.execute(insert);
				}
				running.set(false);

				es.shutdown();
				while (!es.awaitTermination(5000L, TimeUnit.MILLISECONDS)) {

				}

				if (failed.get() != null) {
					Assert.fail("fail", failed.get());
				}

			}
		}
		try (Connection conn = buildConnection()) {
			for (int i = 0; i < 100; i++) {
				String tableName = "holO_client_execution_pool_007_" + i;
				String dropSql = "drop table if exists " + tableName;
				execute(conn, new String[]{dropSql});
			}
		}
		long end = System.currentTimeMillis();
		LOG.info("run test 100 times use {}s", (end - start) / 1000);
	}
}
