/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.connectionhandler;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloClientTestBase;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.exception.HoloClientWithDetailsException;
import com.alibaba.hologres.client.utils.Metrics;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.model.TableSchema;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * retry单元测试.
 */
public class RetryTest extends HoloClientTestBase {

	/**
	 * truncate, need retry.
	 * Method: put(Put put).
	 * 必须要多个few的实例才可能抛异常进而出发retry！！！
	 */
	@Test
	public void testRetry001() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteThreadSize(5);
		config.setWriteBatchSize(4);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_retry_001";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int8 not null,name text,primary key(id))";

			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);

				ExecutorService es = new ThreadPoolExecutor(20, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(20), r -> {
					Thread t = new Thread(r);
					return t;
				}, new ThreadPoolExecutor.AbortPolicy());

				final AtomicBoolean running = new AtomicBoolean(true);
				Runnable truncateJob = () -> {
					try {
						for (int i = 0; i < 10; ++i) {
							Thread.sleep(1000L);
							try (Statement stat = conn.createStatement()) {
								stat.execute("truncate " + tableName);
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
						Assert.assertTrue(e.getClass().getName() + ":" + e.getMessage(), false);
					} finally {
						running.set(false);
					}
				};
				es.submit(truncateJob);
				long index = 0L;
				try {
					while (running.get()) {
						Put put = new Put(schema);
						put.setObject(0, ++index);
						put.setObject(1, "aaaa");
						client.put(put);
					}
					client.flush();
					Metrics.reporter().report();
				} catch (HoloClientException e) {
					Assert.assertTrue(e.getClass().getName() + ":" + e.getMessage(), false);
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * readonly，首先会重试，重试到最后，抛HoloClientException而不是HoloClientWithDetailsException.
	 * Method: put(Put put).
	 * HoloClientWithDetailsException一般指脏数据，又调用方决定跳过还是终止人工干预;
	 * HoloClientException一般是故障了，就应该停止
	 */
	@Test
	public void testRetry002() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteThreadSize(5);
		config.setWriteBatchSize(4);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_retry_002";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int8 not null,name text,primary key(id))";
			String stopReadOnly = "select hg_stop_readonly()";
			execute(conn, new String[]{dropSql, createSql});

			try {
				ExecutorService es = new ThreadPoolExecutor(20, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(20), r -> {
					Thread t = new Thread(r);
					return t;
				}, new ThreadPoolExecutor.AbortPolicy());

				final AtomicBoolean running = new AtomicBoolean(true);
				Runnable startReadOnly = () -> {
					try {
						Thread.sleep(2000L);
						try (Statement stat = conn.createStatement()) {
							stat.execute("select  hg_start_readonly()");
						}
					} catch (Exception e) {
						e.printStackTrace();
						Assert.assertTrue(e.getClass().getName() + ":" + e.getMessage(), false);
					} finally {
						running.set(false);
					}
				};
				es.submit(startReadOnly);

				TableSchema schema = client.getTableSchema(tableName);
				Exception exception = null;
				try {
					int i = 0;
					while (true) {
						Put put = new Put(schema);
						put.setObject(0, ++i);
						put.setObject(1, "aaaa");
						client.put(put);
					}
				} catch (HoloClientWithDetailsException e) {
					Assert.assertTrue(e.getClass().getName() + ":" + e.getMessage(), false);
				} catch (HoloClientException e) {
					exception = e;
					e.printStackTrace();
				}
				Assert.assertEquals(HoloClientException.class.getName(), exception.getClass().getName());
			} finally {
				execute(conn, new String[]{stopReadOnly, dropSql});
			}
		}
	}


	/**
	 * readonly，多点重试，都会写入成功
	 * Method: put(Put put).
	 * HoloClientWithDetailsException一般指脏数据，又调用方决定跳过还是终止人工干预;
	 * HoloClientException一般是故障了，就应该停止
	 */
	@Test
	public void testRetry003() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteThreadSize(5);
		config.setWriteBatchSize(4);
		config.setRetrySleepStepMs(5000L);
		config.setRetryCount(4); //1+6+11=18s
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_retry_003";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int8 not null,name text,primary key(id))";
			String stopReadOnly = "select hg_stop_readonly()";
			execute(conn, new String[]{dropSql, createSql});

			try {
				ExecutorService es = new ThreadPoolExecutor(20, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(20), r -> {
					Thread t = new Thread(r);
					return t;
				}, new ThreadPoolExecutor.AbortPolicy());

				final AtomicBoolean running = new AtomicBoolean(true);
				Runnable startReadOnly = () -> {
					try {
						Thread.sleep(2000L);
						try (Statement stat = conn.createStatement()) {
							stat.execute("select  hg_start_readonly()");
						}
						Thread.sleep(10000L);
						try (Statement stat = conn.createStatement()) {
							stat.execute("select  hg_stop_readonly()");
						}
					} catch (Exception e) {
						e.printStackTrace();
						Assert.assertTrue(e.getClass().getName() + ":" + e.getMessage(), false);
					} finally {
						running.set(false);
					}
				};
				es.submit(startReadOnly);

				TableSchema schema = client.getTableSchema(tableName);

				int i = 0;
				try {
					while (running.get()) {
						Put put = new Put(schema);
						put.setObject(0, ++i);
						put.setObject(1, "aaaa");
						client.put(put);
					}
					client.flush();
				} catch (HoloClientWithDetailsException e) {
					Assert.assertTrue(e.getClass().getName() + ":" + e.getMessage(), false);
				} catch (HoloClientException e) {
					Assert.assertTrue(e.getClass().getName() + ":" + e.getMessage(), false);
				}

				int count = client.sql(conn0 -> {
					try (Statement stat = conn0.createStatement()) {
						try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
							if (rs.next()) {
								return rs.getInt(1);
							}
						}
						return 0;
					}
				}).get();

				Assert.assertNotEquals(0, count);
				Assert.assertEquals(i, count);
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}


	/**
	 * alter, need retry.
	 * Method: put(Put put).
	 * 必须要多个few的实例才可能抛异常进而出发retry！！！
	 */
	@Test
	public void testRetry004() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteThreadSize(10);
		config.setWriteBatchSize(4);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_retry_004";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int8 not null,name text,primary key(id))";

			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);

				ExecutorService es = new ThreadPoolExecutor(20, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(20), r -> {
					Thread t = new Thread(r);
					return t;
				}, new ThreadPoolExecutor.AbortPolicy());

				final AtomicBoolean running = new AtomicBoolean(true);
				Runnable alterJob = () -> {
					try {
						for (int i = 0; i < 10; ++i) {
							Thread.sleep(1000L);
							try (Statement stat = conn.createStatement()) {
								stat.execute("call set_table_property('" + tableName + "','time_to_live_in_seconds','" + ((i + 1) * 3600) + "')");
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
						Assert.assertTrue(e.getClass().getName() + ":" + e.getMessage(), false);
					} finally {
						running.set(false);
					}
				};
				Runnable truncateJob = () -> {
					try {
						try (Statement stat = conn.createStatement()) {
							stat.execute("create table if not exists just_table (id int)");
						}
						while (running.get()) {
							try (Statement stat = conn.createStatement()) {
								stat.execute("truncate table just_table");
							}
						}
						try (Statement stat = conn.createStatement()) {
							stat.execute("drop table if exists just_table");
						}
					} catch (Exception e) {
						e.printStackTrace();
						Assert.assertTrue(e.getClass().getName() + ":" + e.getMessage(), false);
					} finally {

					}
				};
				es.submit(alterJob);
				//es.submit(truncateJob);
				long index = 0L;
				try {
					System.out.println("gogogo:" + schema.getSchemaVersion());
					while (running.get()) {
						Put put = new Put(schema);
						put.setObject(0, ++index);
						put.setObject(1, "aaaa");
						client.put(put);
					}
					client.flush();
					Metrics.reporter().report();
				} catch (HoloClientException e) {
					Assert.assertTrue(e.getClass().getName() + ":" + e.getMessage(), false);
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * 没有权限, 不要重试且抛HoloClientException.
	 * Method: put(Put put).
	 * 必须要多个few的实例才可能抛异常进而出发retry！！！
	 */
	@Test
	public void testRetry005() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteThreadSize(10);
		config.setWriteBatchSize(4);
		try (Connection conn = buildConnection()) {
			String tableName = "holo_client_retry_004";
			String dropSql = "drop table if exists " + tableName + ";drop user if exists \"BASIC$test\"";
			String createSql = "create table " + tableName + "(id int8 not null,name text,primary key(id));CREATE USER \"BASIC$test\" WITH PASSWORD 'test'; ";

			execute(conn, new String[]{dropSql, createSql});

			config.setUsername("BASIC$test");
			config.setPassword("test");
			try (HoloClient client = new HoloClient(config)) {
				TableSchema schema = client.getTableSchema(tableName);
				Assert.assertThrows(HoloClientException.class, () -> {
					try {
						Put put = new Put(schema);
						put.setObject(0, 0);
						put.setObject(1, "aaaa");
						client.put(put);
						client.flush();
					} catch (HoloClientWithDetailsException e) {
						e.printStackTrace();
					}
				});
				Metrics.reporter().report();
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}
}
