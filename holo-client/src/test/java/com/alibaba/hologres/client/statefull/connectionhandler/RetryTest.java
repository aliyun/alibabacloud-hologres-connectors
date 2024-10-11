/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.statefull.connectionhandler;

import com.alibaba.hologres.client.Get;
import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloClientTestBase;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.exception.HoloClientWithDetailsException;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.hologres.client.utils.Metrics;
import org.testng.Assert;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;
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
						Assert.assertTrue(false, e.getClass().getName() + ":" + e.getMessage());
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
					Assert.assertTrue(false, e.getClass().getName() + ":" + e.getMessage());
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
	@Ignore
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
						Assert.assertTrue(false, e.getClass().getName() + ":" + e.getMessage());
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
					Assert.assertTrue(false, e.getClass().getName() + ":" + e.getMessage());
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
	 * readonly，多点重试，都会写入成功,貌似readonly函数出bug了.
	 * Method: put(Put put).
	 * HoloClientWithDetailsException一般指脏数据，又调用方决定跳过还是终止人工干预;
	 * HoloClientException一般是故障了，就应该停止
	 */
	@Ignore
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
						Assert.assertTrue(false, e.getClass().getName() + ":" + e.getMessage());
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
					Assert.assertTrue(false, e.getClass().getName() + ":" + e.getMessage());
				} catch (HoloClientException e) {
					Assert.assertTrue(false, e.getClass().getName() + ":" + e.getMessage());
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
				while (true) {
					try {
						execute(conn, new String[]{stopReadOnly, dropSql});
						break;
					} catch (SQLException e) {
						LOG.warn("", e);
					}
				}

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
		config.setRetryCount(6);
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
						Assert.assertTrue(false, e.getClass().getName() + ":" + e.getMessage());
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
						Assert.assertTrue(false, e.getClass().getName() + ":" + e.getMessage());
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
					Assert.assertTrue(false, e.getClass().getName() + ":" + e.getMessage());
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
	@Ignore
	@Test
	public void testRetry005() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteThreadSize(10);
		config.setWriteBatchSize(4);
		try (Connection conn = buildConnection()) {
			String tableName = "holo_client_retry_005";
			String dropSql = "drop table if exists " + tableName + ";drop user if exists \"BASIC$test\"";
			String createSql = "create table " + tableName + "(id int8 not null,name text,primary key(id));CREATE USER \"BASIC$test\" WITH PASSWORD 'test'; ";

			execute(conn, new String[]{dropSql, createSql});

			config.setUsername("BASIC$test");
			config.setPassword("test");

			//独立账号的变化， 1.3 gateway需要点时间去同步
			Thread.sleep(5000L);

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
				try {
					execute(conn, new String[]{dropSql});
				} catch (SQLException e) {
					LOG.warn("", e);
				}
			}
		}
	}

	/**
	 * TABLE_NOT_FOUND，脏数据，非分区表.
	 * Method: put(Put put).
	 */
	@Test
	public void testRetry006() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteThreadSize(10);
		config.setWriteBatchSize(4);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_retry_006";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int8 not null,name text,primary key(id))";

			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);
				execute(conn, new String[]{dropSql});
				for (int i = 0; i < 5; ++i) {
					Put put = new Put(schema);
					put.setObject(0, i);
					put.setObject(1, i);
					client.put(put);
				}
				client.flush();
			} catch (HoloClientWithDetailsException e) {
				Assert.assertEquals(5, e.size());
			} catch (Exception e) {
				Assert.assertTrue(false, e.getMessage());
			}
			execute(conn, new String[]{dropSql});
		}
	}

	/**
	 * 插入分区表，不符合要求的应该算脏数据.
	 * Method: put(Put put).
	 * 必须要多个few的实例才可能抛异常进而出发retry！！！
	 */
	@Test
	public void testRetry007() throws Exception {
		HoloConfig config = buildConfig();
		config.setWriteThreadSize(10);
		config.setWriteBatchSize(4);
		try (Connection conn = buildConnection()) {
			String tableName = "holo_client_retry_007";
			String childTableName = "holo_client_retry_007_a";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int8 not null,name text,ds text not null, primary key(id,ds)) partition by list(ds);create table " + childTableName + " partition of " + tableName + " for values in ('a')";

			execute(conn, new String[]{dropSql, createSql});

			try (HoloClient client = new HoloClient(config)) {
				TableSchema schema = client.getTableSchema(childTableName);
				{
					Put put = new Put(schema);
					put.setObject(0, 1);
					put.setObject(1, "name0");
					put.setObject(2, "b");
					client.put(put);
				}
				{
					Put put = new Put(schema);
					put.setObject(0, 2);
					put.setObject(1, "name0");
					put.setObject(2, "a");
					client.put(put);
				}

				int dirtySize = 0;
				try {
					client.flush();
				} catch (HoloClientWithDetailsException e) {
					dirtySize = e.size();
				}
				Assert.assertEquals(1, dirtySize);
				Assert.assertNull(client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).setPrimaryKey("ds", "a").build()).get());
				Assert.assertNotNull(client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).setPrimaryKey("ds", "a").build()).get());
				Assert.assertNull(client.get(Get.newBuilder(schema).setPrimaryKey("id", 3).setPrimaryKey("ds", "a").build()).get());
				Metrics.reporter().report();
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * get retry
	 * Method: Get(Get get).
	 * 必须要多个few的实例才可能抛异常进而出发retry！！！
	 */
	@Test
	public void testRetry008() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setAppName("testRetry008");
		config.setReadRetryCount(5);
		config.setReadThreadSize(2);
		config.setReadTimeoutMilliseconds(0);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.holo_client_retry_008";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text, primary key(id))";
			String insertSql = "insert into " + tableName + " values(0, 'name0')";
			execute(conn, new String[]{createSchema, dropSql, createSql, insertSql});

			TableSchema schema = client.getTableSchema(tableName);
			try {
				AtomicBoolean failed = new AtomicBoolean(false);
				ExecutorService es = new ThreadPoolExecutor(20, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(20), r -> {
					Thread t = new Thread(r);
					return t;
				}, new ThreadPoolExecutor.AbortPolicy());
				// 多线程执行get同时alter table, 没有重试的情况会抛出异常，测试失败
				Runnable runnable = () -> {
					try {
						for (int i = 0; i < 10; ++i) {
							Thread.sleep(10);
							Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
							Assert.assertEquals("name0", r.getObject("name"));
						}
					} catch (ExecutionException | HoloClientException | InterruptedException e) {
						e.printStackTrace();
						// 没有重试成功
						failed.set(true);
					}
				};
				// alter table
				Runnable alterJob = () -> {
					try {
						for (int i = 0; i < 10; ++i) {
							Thread.sleep(10);
							execute(conn, new String[]{"call set_table_property('" + tableName + "','time_to_live_in_seconds','" + ((i + 1) * 86400) + "')"});
						}
					} catch (Exception e) {
						e.printStackTrace();
						Assert.assertTrue(false, e.getClass().getName() + ":" + e.getMessage());
					}
				};

				for (int i = 0; i < 10; ++i) {
					es.execute(runnable);
					es.execute(alterJob);
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
				org.testng.Assert.assertEquals(index, count);
			} finally {
				execute(conn, new String[]{dropSql});
			}

		}
	}
}
