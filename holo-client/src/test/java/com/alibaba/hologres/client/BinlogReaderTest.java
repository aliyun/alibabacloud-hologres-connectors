/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.binlog.BinlogOffset;
import com.alibaba.hologres.client.impl.binlog.HoloBinlogDecoder;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.hologres.client.model.binlog.BinlogRecord;
import com.alibaba.hologres.client.utils.DataTypeTestUtil;
import org.postgresql.PGProperty;
import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.hologres.client.utils.DataTypeTestUtil.FIXED_PLAN_TYPE_DATA_WITH_RECORD;

/**
 * Binlog Decoder 测试.
 * holo实例需要大于1.1.2版本.
 * 测试数据库需已经开启create extension roaringbitmap.
 */
public class BinlogReaderTest extends HoloClientTestBase {
	public static final Logger LOG = LoggerFactory.getLogger(BinlogReaderTest.class);
	HoloVersion needVersion = new HoloVersion(1, 1, 2);

	/**
	 * binlogGroupShardReader.
	 * binlogDecoder.
	 */
	@Test
	public void binlogReader002() throws Exception {
		if (properties == null) {
			return;
		}
		if (holoVersion.compareTo(needVersion) < 0) {
			if (holoVersion.compareTo(needVersion) < 0) {
				LOG.info("Skip BinlogReaderTest because holo version is {} < 1.1.2", holoVersion);
			}
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteThreadSize(5);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_binlog_decoder_002";

			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '1');\n";

			execute(conn, new String[]{dropSql, "begin;", createSql, "commit;"});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < 5; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("amount", "16.211");
					put2.setObject("t", "abc,d");
					if (i == 2) {
						put2.setObject("t", null);
					} else if (i == 3) {
						put2.setObject("t", "NULL");
					}
					put2.setObject("ts", "2021-04-12 12:12:12");
					put2.setObject("ba", new byte[]{(byte) (i % 128)});
					put2.setObject("t_a", new String[]{"a", "b,c"});
					put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
					client.put(put2);
				}
				client.flush();

				// 使用新的Properties创建 REPLICATION Connection
				Properties info = new Properties();
				// 创建JDBC连接
				PGProperty.USER.set(info, properties.getProperty("user"));
				PGProperty.PASSWORD.set(info, properties.getProperty("password"));
				PGProperty.ASSUME_MIN_SERVER_VERSION.set(info, "9.4");
				// 消费Binlog，务必加上以下参数
				PGProperty.REPLICATION.set(info, "database");

				try (Connection conne2 = DriverManager.getConnection(properties.getProperty("url"), info)) {

					try (PgConnection pgConnection = conne2.unwrap(PgConnection.class)) {

						// 创建PGReplicationStream并绑定Replicaiton slot
						PGReplicationStream pgReplicationStream =
								pgConnection
										.getReplicationAPI()
										.replicationStream()
										.logical()
										.withSlotOption("table_name", tableName)
										.withSlotOption("parallel_index", 0)
										.withSlotOption("batch_size", "5")
										.start();

						HoloBinlogDecoder decoder = new HoloBinlogDecoder(schema);

						ByteBuffer byteBuffer = pgReplicationStream.readPending();

						while (byteBuffer == null) {
							byteBuffer = pgReplicationStream.readPending();
						}
						long lastLsn = 1L;
						List<BinlogRecord> records = decoder.decode(-1, byteBuffer);
						for (BinlogRecord record : records) {
							lastLsn = record.getBinlogLsn();
							System.out.println("record: " + Arrays.toString(record.getValues()));
							pgReplicationStream.setFlushedLSN(LogSequenceNumber.valueOf(lastLsn));
						}
						Assert.assertEquals(5, records.size());
						pgReplicationStream.forceUpdateStatus();
						pgReplicationStream.close();
					}
				}
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
	 * binlogGroupShardReader.
	 * 读取1000条binlog时间
	 */
	@Test
	public void binlogReader003() throws Exception {
		if (properties == null || holoVersion.compareTo(needVersion) < 0) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setBinlogReadBatchSize(128);

		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_binlog_reader_003";

			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '3');\n";
			execute(conn, new String[]{dropSql});
			execute(conn, new String[]{"begin;", createSql, "commit;"});

			BinlogShardGroupReader reader = null;

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < 1000; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("amount", "16.211");
					put2.setObject("t", "abc,d");
					if (i == 2) {
						put2.setObject("t", null);
					} else if (i == 3) {
						put2.setObject("t", "NULL");
					}
					put2.setObject("ts", "2021-04-12 12:12:12");
					put2.setObject("ba", new byte[]{(byte) (i % 128)});
					put2.setObject("t_a", new String[]{"a", "b,c"});
					put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
					client.put(put2);
				}
				client.flush();

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName).build());

				long start = System.nanoTime();
				int count = 0;
				while (reader.getBinlogRecord() != null) {
					count++;
					if (count == 1000) {
						reader.cancel();
						break;
					}
				}
				LOG.info("reader cancel");

				long end = System.nanoTime();
				LOG.info("Binlog reader count: {}, cost: {} ms", count, (end - start) / 1000000L);

				Assert.assertEquals(count, 1000);

			} finally {
				if (reader != null) {
					reader.cancel();
				}
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * binlogGroupShardReader.
	 * 指定start time
	 */
	@Test
	public void binlogReader004() throws Exception {
		if (properties == null || holoVersion.compareTo(needVersion) < 0) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setBinlogReadBatchSize(128);
		int shardCount = 3;

		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_binlog_reader_004";

			String dropSql1 = "drop table if exists " + tableName;
			String createSql = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '" + shardCount + "');\n";
			execute(conn, new String[]{dropSql1});
			execute(conn, new String[]{"begin;", createSql, "commit;"});

			BinlogShardGroupReader reader = null;

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < 1000; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("amount", "16.211");
					put2.setObject("t", "abc,d");
					if (i == 2) {
						put2.setObject("t", null);
					} else if (i == 3) {
						put2.setObject("t", "NULL");
					}
					put2.setObject("ts", "2021-04-12 12:12:12");
					put2.setObject("ba", new byte[]{(byte) (i % 128)});
					put2.setObject("t_a", new String[]{"a", "b,c"});
					put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
					client.put(put2);
				}
				client.flush();

				Thread.sleep(1000L);
				Timestamp now = new Timestamp(System.currentTimeMillis());

				for (int i = 1000; i < 2000; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("amount", "16.211");
					put2.setObject("t", "abc,d");
					if (i == 2) {
						put2.setObject("t", null);
					} else if (i == 3) {
						put2.setObject("t", "NULL");
					}
					put2.setObject("ts", "2021-04-12 12:12:12");
					put2.setObject("ba", new byte[]{(byte) (i % 128)});
					put2.setObject("t_a", new String[]{"a", "b,c"});
					put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
					client.put(put2);
				}
				client.flush();

				Map<Integer, Long> lsnExpects = new ConcurrentHashMap<>(shardCount);
				try (Statement stat = conn.createStatement()) {
					ResultSet rs = stat.executeQuery("select hg_shard_id, max(hg_binlog_lsn) from " + tableName + " group by hg_shard_id;");
					while (rs.next()) {
						lsnExpects.put(rs.getInt(1), rs.getLong(2));
					}
				}

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName).setBinlogReadStartTime(now.toString()).build());
				Map<Integer, Long> lsnResults = new ConcurrentHashMap<>(shardCount);
				int count = 0;
				BinlogRecord r;
				while ((r = reader.getBinlogRecord()) != null) {
					count++;
					lsnResults.put(r.getShardId(), r.getBinlogLsn());
					if (count == 1000) {
						reader.cancel();
						break;
					}
				}
				LOG.info("reader cancel");

				Assert.assertEquals(count, 1000);
				Assert.assertEquals(lsnExpects, lsnResults);

			} finally {
				if (reader != null) {
					reader.cancel();
				}
				execute(conn, new String[]{dropSql1});
			}
		}
	}

	/**
	 * binlogGroupShardReader.
	 * 读取100000条binlog时间
	 */
	@Test
	public void binlogReader005() throws Exception {
		if (properties == null || holoVersion.compareTo(needVersion) < 0) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteThreadSize(10);
		config.setBinlogReadBatchSize(512);

		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_binlog_reader_005";

			String dropSql1 = "drop table if exists " + tableName;
			String createSql = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '10');\n";
			execute(conn, new String[]{dropSql1});
			execute(conn, new String[]{"begin;", createSql, "commit;"});
			BinlogShardGroupReader reader = null;

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < 100000; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("amount", "16.211");
					put2.setObject("t", "abc,d");
					if (i == 2) {
						put2.setObject("t", null);
					} else if (i == 3) {
						put2.setObject("t", "NULL");
					}
					put2.setObject("ts", "2021-04-12 12:12:12");
					put2.setObject("ba", new byte[]{(byte) (i % 128)});
					put2.setObject("t_a", new String[]{"a", "b,c"});
					put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
					client.put(put2);
				}
				client.flush();

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName).build());

				long start = System.nanoTime();
				int count = 0;
				while (reader.getBinlogRecord() != null) {
					count++;
					if (count == 100000) {
						reader.cancel();
						break;
					}
				}
				LOG.info("reader cancel");

				long end = System.nanoTime();
				LOG.info("Binlog reader count: {}, cost: {} ms", count, (end - start) / 1000000L);

				Assert.assertEquals(count, 100000);

			} finally {
				if (reader != null) {
					reader.cancel();
				}
				execute(conn, new String[]{dropSql1});
			}
		}
	}

	/**
	 * binlogGroupShardReader.
	 * update and binlogIgnoreBeforeUpdate=false
	 */
	@Test
	public void binlogReader007() throws Exception {
		if (properties == null || holoVersion.compareTo(needVersion) < 0) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setBinlogReadBatchSize(128);

		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_binlog_reader_007";
			String dropSql1 = "drop table if exists " + tableName;
			String createSql = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '3');\n";
			execute(conn, new String[]{dropSql1});
			execute(conn, new String[]{"begin;", createSql, "commit;"});
			BinlogShardGroupReader reader = null;

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < 1000; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("amount", "16.211");
					put2.setObject("t", "abc,d");
					if (i == 2) {
						put2.setObject("t", null);
					} else if (i == 3) {
						put2.setObject("t", "NULL");
					}
					put2.setObject("ts", "2021-04-12 12:12:12");
					put2.setObject("ba", new byte[]{(byte) (i % 128)});
					put2.setObject("t_a", new String[]{"a", "b,c"});
					put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
					client.put(put2);
				}
				client.flush();

				// update 1000条数据，update操作会产生"BEFORE_UPDATE"和"AFTER_UPDATE"两条binlog；一共会有3000条binlog
				for (int i = 0; i < 1000; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("amount", "16.211");
					put2.setObject("t", "abc,d");
					if (i == 2) {
						put2.setObject("t", null);
					} else if (i == 3) {
						put2.setObject("t", "NULL");
					}
					put2.setObject("ts", "2021-04-12 12:12:12");
					put2.setObject("ba", new byte[]{(byte) (i % 128)});
					put2.setObject("t_a", new String[]{"a", "b,c"});
					put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
					client.put(put2);
				}
				client.flush();

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName).build());

				long start = System.nanoTime();
				int count = 0;
				while (reader.getBinlogRecord() != null) {
					count++;
					if (count == 3000) {
						//TODO why?
						reader.cancel();
					}
					if (reader.isCanceled()) {
						break;
					}
				}
				LOG.info("reader cancel");

				long end = System.nanoTime();
				LOG.info("Binlog reader count: {}, cost: {} ms", count, (end - start) / 1000000L);

				Assert.assertEquals(count, 3000);

			} finally {
				if (reader != null) {
					reader.cancel();
				}
				execute(conn, new String[]{dropSql1});
			}
		}
	}

	/**
	 * binlogGroupShardReader.
	 * update and binlogIgnoreBeforeUpdate=true
	 */
	@Test
	public void binlogReader011() throws Exception {
		if (properties == null || holoVersion.compareTo(needVersion) < 0) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setBinlogReadBatchSize(128);
		config.setBinlogIgnoreBeforeUpdate(true);

		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_binlog_reader_011";

			String dropSql1 = "drop table if exists " + tableName;
			String createSql = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '3');\n";
			execute(conn, new String[]{dropSql1});

			execute(conn, new String[]{"begin;", createSql, "commit;"});
			BinlogShardGroupReader reader = null;

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < 1000; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("amount", "16.211");
					put2.setObject("t", "abc,d");
					if (i == 2) {
						put2.setObject("t", null);
					} else if (i == 3) {
						put2.setObject("t", "NULL");
					}
					put2.setObject("ts", "2021-04-12 12:12:12");
					put2.setObject("ba", new byte[]{(byte) (i % 128)});
					put2.setObject("t_a", new String[]{"a", "b,c"});
					put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
					client.put(put2);
				}
				client.flush();

				// update 1000条数据，update操作会产生"BEFORE_UPDATE"和"AFTER_UPDATE"两条binlog；一共会有3000条binlog， binlogIgnoreBeforeUpdate=true，只读取2000条
				for (int i = 0; i < 1000; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("amount", "16.211");
					put2.setObject("t", "abc,d");
					if (i == 2) {
						put2.setObject("t", null);
					} else if (i == 3) {
						put2.setObject("t", "NULL");
					}
					put2.setObject("ts", "2021-04-12 12:12:12");
					put2.setObject("ba", new byte[]{(byte) (i % 128)});
					put2.setObject("t_a", new String[]{"a", "b,c"});
					put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
					client.put(put2);
				}
				client.flush();

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName).build());

				int count = 0;
				while (reader.getBinlogRecord() != null) {
					count++;
					if (count == 2000) {
						// 延时关闭，测试是否仍然读取到新的binlog
						//TODO why?
						reader.cancel();
					}
					if (reader.isCanceled()) {
						break;
					}
				}
				LOG.info("reader cancel");

				Assert.assertEquals(count, 2000);

			} finally {
				if (reader != null) {
					reader.cancel();
				}
				execute(conn, new String[]{dropSql1});
			}
		}
	}

	/**
	 * binlogGroupShardReader.
	 * delete and binlogIgnoreDelete=false
	 */
	@Test
	public void binlogReader013() throws Exception {
		if (properties == null || holoVersion.compareTo(needVersion) < 0) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setBinlogReadBatchSize(128);

		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_binlog_reader_013";

			String dropSql1 = "drop table if exists " + tableName;
			String createSql = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '3');\n";
			String deleteSql = "delete from " + tableName + ";\n";

			execute(conn, new String[]{dropSql1});
			execute(conn, new String[]{"begin;", createSql, "commit;"});
			BinlogShardGroupReader reader = null;

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < 1000; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("amount", "16.211");
					put2.setObject("t", "abc,d");
					if (i == 2) {
						put2.setObject("t", null);
					} else if (i == 3) {
						put2.setObject("t", "NULL");
					}
					put2.setObject("ts", "2021-04-12 12:12:12");
					put2.setObject("ba", new byte[]{(byte) (i % 128)});
					put2.setObject("t_a", new String[]{"a", "b,c"});
					put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
					client.put(put2);
				}
				client.flush();

				// delete 1000条数据，一共会有2000条binlog
				execute(conn, new String[]{deleteSql});

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName).build());

				int count = 0;
				while (reader.getBinlogRecord() != null) {
					count++;
					if (count == 2000) {
						reader.cancel();
					}
					if (reader.isCanceled()) {
						break;
					}
				}

				Assert.assertEquals(count, 2000);

			} finally {
				if (reader != null) {
					reader.cancel();
				}
				execute(conn, new String[]{dropSql1});
			}
		}
	}

	/**
	 * binlogGroupShardReader.
	 * delete and binlogIgnoreDelete=true
	 */
	@Test
	public void binlogReader017() throws Exception {
		if (properties == null || holoVersion.compareTo(needVersion) < 0) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setBinlogReadBatchSize(128);
		config.setBinlogIgnoreDelete(true);

		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_binlog_reader_017";
			String dropSql1 = "drop table if exists " + tableName;
			String createSql = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '3');\n";
			String deleteSql = "delete from " + tableName + ";\n";

			execute(conn, new String[]{dropSql1});
			execute(conn, new String[]{"begin;", createSql, "commit;"});
			BinlogShardGroupReader reader = null;

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < 1000; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("amount", "16.211");
					put2.setObject("t", "abc,d");
					if (i == 2) {
						put2.setObject("t", null);
					} else if (i == 3) {
						put2.setObject("t", "NULL");
					}
					put2.setObject("ts", "2021-04-12 12:12:12");
					put2.setObject("ba", new byte[]{(byte) (i % 128)});
					put2.setObject("t_a", new String[]{"a", "b,c"});
					put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
					client.put(put2);
				}
				client.flush();

				// delete 1000条数据，一共会有2000条binlog; binlogIgnoreDelete=true，只读取1000条
				execute(conn, new String[]{deleteSql});

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName).build());

				int count = 0;
				while (reader.getBinlogRecord() != null) {
					count++;
					if (count == 1000) {
						// 延时关闭，测试是否仍然读取到新的binlog
						reader.cancel();
					}
					if (reader.isCanceled()) {
						break;
					}
				}
				LOG.info("reader cancel");

				Assert.assertEquals(count, 1000);

			} finally {
				if (reader != null) {
					reader.cancel();
				}
				execute(conn, new String[]{dropSql1});
			}
		}
	}

	/**
	 * binlogGroupShardReader.
	 * Alter table: add one new column.
	 */
	@Ignore
	@Test
	public void btinlogReader019() throws Exception {
		if (properties == null || holoVersion.compareTo(needVersion) < 0) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteThreadSize(10);
		config.setBinlogReadBatchSize(8);  // 防止读取太快刷完所有写入的数据

		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_binlog_reader_019";
			String dropSql1 = "drop table if exists " + tableName;
			String createSql = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '3');\n";
			String alterSql = "ALTER TABLE IF EXISTS " + tableName + " ADD COLUMN new_column_1 int;\n";

			execute(conn, new String[]{dropSql1});
			execute(conn, new String[]{"begin;", createSql, "commit;"});
			try {
				try (BinlogShardGroupReader reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName).build())) {
					final CompletableFuture<Void> writeFuture = new CompletableFuture<>();
					new Thread(() -> {
						try {
							TableSchema schema = client.getTableSchema(tableName, true);
							for (int i = 0; i < 10000; ++i) {
								if (i == 2000) {
									client.flush();
									execute(conn, new String[]{alterSql});
									LOG.info("alter table: add one new column");
									schema = client.getTableSchema(tableName, true);
									LOG.info("after alter, schema version:{}", schema.getSchemaVersion());
								}
								Put put2 = new Put(schema);
								put2.setObject("id", i);
								put2.setObject("amount", "16.211");
								put2.setObject("t", "abc,d");
								if (i == 2) {
									put2.setObject("t", null);
								} else if (i == 3) {
									put2.setObject("t", "NULL");
								}
								put2.setObject("ts", "2021-04-12 12:12:12");
								put2.setObject("ba", new byte[]{(byte) (i % 128)});
								put2.setObject("t_a", new String[]{"a", "b,c"});
								put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
								if (i >= 2000) {
									put2.setObject("new_column_1", i);
								}
								client.put(put2);
							}

							client.flush();
							writeFuture.complete(null);
						} catch (Exception e) {
							writeFuture.completeExceptionally(e);
						}
					}).start();
					BinlogRecord record;
					long start = System.nanoTime();
					int count = 0;
					Map<Integer, Long> maxLsn = new HashMap<>();
					Set<Long> set = new HashSet<>();
					while ((record = reader.getBinlogRecord()) != null) {
						if (writeFuture.isDone()) {
							writeFuture.get();
						}
						if (record.isHeartBeat()) {
							continue;
						}
						count++;
						int id = (int) record.getObject("id");
						if (id < 2000) {
							if (record.getSchema().getColumnSchema().length == 8) {
								Assert.assertNull(record.getObject(7));
							}
						} else {
							Assert.assertEquals(record.getSchema().getColumnSchema().length, 8, record.getObject(0) + ":" + record.getClass().getName());
							Assert.assertEquals((int) record.getObject(7), id);
						}

						//shard count不超过100
						Assert.assertTrue(set.add(record.getBinlogLsn() * 100 + record.getShardId()));
						Long lastLsn = maxLsn.put(record.getShardId(), record.getBinlogLsn());
						if (lastLsn != null) {
							Assert.assertTrue(lastLsn < record.getBinlogLsn());
						}
						if (count == 10000) {
							System.out.println(Arrays.toString(record.getValues()));
							reader.cancel();
							break;
						}
					}
					writeFuture.get();

					LOG.info("reader cancel");
					long end = System.nanoTime();
					LOG.info("Binlog reader count: {}, cost: {} ms", count, (end - start) / 1000000L);

					Assert.assertEquals(10000, count);
				}

			} finally {
				execute(conn, new String[]{dropSql1});
			}
		}
	}

	/**
	 * binlogGroupShardReader.
	 * Alter table: drop one column.
	 */
	@Ignore
	@Test
	public void binlogReader021() throws Exception {
		if (properties == null || holoVersion.compareTo(needVersion) < 0) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteThreadSize(10);
		config.setBinlogReadBatchSize(8);  // 防止读取太快刷完所有写入的数据
		config.setBinlogHeartBeatIntervalMs(5000L);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_binlog_reader_021";
			String dropSql1 = "drop table if exists " + tableName;
			String createSql = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '3');\n";
			String alterSql = "set hg_experimental_enable_drop_column=true;\n"
					+ "ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN ba;\n";

			execute(conn, new String[]{dropSql1});

			execute(conn, new String[]{"begin;", createSql, "commit;"});
			try (BinlogShardGroupReader reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName).build())) {
				final CompletableFuture<Void> writeFuture = new CompletableFuture<>();
				new Thread(() -> {
					try {
						TableSchema schema = client.getTableSchema(tableName, true);
						for (int i = 0; i < 10000; ++i) {
							if (i == 2000) {
								client.flush();
								execute(conn, new String[]{alterSql});
								LOG.info("alter table: drop one new column");
								schema = client.getTableSchema(tableName, true);
								LOG.info("after alter, schema version:{}", schema.getSchemaVersion());
							}
							Put put2 = new Put(schema);
							put2.setObject("id", i);
							put2.setObject("amount", "16.211");
							put2.setObject("t", "abc,d");
							if (i == 2) {
								put2.setObject("t", null);
							} else if (i == 3) {
								put2.setObject("t", "NULL");
							}
							put2.setObject("ts", "2021-04-12 12:12:12");
							if (i < 2000) {
								put2.setObject("ba", new byte[]{(byte) (i % 128)});
							}
							put2.setObject("t_a", new String[]{"a", "b,c"});
							put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
							client.put(put2);
						}

						client.flush();
						writeFuture.complete(null);
					} catch (Exception e) {
						writeFuture.completeExceptionally(e);
					}
				}).start();
				BinlogRecord record;
				long start = System.nanoTime();
				int count = 0;
				while ((record = reader.getBinlogRecord()) != null) {
					if (writeFuture.isDone()) {
						writeFuture.get();
					}
					if (record.isHeartBeat()) {
						continue;
					}
					count++;
					int id = (int) record.getObject("id");
					if (id >= 2000) {
						Assert.assertEquals(6, record.getSchema().getColumnSchema().length);
					}
					if (count == 10000) {
						System.out.println(Arrays.toString(record.getValues()));
						reader.cancel();
						break;
					}
				}
				LOG.info("reader cancel");

				long end = System.nanoTime();
				LOG.info("Binlog reader count: {}, cost: {} ms", count, (end - start) / 1000000L);

				Assert.assertEquals(10000, count);

			} finally {
				execute(conn, new String[]{dropSql1});
			}
		}
	}

	/**
	 * binlogGroupShardReader.
	 * 单shard表 start_lsn 测试
	 */
	@Test
	public void binlogReader023() throws Exception {
		if (properties == null || holoVersion.compareTo(needVersion) < 0) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setBinlogReadBatchSize(128);

		long lsn = 0;
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_binlog_reader_023";

			String dropSql1 = "drop table if exists " + tableName;
			String createSql = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '1');\n";
			execute(conn, new String[]{dropSql1});

			execute(conn, new String[]{"begin;", createSql, "commit;"});
			BinlogShardGroupReader reader = null;

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < 1000; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("amount", "16.211");
					put2.setObject("t", "abc,d");
					if (i == 2) {
						put2.setObject("t", null);
					} else if (i == 3) {
						put2.setObject("t", "NULL");
					}
					put2.setObject("ts", "2021-04-12 12:12:12");
					put2.setObject("ba", new byte[]{(byte) (i % 128)});
					put2.setObject("t_a", new String[]{"a", "b,c"});
					put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
					client.put(put2);
				}
				client.flush();

				long lsnExpect = -1;

				try (Statement stat = conn.createStatement()) {
					ResultSet rs = stat.executeQuery("select hg_shard_id, max(hg_binlog_lsn) from " + tableName + " group by hg_shard_id;");
					while (rs.next()) {
						lsnExpect = rs.getLong(2);
					}
				}

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName).build());

				int count = 0;
				BinlogRecord record;
				while ((record = reader.getBinlogRecord()) != null) {
					count++;
					if (count == 500) {
						lsn = record.getBinlogLsn();
						reader.cancel();
						break;
					}
				}
				LOG.info("reader cancel");

				Assert.assertEquals(count, 500);

				int shardCount = 1;
				Map<Integer, BinlogOffset> offsetMap = new HashMap<>(shardCount);
				for (int i = 0; i < shardCount; i++) {
					offsetMap.put(i, new BinlogOffset().setSequence(lsn));
				}
				try (HoloClient client2 = new HoloClient(config)) {
					Subscribe.OffsetBuilder builder = Subscribe.newOffsetBuilder(tableName);
					for (Map.Entry<Integer, BinlogOffset> entry : offsetMap.entrySet()) {
						builder.addShardStartOffset(entry.getKey(), entry.getValue());
					}
					reader = client2.binlogSubscribe(builder.build());

					while ((record = reader.getBinlogRecord()) != null) {
						count++;
						if (count == 1000) {
							lsn = record.getBinlogLsn();
							reader.cancel();
							break;
						}
					}
					LOG.info("reader cancel");

					Assert.assertEquals(lsnExpect, lsn);
					Assert.assertEquals(count, 1000);
				}
			} finally {
				if (reader != null) {
					reader.cancel();
				}
				execute(conn, new String[]{dropSql1});
			}
		}
	}

	/**
	 * binlogGroupShardReader.
	 * 多shard表 BinlogOffset 仅设置timestamp 测试
	 */
	@Test
	public void binlogReader028() throws Exception {
		if (properties == null || holoVersion.compareTo(needVersion) < 0) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setBinlogReadBatchSize(128);
		int shardCount = 3;

		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_binlog_reader_028";

			String dropSql1 = "drop table if exists " + tableName;

			String createSql = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], "
					+ "primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '" + shardCount + "');\n";

			execute(conn, new String[]{dropSql1});
			execute(conn, new String[]{"begin;", createSql, "commit;"});
			BinlogShardGroupReader reader = null;

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < 1000; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("amount", "16.211");
					put2.setObject("t", "abc,d");
					if (i == 2) {
						put2.setObject("t", null);
					} else if (i == 3) {
						put2.setObject("t", "NULL");
					}
					put2.setObject("ts", "2021-04-12 12:12:12");
					put2.setObject("ba", new byte[]{(byte) (i % 128)});
					put2.setObject("t_a", new String[]{"a", "b,c"});
					put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
					client.put(put2);
				}
				client.flush();

				Thread.sleep(1000L);
				Timestamp now = new Timestamp(System.currentTimeMillis());

				for (int i = 1000; i < 2000; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("amount", "16.211");
					put2.setObject("t", "abc,d");
					if (i == 2) {
						put2.setObject("t", null);
					} else if (i == 3) {
						put2.setObject("t", "NULL");
					}
					put2.setObject("ts", "2021-04-12 12:12:12");
					put2.setObject("ba", new byte[]{(byte) (i % 128)});
					put2.setObject("t_a", new String[]{"a", "b,c"});
					put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
					client.put(put2);
				}
				client.flush();

				Map<Integer, Long> lsnExpects = new ConcurrentHashMap<>(shardCount);
				try (Statement stat = conn.createStatement()) {
					ResultSet rs = stat.executeQuery(
							"select hg_shard_id, max(hg_binlog_lsn) from " + tableName + " group by hg_shard_id;");
					while (rs.next()) {
						lsnExpects.put(rs.getInt(1), rs.getLong(2));
					}
				}

				Subscribe.OffsetBuilder offsetBuilder = Subscribe.newOffsetBuilder(tableName);
				for (int i = 0; i < shardCount; i++) {
					// BinlogOffset 的sequence等于-1但timestamp有效时，根据timestamp开始消费
					offsetBuilder.addShardStartOffset(i, new BinlogOffset(-1, now.getTime() * 1000L));
				}
				reader = client.binlogSubscribe(offsetBuilder.build());
				Map<Integer, Long> lsnResults = new ConcurrentHashMap<>(shardCount);
				int count = 0;
				BinlogRecord r;
				while ((r = reader.getBinlogRecord()) != null) {
					count++;
					lsnResults.put(r.getShardId(), r.getBinlogLsn());
					if (count == 1000) {
						reader.cancel();
						break;
					}
				}
				LOG.info("reader cancel");

				Assert.assertEquals(count, 1000);
				Assert.assertEquals(lsnExpects, lsnResults);

			} finally {
				if (reader != null) {
					reader.cancel();
				}
				execute(conn, new String[]{dropSql1});
			}
		}
	}

	/**
	 * binlogGroupShardReader.
	 * 多shard表 BinlogOffset 测试
	 */
	@Test
	public void binlogReader029() throws Exception {
		if (properties == null || holoVersion.compareTo(needVersion) < 0) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setBinlogReadBatchSize(128);

		int shardCount = 5;
		Map<Integer, BinlogOffset> offsetMap = new HashMap<>(shardCount);
		for (int i = 0; i < shardCount; i++) {
			offsetMap.put(i, new BinlogOffset());
		}
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_binlog_reader_029";
			String dropSql1 = "drop table if exists " + tableName;
			String createSql = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '" + shardCount + "');\n";
			execute(conn, new String[]{dropSql1});

			execute(conn, new String[]{"begin;", createSql, "commit;"});
			BinlogShardGroupReader reader = null;

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < 50000; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("amount", "16.211");
					put2.setObject("t", "abc,d");
					if (i == 2) {
						put2.setObject("t", null);
					} else if (i == 3) {
						put2.setObject("t", "NULL");
					}
					put2.setObject("ts", "2021-04-12 12:12:12");
					put2.setObject("ba", new byte[]{(byte) (i % 128)});
					put2.setObject("t_a", new String[]{"a", "b,c"});
					put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
					client.put(put2);
				}
				client.flush();

				Map<Integer, Long> lsnExpects = new ConcurrentHashMap<>(shardCount);
				try (Statement stat = conn.createStatement()) {
					ResultSet rs = stat.executeQuery("select hg_shard_id, max(hg_binlog_lsn) from " + tableName + " group by hg_shard_id;");
					while (rs.next()) {
						lsnExpects.put(rs.getInt(1), rs.getLong(2));
					}
				}

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName).build());

				int count = 0;
				BinlogRecord record;
				while ((record = reader.getBinlogRecord()) != null) {
					count++;
					// offsetMap中保存每个shard消费到的最后一条binlog的lsn、timestamp
					offsetMap.replace(record.getShardId(), new BinlogOffset(record.getBinlogLsn(), record.getBinlogTimestamp()));
					if (count == 25000) {
						System.out.println("binlog offset map have saved: " + offsetMap);
						reader.cancel();
						break;
					}
				}
				LOG.info("reader cancel");

				Assert.assertEquals(count, 25000);

				try (HoloClient client2 = new HoloClient(config)) {
					Subscribe.OffsetBuilder builder = Subscribe.newOffsetBuilder(tableName);
					for (Map.Entry<Integer, BinlogOffset> entry : offsetMap.entrySet()) {
						builder.addShardStartOffset(entry.getKey(), entry.getValue());
					}
					reader = client2.binlogSubscribe(builder.build());

					while ((record = reader.getBinlogRecord()) != null) {
						count++;
						offsetMap.replace(record.getShardId(), new BinlogOffset(record.getBinlogLsn(), record.getBinlogTimestamp()));
						if (count == 50000) {
							reader.cancel();
							break;
						}
					}
					LOG.info("reader cancel");

					Assert.assertEquals(count, 50000);
					for (int i = 0; i < shardCount; i++) {
						Assert.assertEquals(lsnExpects.get(i), Long.valueOf(offsetMap.get(i).getSequence()));
					}
				}
			} finally {
				if (reader != null) {
					reader.cancel();
				}
				execute(conn, new String[]{dropSql1});
			}
		}
	}

	/**
	 * binlogGroupShardReader.
	 * 多shard表 BinlogOffset 测试, 从holo保存的位点恢复
	 */
	@Test
	public void binlogReader030() throws Exception {
		if (properties == null || holoVersion.compareTo(needVersion) < 0) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setBinlogReadBatchSize(128);

		int shardCount = 5;
		Map<Integer, BinlogOffset> offsetMap = new HashMap<>(shardCount);
		for (int i = 0; i < shardCount; i++) {
			offsetMap.put(i, new BinlogOffset());
		}
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_binlog_reader_030";
			String publicationName = "holo_client_binlog_reader_030_publication_test";
			String slotName = "holo_client_binlog_reader_030_slot_1";

			String dropSql1 = "drop table if exists " + tableName + "; drop publication if exists " + publicationName + ";\n";
			String dropSql2 = "delete from hologres.hg_replication_progress where slot_name='" + slotName + "';\n";
			String dropSql3 = "call hg_drop_logical_replication_slot('" + slotName + "');";
			String createSql2 = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '" + shardCount + "');\n";
			String createSql3 = "create publication " + publicationName + " for table " + tableName + ";\n";
			String createSql4 = "call hg_create_logical_replication_slot('" + slotName + "', 'hgoutput', '" + publicationName + "');\n";

			execute(conn, new String[]{dropSql1});
			tryExecute(conn, new String[]{dropSql2, dropSql3});
			execute(conn, new String[]{"begin;", createSql2, "commit;", createSql3});
			execute(conn, new String[]{createSql4});

			BinlogShardGroupReader reader = null;

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < 50000; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("amount", "16.211");
					put2.setObject("t", "abc,d");
					if (i == 2) {
						put2.setObject("t", null);
					} else if (i == 3) {
						put2.setObject("t", "NULL");
					}
					put2.setObject("ts", "2021-04-12 12:12:12");
					put2.setObject("ba", new byte[]{(byte) (i % 128)});
					put2.setObject("t_a", new String[]{"a", "b,c"});
					put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
					client.put(put2);
				}
				client.flush();

				Map<Integer, Long> lsnExpects = new ConcurrentHashMap<>(shardCount);
				try (Statement stat = conn.createStatement()) {
					ResultSet rs = stat.executeQuery("select hg_shard_id, max(hg_binlog_lsn) from " + tableName + " group by hg_shard_id;");
					while (rs.next()) {
						lsnExpects.put(rs.getInt(1), rs.getLong(2));
					}
					System.out.println("binlog lsnExpects: " + lsnExpects);

				}

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName, slotName).build());

				int count = 0;
				BinlogRecord record;
				while ((record = reader.getBinlogRecord()) != null) {
					count++;
					// offsetMap中保存每个shard消费到的最后一条binlog的lsn、timestamp
					offsetMap.replace(record.getShardId(), new BinlogOffset(record.getBinlogLsn(), record.getBinlogTimestamp()));
					if (count == 25000) {
						System.out.println("binlog offset map have saved: " + offsetMap);
						// 手动提交每个shard最新的lsn
						for (int shardId : offsetMap.keySet()) {
							reader.commitFlushedLsn(shardId, offsetMap.get(shardId).getSequence(), 5000L);
						}
						Thread.sleep(20000);
						reader.cancel();
						break;
					}
				}
				LOG.info("reader cancel");

				Assert.assertEquals(count, 25000);

				try (HoloClient client2 = new HoloClient(config)) {
					// 不设置offsetMap，config也未设置startTime，因此从holo保存的消费位点启动
					reader = client2.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName, slotName).build());
					// Committer的lsn初始化为-1，调用getBinlogRecord之前进行commit, -1不能被flush到holo
					reader.commit(5000L);
					reader.cancel();
					// 从holo保存的消费位点启动, 验证上方commit是否将-1错误的写入holo
					reader = client2.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName, slotName).build());
					while ((record = reader.getBinlogRecord()) != null) {
						count++;
						offsetMap.replace(record.getShardId(), new BinlogOffset(record.getBinlogLsn(), record.getBinlogTimestamp()));
						if (count == 50000) {
							reader.cancel();
							break;
						}
					}
					LOG.info("reader cancel");

					Assert.assertEquals(count, 50000);
					for (int i = 0; i < shardCount; i++) {
						Assert.assertEquals(lsnExpects.get(i), Long.valueOf(offsetMap.get(i).getSequence()));
					}
				}
			} finally {
				if (reader != null) {
					reader.cancel();
				}
				tryExecute(conn, new String[]{dropSql1});
			}
		}
	}

	/**
	 * binlogGroupShardReader.
	 * 用户自定义schema下的表
	 */
	@Test
	public void binlogReader031() throws Exception {
		if (properties == null || holoVersion.compareTo(needVersion) < 0) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setBinlogReadBatchSize(128);

		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "new_schema.holo_client_binlog_reader_031";
			String dropSql1 = "drop table if exists " + tableName;
			String createSchema = "create schema if not exists new_schema;\n";

			String createSql = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '3');\n";
			execute(conn, new String[]{dropSql1});

			execute(conn, new String[]{createSchema, "begin;", createSql, "commit;"});
			BinlogShardGroupReader reader = null;

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < 1000; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("amount", "16.211");
					put2.setObject("t", "abc,d");
					if (i == 2) {
						put2.setObject("t", null);
					} else if (i == 3) {
						put2.setObject("t", "NULL");
					}
					put2.setObject("ts", "2021-04-12 12:12:12");
					put2.setObject("ba", new byte[]{(byte) (i % 128)});
					put2.setObject("t_a", new String[]{"a", "b,c"});
					put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
					client.put(put2);
				}
				client.flush();

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName).build());

				long start = System.nanoTime();
				int count = 0;
				while (reader.getBinlogRecord() != null) {
					count++;
					if (count == 1000) {
						reader.cancel();
						break;
					}
				}
				LOG.info("reader cancel");

				long end = System.nanoTime();
				LOG.info("Binlog reader count: {}, cost: {} ms", count, (end - start) / 1000000L);

				Assert.assertEquals(count, 1000);

			} finally {
				if (reader != null) {
					reader.cancel();
				}
				execute(conn, new String[]{dropSql1});
			}
		}
	}

	/**
	 * binlogGroupShardReader.
	 * 单shard表 restart 测试
	 */
	@Test
	public void binlogReader033() throws Exception {
		if (properties == null || holoVersion.compareTo(needVersion) < 0) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setRetryCount(3);
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setBinlogReadBatchSize(128);

		long lsn = 0;
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_binlog_reader_033";
			String dropSql1 = "drop table if exists " + tableName;
			String createSql = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '1');\n";
			execute(conn, new String[]{dropSql1});

			execute(conn, new String[]{"begin;", createSql, "commit;"});
			BinlogShardGroupReader reader = null;
			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < 10000; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("amount", "16.211");
					put2.setObject("t", "abc,d");
					if (i == 2) {
						put2.setObject("t", null);
					} else if (i == 3) {
						put2.setObject("t", "NULL");
					}
					put2.setObject("ts", "2021-04-12 12:12:12");
					put2.setObject("ba", new byte[]{(byte) (i % 128)});
					put2.setObject("t_a", new String[]{"a", "b,c"});
					put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
					client.put(put2);
				}
				client.flush();

				long lsnExpect = -1;

				try (Statement stat = conn.createStatement()) {
					ResultSet rs = stat.executeQuery("select hg_shard_id, max(hg_binlog_lsn) from " + tableName + " group by hg_shard_id;");
					while (rs.next()) {
						lsnExpect = rs.getLong(2);
					}
				}

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName).build());

				int count = 0;
				BinlogRecord record;
				long lastTime = System.currentTimeMillis();

				while (true) {
					long currentTime = System.currentTimeMillis();
					// 读取超时5秒退出
					if (currentTime - lastTime > 5000) {
						System.out.println("break");
						reader.cancel();
						break;
					}
					if ((record = reader.getBinlogRecord()) != null) {
						count++;
						// System.out.println("count: " + count + " shardId: " + record.getShardId() + " record lsn : " + record.getBinlogLsn());

						lsn = record.getBinlogLsn();
						if (count == 3000) {
							reader.commit(5000L);
							//TODO
							// reader.closeShardReader(0);
						}
						if (count == 8000) {
							reader.commit(5000L);
							//TODO
							//reader.closeShardReader(0);
						}
						lastTime = System.currentTimeMillis();

						if (lsn == lsnExpect) {
							break;
						}
					}
				}

				Assert.assertEquals(lsnExpect, lsn);
				Assert.assertTrue(count >= 10000);

			} finally {
				if (reader != null) {
					reader.cancel();
				}
				execute(conn, new String[]{dropSql1});
			}
		}
	}

	/**
	 * binlogGroupShardReader.
	 * 多shard表 restart测试
	 */
	@Test
	public void binlogReader037() throws Exception {
		if (properties == null || holoVersion.compareTo(needVersion) < 0) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteThreadSize(10);
		config.setBinlogReadBatchSize(20);

		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_binlog_reader_037";
			String publicationName = "holo_client_binlog_reader_037_publication_test";
			String slotName = "holo_client_binlog_reader_037_slot_1";

			String dropSql1 = "drop table if exists " + tableName + "; drop publication if exists " + publicationName + ";\n";
			String dropSql2 = "delete from hologres.hg_replication_progress where slot_name='" + slotName + "';\n";
			String dropSql3 = "call hg_drop_logical_replication_slot('" + slotName + "');";
			String createSql2 = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '10');\n";
			String createSql3 = "create publication " + publicationName + " for table " + tableName + ";\n";
			String createSql4 = "call hg_create_logical_replication_slot('" + slotName + "', 'hgoutput', '" + publicationName + "');\n";

			execute(conn, new String[]{dropSql1});
			tryExecute(conn, new String[]{dropSql2, dropSql3});
			execute(conn, new String[]{"begin;", createSql2, "commit;", createSql3});
			execute(conn, new String[]{createSql4});

			BinlogShardGroupReader reader = null;

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < 100000; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("amount", "16.211");
					put2.setObject("t", "abc,d");
					if (i == 2) {
						put2.setObject("t", null);
					} else if (i == 3) {
						put2.setObject("t", "NULL");
					}
					put2.setObject("ts", "2021-04-12 12:12:12");
					put2.setObject("ba", new byte[]{(byte) (i % 128)});
					put2.setObject("t_a", new String[]{"a", "b,c"});
					put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
					client.put(put2);
				}
				client.flush();

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName, slotName).build());

				Thread.sleep(80000L);
				long start = System.nanoTime();
				int count = 0;
				BinlogRecord record;
				LOG.info("begin to getBinlogRecord");
				Set<Long> set = new HashSet<>();
				while ((record = reader.getBinlogRecord()) != null) {
					count++;
					Assert.assertTrue(set.add(record.getBinlogLsn() * 100 + record.getShardId()));
					if (count % 10000 == 0) {
						reader.commit(500000L);
						//reader.closeShardReader(5);
					}
					if (count == 50000) {
						reader.commit(500000L);
						//reader.closeShardReader(5);
					}
					if (count == 100000) {
						reader.cancel();
						break;
					}
				}
				LOG.info("reader cancel");

				long end = System.nanoTime();
				LOG.info("Binlog reader count: {}, cost: {} ms", count, (end - start) / 1000000L);

				Assert.assertEquals(count, 100000);

			} finally {
				if (reader != null) {
					reader.cancel();
				}
				tryExecute(conn, new String[]{dropSql1, dropSql2, dropSql3});
			}
		}
	}

	/**
	 * binlogGroupShardReader.
	 * 实例版本大于2.1时，不需要传入slot
	 */
	@Test
	public void binlogReader041() throws Exception {
		if (properties == null || holoVersion.compareTo(needVersion) < 0) {
			return;
		}

		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setBinlogReadBatchSize(128);

		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "new_schema.holo_client_binlog_reader_041";

			String dropSql1 = "drop table if exists " + tableName;
			String createSchema = "create schema if not exists new_schema;\n";

			String createSql = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '3');\n";

			execute(conn, new String[]{dropSql1});
			execute(conn, new String[]{createSchema, "begin;", createSql, "commit;"});

			BinlogShardGroupReader reader = null;

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < 1000; ++i) {
					Put put2 = new Put(schema);
					put2.setObject("id", i);
					put2.setObject("amount", "16.211");
					put2.setObject("t", "abc,d");
					if (i == 2) {
						put2.setObject("t", null);
					} else if (i == 3) {
						put2.setObject("t", "NULL");
					}
					put2.setObject("ts", "2021-04-12 12:12:12");
					put2.setObject("ba", new byte[]{(byte) (i % 128)});
					put2.setObject("t_a", new String[]{"a", "b,c"});
					put2.setObject("i_a", new int[]{1, 2, 3, 4, 5});
					client.put(put2);
				}
				client.flush();

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName).build());

				long start = System.nanoTime();
				int count = 0;
				while (reader.getBinlogRecord() != null) {
					count++;
					if (count == 1000) {
						reader.cancel();
						break;
					}
				}
				LOG.info("reader cancel");

				long end = System.nanoTime();
				LOG.info("Binlog reader count: {}, cost: {} ms", count, (end - start) / 1000000L);

				Assert.assertEquals(count, 1000);

			} catch (HoloClientException e) {
				if (!(holoVersion.compareTo(new HoloVersion("2.1.0")) < 0
						&& e.getMessage().contains("For hologres instance version lower than r2.1.0, need to provide slotName to subscribe binlog. your version is HoloVersion"))) {
					throw new RuntimeException(e);
				}
			} finally {
				if (reader != null) {
					reader.cancel();
				}
				execute(conn, new String[]{dropSql1});
			}
		}
	}

	@DataProvider(name = "typeCaseDataWithRecord")
	public Object[][] createDataForReadBinlog() {
		HoloConfig config = buildConfig();
		DataTypeTestUtil.TypeCaseDataWithRecord[] typeToTest = FIXED_PLAN_TYPE_DATA_WITH_RECORD;
		// 只测试fixed plan支持的类型
		Object[][] ret = new Object[typeToTest.length][];
		for (int i = 0; i < typeToTest.length; ++i) {
			ret[i] = new Object[]{typeToTest[i]};
		}
		return ret;
	}

	/**
	 * binlog reader data type test.
	 */
	@Test(dataProvider = "typeCaseDataWithRecord")
	public void binlogReaderDataTypeTest(DataTypeTestUtil.TypeCaseDataWithRecord typeCaseData) throws Exception {
		if (properties == null || holoVersion.compareTo(needVersion) < 0) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setUseFixedFe(false);
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setBinlogReadBatchSize(128);

		final int totalCount = 10;
		final int nullPkId = 5;
		String typeName = typeCaseData.getName();

		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			if (typeName.equals("jsonb")) {
				// set "alter database xx set hg_experimental_enable_binlog_jsonb = on" in test instance to support jsonb.
				try (Statement st = conn.createStatement()) {
					ResultSet rt = st.executeQuery("show hg_experimental_enable_binlog_jsonb");
					if (rt.next() && rt.getObject(1).equals("off")) {
						return;
					}
				}
			}
			String tableName = "holo_client_type_binlog_reader_" + typeName;
			String dropSql1 = "drop table if exists " + tableName;
			String createSql = "create table " + tableName
					+ "(id " + typeCaseData.getColumnType() + ", pk int primary key);\n"
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '1');\n";

			execute(conn, new String[]{dropSql1});
			execute(conn, new String[]{"begin;", createSql, "commit;"});

			BinlogShardGroupReader reader = null;

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < totalCount; ++i) {
					Record record = Record.build(schema);
					if (i == nullPkId) {
						record.setObject(0, null);
					} else {
						record.setObject(0, typeCaseData.getSupplier().apply(i, conn.unwrap(BaseConnection.class)));
					}
					record.setObject(1, i);
					client.put(new Put(record));
				}
				client.flush();

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName).build());

				int count = 0;
				BinlogRecord record;
				LOG.info("begin to getBinlogRecord");
				while ((record = reader.getBinlogRecord()) != null) {
					System.out.println(record);
					int pk = (int) record.getObject(1);
					if (pk == nullPkId) {
						Assert.assertNull(record.getObject(0));
					} else {
						typeCaseData.getPredicate().run(pk, record);
					}
					count++;
					if (count == totalCount) {
						reader.cancel();
						break;
					}
				}
				LOG.info("reader cancel");
			} finally {
				if (reader != null) {
					reader.cancel();
				}
				execute(conn, new String[]{dropSql1});
			}
		}
	}
}
