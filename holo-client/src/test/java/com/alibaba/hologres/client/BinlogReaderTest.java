/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.impl.binlog.BinlogOffset;
import com.alibaba.hologres.client.impl.binlog.HoloBinlogDecoder;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.hologres.client.model.binlog.BinlogRecord;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.PGProperty;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * Binlog Decoder 测试.
 * holo实例需要大于1.1.2版本.
 * 测试数据库需要开启jdbc消费binlog功能支持："create extension if not exists hg_binlog;"
 */
public class BinlogReaderTest extends HoloClientTestBase {
	public static final Logger LOG = LoggerFactory.getLogger(BinlogReaderTest.class);
	HoloVersion needVersion = new HoloVersion(1, 1, 2);

	private static final String CREATE_EXTENSION_SQL = "create extension if not exists hg_binlog";

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
			String publicationName = "holo_client_binlog_decoder_002_publication_test";
			String slotName = "holo_client_binlog_decoder_002_slot_1";

			String dropSql1 = "drop table if exists " + tableName + "; drop publication if exists " + publicationName + ";\n";
			String dropSql2 = "delete from hologres.hg_replication_progress where slot_name='" + slotName + "';\n";
			String dropSql3 = "call hg_drop_logical_replication_slot('" + slotName + "');";
			String createSql1 = "create extension if not exists hg_binlog;\n";
			String createSql2 = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '1');\n";
			String createSql3 = "create publication " + publicationName + " for table " + tableName + ";\n";
			String createSql4 = "call hg_create_logical_replication_slot('" + slotName + "', 'hgoutput', '" + publicationName + "');\n";

			execute(conn, new String[]{CREATE_EXTENSION_SQL, dropSql1, dropSql2});
			try {
				execute(conn, new String[]{dropSql3});
			} catch (SQLException e) {
				LOG.info(slotName + " not exists.");
			}
			execute(conn, new String[]{createSql1, "begin;", createSql2, "commit;", createSql3});
			execute(conn, new String[]{createSql4});

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
										.withSlotName(slotName)
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
				execute(conn, new String[]{dropSql1, dropSql2, dropSql3});
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
			String publicationName = "holo_client_binlog_reader_003_publication_test";
			String slotName = "holo_client_binlog_reader_003_slot_1";

			String dropSql1 = "drop table if exists " + tableName + "; drop publication if exists " + publicationName + ";\n";
			String dropSql2 = "delete from hologres.hg_replication_progress where slot_name='" + slotName + "';\n";
			String dropSql3 = "call hg_drop_logical_replication_slot('" + slotName + "');";
			String createSql1 = "create extension if not exists hg_binlog;\n";
			String createSql2 = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '3');\n";
			String createSql3 = "create publication " + publicationName + " for table " + tableName + ";\n";
			String createSql4 = "call hg_create_logical_replication_slot('" + slotName + "', 'hgoutput', '" + publicationName + "');\n";

			execute(conn, new String[]{CREATE_EXTENSION_SQL, dropSql1, dropSql2});
			try {
				execute(conn, new String[]{dropSql3});
			} catch (SQLException e) {
				LOG.info(slotName + " not exists.");
			}
			execute(conn, new String[]{createSql1, "begin;", createSql2, "commit;", createSql3});
			execute(conn, new String[]{createSql4});

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

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName, slotName).build());

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
				execute(conn, new String[]{dropSql1, dropSql2, dropSql3});
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
			String publicationName = "holo_client_binlog_reader_004_publication_test";
			String slotName = "holo_client_binlog_reader_004_slot_1";

			String dropSql1 = "drop table if exists " + tableName + "; drop publication if exists " + publicationName + ";\n";
			String dropSql2 = "delete from hologres.hg_replication_progress where slot_name='" + slotName + "';\n";
			String dropSql3 = "call hg_drop_logical_replication_slot('" + slotName + "');";
			String createSql1 = "create extension if not exists hg_binlog;\n";
			String createSql2 = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '" + shardCount + "');\n";
			String createSql3 = "create publication " + publicationName + " for table " + tableName + ";\n";
			String createSql4 = "call hg_create_logical_replication_slot('" + slotName + "', 'hgoutput', '" + publicationName + "');\n";

			execute(conn, new String[]{CREATE_EXTENSION_SQL, dropSql1, dropSql2});
			try {
				execute(conn, new String[]{dropSql3});
			} catch (SQLException e) {
				LOG.info(slotName + " not exists.");
			}
			execute(conn, new String[]{createSql1, "begin;", createSql2, "commit;", createSql3});
			execute(conn, new String[]{createSql4});

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

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName, slotName).setBinlogReadStartTime(now.toString()).build());
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
				execute(conn, new String[]{dropSql1, dropSql2, dropSql3});
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
			String publicationName = "holo_client_binlog_reader_005_publication_test";
			String slotName = "holo_client_binlog_reader_005_slot_1";

			String dropSql1 = "drop table if exists " + tableName + "; drop publication if exists " + publicationName + ";\n";
			String dropSql2 = "delete from hologres.hg_replication_progress where slot_name='" + slotName + "';\n";
			String dropSql3 = "call hg_drop_logical_replication_slot('" + slotName + "');";
			String createSql1 = "create extension if not exists hg_binlog;\n";
			String createSql2 = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '10');\n";
			String createSql3 = "create publication " + publicationName + " for table " + tableName + ";\n";
			String createSql4 = "call hg_create_logical_replication_slot('" + slotName + "', 'hgoutput', '" + publicationName + "');\n";

			execute(conn, new String[]{CREATE_EXTENSION_SQL, dropSql1, dropSql2});
			try {
				execute(conn, new String[]{dropSql3});
			} catch (SQLException e) {
				LOG.info(slotName + " not exists.");
			}
			execute(conn, new String[]{createSql1, "begin;", createSql2, "commit;", createSql3});
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
				execute(conn, new String[]{dropSql1, dropSql2, dropSql3});
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
			String publicationName = "holo_client_binlog_reader_007_publication_test";
			String slotName = "holo_client_binlog_reader_007_slot_1";

			String dropSql1 = "drop table if exists " + tableName + "; drop publication if exists " + publicationName + ";\n";
			String dropSql2 = "delete from hologres.hg_replication_progress where slot_name='" + slotName + "';\n";
			String dropSql3 = "call hg_drop_logical_replication_slot('" + slotName + "');";
			String createSql1 = "create extension if not exists hg_binlog;\n";
			String createSql2 = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '3');\n";
			String createSql3 = "create publication " + publicationName + " for table " + tableName + ";\n";
			String createSql4 = "call hg_create_logical_replication_slot('" + slotName + "', 'hgoutput', '" + publicationName + "');\n";

			execute(conn, new String[]{CREATE_EXTENSION_SQL, dropSql1, dropSql2});
			try {
				execute(conn, new String[]{dropSql3});
			} catch (SQLException e) {
				LOG.info(slotName + " not exists.");
			}
			execute(conn, new String[]{createSql1, "begin;", createSql2, "commit;", createSql3});
			execute(conn, new String[]{createSql4});

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

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName, slotName).build());

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
				execute(conn, new String[]{dropSql1, dropSql2, dropSql3});
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
			String publicationName = "holo_client_binlog_reader_011_publication_test";
			String slotName = "holo_client_binlog_reader_011_slot_1";

			String dropSql1 = "drop table if exists " + tableName + "; drop publication if exists " + publicationName + ";\n";
			String dropSql2 = "delete from hologres.hg_replication_progress where slot_name='" + slotName + "';\n";
			String dropSql3 = "call hg_drop_logical_replication_slot('" + slotName + "');";
			String createSql1 = "create extension if not exists hg_binlog;\n";
			String createSql2 = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '3');\n";
			String createSql3 = "create publication " + publicationName + " for table " + tableName + ";\n";
			String createSql4 = "call hg_create_logical_replication_slot('" + slotName + "', 'hgoutput', '" + publicationName + "');\n";

			execute(conn, new String[]{CREATE_EXTENSION_SQL, dropSql1, dropSql2});
			try {
				execute(conn, new String[]{dropSql3});
			} catch (SQLException e) {
				LOG.info(slotName + " not exists.");
			}
			execute(conn, new String[]{createSql1, "begin;", createSql2, "commit;", createSql3});
			execute(conn, new String[]{createSql4});

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

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName, slotName).build());

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
				execute(conn, new String[]{dropSql1, dropSql2, dropSql3});
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
			String publicationName = "holo_client_binlog_reader_013_publication_test";
			String slotName = "holo_client_binlog_reader_013_slot_1";

			String dropSql1 = "drop table if exists " + tableName + "; drop publication if exists " + publicationName + ";\n";
			String dropSql2 = "delete from hologres.hg_replication_progress where slot_name='" + slotName + "';\n";
			String dropSql3 = "call hg_drop_logical_replication_slot('" + slotName + "');";
			String createSql1 = "create extension if not exists hg_binlog;\n";
			String createSql2 = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '3');\n";
			String createSql3 = "create publication " + publicationName + " for table " + tableName + ";\n";
			String createSql4 = "call hg_create_logical_replication_slot('" + slotName + "', 'hgoutput', '" + publicationName + "');\n";
			String deleteSql = "delete from " + tableName + ";\n";

			execute(conn, new String[]{CREATE_EXTENSION_SQL, dropSql1, dropSql2});
			try {
				execute(conn, new String[]{dropSql3});
			} catch (SQLException e) {
				LOG.info(slotName + " not exists.");
			}
			execute(conn, new String[]{createSql1, "begin;", createSql2, "commit;", createSql3});
			execute(conn, new String[]{createSql4});

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

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName, slotName).build());

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
				execute(conn, new String[]{dropSql1, dropSql2, dropSql3});
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
			String publicationName = "holo_client_binlog_reader_017_publication_test";
			String slotName = "holo_client_binlog_reader_017_slot_1";

			String dropSql1 = "drop table if exists " + tableName + "; drop publication if exists " + publicationName + ";\n";
			String dropSql2 = "delete from hologres.hg_replication_progress where slot_name='" + slotName + "';\n";
			String dropSql3 = "call hg_drop_logical_replication_slot('" + slotName + "');";
			String createSql1 = "create extension if not exists hg_binlog;\n";
			String createSql2 = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '3');\n";
			String createSql3 = "create publication " + publicationName + " for table " + tableName + ";\n";
			String createSql4 = "call hg_create_logical_replication_slot('" + slotName + "', 'hgoutput', '" + publicationName + "');\n";
			String deleteSql = "delete from " + tableName + ";\n";

			execute(conn, new String[]{CREATE_EXTENSION_SQL, dropSql1, dropSql2});
			try {
				execute(conn, new String[]{dropSql3});
			} catch (SQLException e) {
				LOG.info(slotName + " not exists.");
			}
			execute(conn, new String[]{createSql1, "begin;", createSql2, "commit;", createSql3});
			execute(conn, new String[]{createSql4});

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

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName, slotName).build());

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
				execute(conn, new String[]{dropSql1, dropSql2, dropSql3});
			}
		}
	}

	/**
	 * binlogGroupShardReader.
	 * Alter table: add one new column.
	 */
	@Test
	public void binlogReader019() throws Exception {
		if (properties == null || holoVersion.compareTo(needVersion) < 0) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteThreadSize(10);
		config.setBinlogReadBatchSize(8);  // 防止读取太快刷完所有写入的数据

		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_binlog_reader_019";
			String publicationName = "holo_client_binlog_reader_019_publication_test";
			String slotName = "holo_client_binlog_reader_019_slot_1";

			String dropSql1 = "drop table if exists " + tableName + "; drop publication if exists " + publicationName + ";\n";
			String dropSql2 = "delete from hologres.hg_replication_progress where slot_name='" + slotName + "';\n";
			String dropSql3 = "call hg_drop_logical_replication_slot('" + slotName + "');";
			String createSql1 = "create extension if not exists hg_binlog;\n";
			String createSql2 = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '3');\n";
			String createSql3 = "create publication " + publicationName + " for table " + tableName + ";\n";
			String createSql4 = "call hg_create_logical_replication_slot('" + slotName + "', 'hgoutput', '" + publicationName + "');\n";
			String alterSql = "ALTER TABLE IF EXISTS " + tableName + " ADD COLUMN new_column_1 int;\n";

			execute(conn, new String[]{CREATE_EXTENSION_SQL, dropSql1, dropSql2});
			try {
				execute(conn, new String[]{dropSql3});
			} catch (SQLException e) {
				LOG.info(slotName + " not exists.");
			}
			execute(conn, new String[]{createSql1, "begin;", createSql2, "commit;", createSql3});
			execute(conn, new String[]{createSql4});

			try {
				try (BinlogShardGroupReader reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName, slotName).build())) {
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
							Assert.assertEquals(8, record.getSchema().getColumnSchema().length);
							Assert.assertEquals(id, (int) record.getObject(7));
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
				execute(conn, new String[]{dropSql1, dropSql2, dropSql3});
			}
		}
	}

	/**
	 * binlogGroupShardReader.
	 * Alter table: drop one column.
	 */
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
			String publicationName = "holo_client_binlog_reader_021_publication_test";
			String slotName = "holo_client_binlog_reader_021_slot_1";

			String dropSql1 = "drop table if exists " + tableName + "; drop publication if exists " + publicationName + ";\n";
			String dropSql2 = "delete from hologres.hg_replication_progress where slot_name='" + slotName + "';\n";
			String dropSql3 = "call hg_drop_logical_replication_slot('" + slotName + "');";
			String createSql1 = "create extension if not exists hg_binlog;\n";
			String createSql2 = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '3');\n";
			String createSql3 = "create publication " + publicationName + " for table " + tableName + ";\n";
			String createSql4 = "call hg_create_logical_replication_slot('" + slotName + "', 'hgoutput', '" + publicationName + "');\n";
			String alterSql = "set hg_experimental_enable_drop_column=true;\n"
					+ "ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN ba;\n";

			execute(conn, new String[]{CREATE_EXTENSION_SQL, dropSql1, dropSql2});
			try {
				execute(conn, new String[]{dropSql3});
			} catch (SQLException e) {
				LOG.info(slotName + " not exists.");
			}
			execute(conn, new String[]{createSql1, "begin;", createSql2, "commit;", createSql3});
			execute(conn, new String[]{createSql4});

			try (BinlogShardGroupReader reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName, slotName).build())) {
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
				execute(conn, new String[]{dropSql1, dropSql2, dropSql3});
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
			String publicationName = "holo_client_binlog_reader_023_publication_test";
			String slotName = "holo_client_binlog_reader_023_slot_1";

			String dropSql1 = "drop table if exists " + tableName + "; drop publication if exists " + publicationName + ";\n";
			String dropSql2 = "delete from hologres.hg_replication_progress where slot_name='" + slotName + "';\n";
			String dropSql3 = "call hg_drop_logical_replication_slot('" + slotName + "');";
			String createSql1 = "create extension if not exists hg_binlog;\n";
			String createSql2 = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '1');\n";
			String createSql3 = "create publication " + publicationName + " for table " + tableName + ";\n";
			String createSql4 = "call hg_create_logical_replication_slot('" + slotName + "', 'hgoutput', '" + publicationName + "');\n";

			execute(conn, new String[]{CREATE_EXTENSION_SQL, dropSql1, dropSql2});
			try {
				execute(conn, new String[]{dropSql3});
			} catch (SQLException e) {
				LOG.info(slotName + " not exists.");
			}
			execute(conn, new String[]{createSql1, "begin;", createSql2, "commit;", createSql3});
			execute(conn, new String[]{createSql4});

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

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName, slotName).build());

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
					Subscribe.OffsetBuilder builder = Subscribe.newOffsetBuilder(tableName, slotName);
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
				execute(conn, new String[]{dropSql1, dropSql2, dropSql3});
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
			String publicationName = "holo_client_binlog_reader_029_publication_test";
			String slotName = "holo_client_binlog_reader_029_slot_1";

			String dropSql1 = "drop table if exists " + tableName + "; drop publication if exists " + publicationName + ";\n";
			String dropSql2 = "delete from hologres.hg_replication_progress where slot_name='" + slotName + "';\n";
			String dropSql3 = "call hg_drop_logical_replication_slot('" + slotName + "');";
			String createSql1 = "create extension if not exists hg_binlog;\n";
			String createSql2 = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '" + shardCount + "');\n";
			String createSql3 = "create publication " + publicationName + " for table " + tableName + ";\n";
			String createSql4 = "call hg_create_logical_replication_slot('" + slotName + "', 'hgoutput', '" + publicationName + "');\n";

			execute(conn, new String[]{CREATE_EXTENSION_SQL, dropSql1, dropSql2});
			try {
				execute(conn, new String[]{dropSql3});
			} catch (SQLException e) {
				LOG.info(slotName + " not exists.");
			}
			execute(conn, new String[]{createSql1, "begin;", createSql2, "commit;", createSql3});
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
						reader.cancel();
						break;
					}
				}
				LOG.info("reader cancel");

				Assert.assertEquals(count, 25000);

				try (HoloClient client2 = new HoloClient(config)) {
					Subscribe.OffsetBuilder builder = Subscribe.newOffsetBuilder(tableName, slotName);
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
				execute(conn, new String[]{dropSql1, dropSql2, dropSql3});
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
			String createSql1 = "create extension if not exists hg_binlog;\n";
			String createSql2 = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '" + shardCount + "');\n";
			String createSql3 = "create publication " + publicationName + " for table " + tableName + ";\n";
			String createSql4 = "call hg_create_logical_replication_slot('" + slotName + "', 'hgoutput', '" + publicationName + "');\n";

			execute(conn, new String[]{dropSql1, dropSql2});
			try {
				execute(conn, new String[]{dropSql3});
			} catch (SQLException e) {
				LOG.info(slotName + " not exists.");
			}
			execute(conn, new String[]{createSql1, "begin;", createSql2, "commit;", createSql3});
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
				execute(conn, new String[]{dropSql1, dropSql2, dropSql3});
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
			String publicationName = "holo_client_binlog_reader_031_publication_test";
			String slotName = "holo_client_binlog_reader_031_slot_1";

			String dropSql1 = "drop table if exists " + tableName + "; drop publication if exists " + publicationName + ";\n";
			String dropSql2 = "delete from hologres.hg_replication_progress where slot_name='" + slotName + "';\n";
			String dropSql3 = "call hg_drop_logical_replication_slot('" + slotName + "');";
			String createSchema = "create schema if not exists new_schema;\n";
			String createSql1 = "create extension if not exists hg_binlog;\n";
			String createSql2 = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '3');\n";
			String createSql3 = "create publication " + publicationName + " for table " + tableName + ";\n";
			String createSql4 = "call hg_create_logical_replication_slot('" + slotName + "', 'hgoutput', '" + publicationName + "');\n";

			execute(conn, new String[]{CREATE_EXTENSION_SQL, dropSql1, dropSql2});
			try {
				execute(conn, new String[]{dropSql3});
			} catch (SQLException e) {
				LOG.info(slotName + " not exists.");
			}
			execute(conn, new String[]{createSchema, createSql1, "begin;", createSql2, "commit;", createSql3});
			execute(conn, new String[]{createSql4});

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

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName, slotName).build());

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
				execute(conn, new String[]{dropSql1, dropSql2, dropSql3});
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
			String publicationName = "holo_client_binlog_reader_033_publication_test";
			String slotName = "holo_client_binlog_reader_033_slot_1";

			String dropSql1 = "drop table if exists " + tableName + "; drop publication if exists " + publicationName + ";\n";
			String dropSql2 = "delete from hologres.hg_replication_progress where slot_name='" + slotName + "';\n";
			String dropSql3 = "call hg_drop_logical_replication_slot('" + slotName + "');";
			String createSql1 = "create extension if not exists hg_binlog;\n";
			String createSql2 = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '1');\n";
			String createSql3 = "create publication " + publicationName + " for table " + tableName + ";\n";
			String createSql4 = "call hg_create_logical_replication_slot('" + slotName + "', 'hgoutput', '" + publicationName + "');\n";

			execute(conn, new String[]{dropSql1, dropSql2});
			try {
				execute(conn, new String[]{dropSql3});
			} catch (SQLException e) {
				LOG.info(slotName + " not exists.");
			}
			execute(conn, new String[]{createSql1, "begin;", createSql2, "commit;", createSql3});
			execute(conn, new String[]{createSql4});

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

				reader = client.binlogSubscribe(Subscribe.newStartTimeBuilder(tableName, slotName).build());

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
				execute(conn, new String[]{dropSql1, dropSql2, dropSql3});
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
			String createSql1 = "create extension if not exists hg_binlog;\n";
			String createSql2 = "create table " + tableName
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName + "', 'binlog.level', 'replica');\n"
					+ "call set_table_property('" + tableName + "', 'shard_count', '10');\n";
			String createSql3 = "create publication " + publicationName + " for table " + tableName + ";\n";
			String createSql4 = "call hg_create_logical_replication_slot('" + slotName + "', 'hgoutput', '" + publicationName + "');\n";

			execute(conn, new String[]{dropSql1, dropSql2});
			try {
				execute(conn, new String[]{dropSql3});
			} catch (SQLException e) {
				LOG.info(slotName + " not exists.");
			}
			execute(conn, new String[]{createSql1, "begin;", createSql2, "commit;", createSql3});
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
				execute(conn, new String[]{dropSql1, dropSql2, dropSql3});
			}
		}
	}

}
