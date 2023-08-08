/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.model.ExportContext;
import com.alibaba.hologres.client.model.ImportContext;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;

/**
 * 批量导出测试.
 */
public class BulkScanTest extends HoloClientTestBase {
	public static final Logger LOG = LoggerFactory.getLogger(BulkScanTest.class);

	public static String print(byte[] ret) {
		int start = 0;
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		while (start < ret.length) {
			int length = Math.min(16, ret.length - start);
			sb.append(String.format("%8s", Integer.toHexString(start)));
			for (int i = 0; i < length; ++i) {
				sb.append(" ").append(String.format("%2s", Integer.toHexString(Byte.toUnsignedInt(ret[start + i]))));
			}
			sb.append("\n");
			sb.append(String.format("%8s", ""));
			for (int i = 0; i < length; ++i) {
				byte b = ret[start + i];
				sb.append("  ");
				sb.append(b > 32 ? ((char) ret[start + i]) : '?');
			}
			sb.append("\n");
			start += length;
		}
		return sb.toString();
	}

	/**
	 * bulkScan.
	 * Method: put(Put put).
	 */
	@Test
	public void bulkScan002() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteThreadSize(5);
		config.setUseFixedFe(false);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_bulkscan_002\"";
			String tableName2 = "test_schema.\"holO_client_bulkscan_002_002\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName + ";drop table if exists " + tableName2;
			String createSql = "create table " + tableName + "(id int not null,amount decimal(12,2), t text,ts timestamptz, ba bytea,t_a text[],i_a int[], primary key(id));create table " + tableName2 + "(id int not null,amount decimal(12,2), t text,ts timestamptz, ba bytea,t_a text[],i_a int[], primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

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

				PipedInputStream pi = new PipedInputStream();
				PipedOutputStream po = new PipedOutputStream();
				po.connect(pi);
				ExportContext er = client.exportData(Exporter.newBuilder(schema).setOutputStream(po).build());

				/*if(er.getInputStream() == null){
					er.getRowCount().get();
					return;
				}else {
					RecordInputFormat recordFormat = new RecordInputFormat(er.getInputStream(), schema);
					Record r;
					int c = 0;
					while ((r = recordFormat.getRecord()) != null) {
						LOG.info("{}", r);
						++c;
					}
					if (c > 0) {
						LOG.info("RecordInputStream:{}", c);
						LOG.info("result:{}", er.getRowCount().get());
					}
				}*/

				PipedInputStream pis = new PipedInputStream();
				TableSchema schema2 = client.getTableSchema(tableName2);
				ImportContext ic = client.importData(Importer.newBuilder(schema2).setInputStream(pi).build());

				LOG.info("ready?");
				er.getRowCount().get();
				po.close();
				LOG.info("go!");
				long count = ic.getRowCount().get();
				LOG.info("write {}", count);

				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName2)) {
						if (rs.next()) {
							Assert.assertEquals(1000, rs.getInt(1));
						}
					}
				}

			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * bulkScan.
	 * Method: put(Put put).
	 */
	@Test
	public void bulkScan003() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteThreadSize(5);
		config.setUseFixedFe(false);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_bulkscan_003\"";
			String tableName2 = "test_schema.\"holO_client_bulkscan_003_002\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName + ";drop table if exists " + tableName2;
			String createSql = "create table " + tableName + "(id int not null,amount decimal(12,2), t text,ts timestamptz, ba bytea,t_a text[],i_a int[], primary key(id));create table " + tableName2 + "(id int not null,amount decimal(12,2), t text,ts timestamptz, ba bytea,t_a text[],i_a int[], primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			ExportContext exportContext = null;
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

				exportContext = client.exportData(Exporter.newBuilder(schema).build());

				TableSchema schema2 = client.getTableSchema(tableName2);

				LOG.info("ready?{}", Command.getShardCount(client, schema));
				RecordInputFormat recordFormat = new RecordInputFormat(exportContext, schema);
				ImportContext importContext = client.importData(Importer.newBuilder(schema2).build());
				try (RecordOutputFormat recordOutputFormat = new RecordOutputFormat(importContext, schema2)) {
					Record r;
					int c = 0;
					while ((r = recordFormat.getRecord()) != null) {
						//LOG.info("{}", r);
						recordOutputFormat.putRecord(r);
						++c;
					}
					if (c > 0) {
						LOG.info("RecordInputStream:{}", c);
						LOG.info("result:{}", exportContext.getRowCount().get());
					}
				}

				exportContext.getRowCount().get();
				LOG.info("go!");
				long count = importContext.getRowCount().get();
				LOG.info("write {}", count);

				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName2)) {
						if (rs.next()) {
							Assert.assertEquals(1000, rs.getInt(1));
						}
					}
				}

			} catch (Exception e) {
				LOG.error("", e);
			} finally {
				if (exportContext != null) {
					try {
						exportContext.cancel();
					} catch (Exception e) {
						LOG.error("er", e);
					}
				}

				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * bulkScan.
	 * Method: put(Put put).
	 */
	@Test
	public void bulkScan004() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteThreadSize(5);
		config.setUseFixedFe(false);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_bulkscan_004\"";
			String tableName2 = "test_schema.\"holO_client_bulkscan_004_002\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName + ";drop table if exists " + tableName2;
			String createSql = "create table " + tableName + "(id int not null,amount decimal(12,2), t text,ts timestamptz, ba bytea,t_a text[],i_a int[], primary key(id));create table " + tableName2 + "(id int not null,amount decimal(12,2), t text,ts timestamptz, ba bytea,t_a text[],i_a int[], primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			ExportContext exportContext = null;
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

				exportContext = client.exportData(Exporter.newBuilder(schema).build());

				TableSchema schema2 = client.getTableSchema(tableName2);

				LOG.info("ready?{}", Command.getShardCount(client, schema));
				RecordInputFormat recordFormat = new RecordInputFormat(exportContext, schema);
				ImportContext importContext = client.importData(Importer.newBuilder(schema2).setThreadSize(3).build());
				try (RecordOutputFormat recordOutputFormat = new RecordOutputFormat(importContext, schema2)) {
					Record r;
					int c = 0;
					while ((r = recordFormat.getRecord()) != null) {
						//LOG.info("{}", r);
						recordOutputFormat.putRecord(r);
						++c;
					}
					if (c > 0) {
						LOG.info("RecordInputStream:{}", c);
						LOG.info("result:{}", exportContext.getRowCount().get());
					}
				}

				exportContext.getRowCount().get();
				LOG.info("go!");
				long count = importContext.getRowCount().get();
				LOG.info("write {}", count);

				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName2)) {
						if (rs.next()) {
							Assert.assertEquals(1000, rs.getInt(1));
						}
					}
				}

			} catch (Exception e) {
				LOG.error("", e);
			} finally {
				if (exportContext != null) {
					try {
						exportContext.cancel();
					} catch (Exception e) {
						LOG.error("er", e);
					}
				}

				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * bulkScan.
	 * Method: put(Put put).
	 */
	@Test
	public void bulkScan005() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteThreadSize(5);
		config.setUseFixedFe(false);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_bulkscan_005\"";
			String tableName2 = "test_schema.\"holO_client_bulkscan_005_002\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName + ";drop table if exists " + tableName2;
			String createSql = "create table " + tableName + "(id int not null,amount decimal(12,2), t text,ts timestamptz, ba bytea,t_a text[],i_a int[], primary key(id));create table " + tableName2 + "(id int not null,amount decimal(12,2), t text,ts timestamptz, ba bytea,t_a text[],i_a int[], primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			ExportContext exportContext = null;
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

				exportContext = client.exportData(Exporter.newBuilder(schema).setShardRange(0, 5).build());

				TableSchema schema2 = client.getTableSchema(tableName2);

				LOG.info("ready?{}", Command.getShardCount(client, schema));
				RecordInputFormat recordFormat = new RecordInputFormat(exportContext, schema);
				ImportContext importContext = client.importData(Importer.newBuilder(schema2).setShardRange(0, 5).setThreadSize(3).build());
				int c = 0;
				try (RecordOutputFormat recordOutputFormat = new RecordOutputFormat(importContext, schema2)) {
					Record r;
					while ((r = recordFormat.getRecord()) != null) {
						//LOG.info("{}", r);
						recordOutputFormat.putRecord(r);
						++c;
					}
					if (c > 0) {
						LOG.info("RecordInputStream:{}", c);
						LOG.info("result:{}", exportContext.getRowCount().get());
					}
				}

				exportContext.getRowCount().get();
				LOG.info("go!");
				long count = importContext.getRowCount().get();
				LOG.info("write {}", count);
				Assert.assertEquals(c, count);

				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName2)) {
						if (rs.next()) {
							Assert.assertEquals(count, rs.getInt(1));
						}
					}
				}

			} catch (Exception e) {
				LOG.error("", e);
			} finally {
				if (exportContext != null) {
					try {
						exportContext.cancel();
					} catch (Exception e) {
						LOG.error("er", e);
					}
				}

				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * test cancel.
	 * 据说是引擎的问题，先ignore了
	 * Method: put(Put put).
	 */
	@Ignore
	@Test
	public void bulkScan006() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteThreadSize(5);
		config.setUseFixedFe(false);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_bulkscan_003\"";
			String tableName2 = "test_schema.\"holO_client_bulkscan_003_002\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName + ";drop table if exists " + tableName2;
			String createSql = "create table " + tableName + "(id int not null,amount decimal(12,2), t text,ts timestamptz, ba bytea,t_a text[],i_a int[], primary key(id));create table " + tableName2 + "(id int not null,amount decimal(12,2), t text,ts timestamptz, ba bytea,t_a text[],i_a int[], primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			ExportContext exportContext = null;
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

				exportContext = client.exportData(Exporter.newBuilder(schema).build());

				TableSchema schema2 = client.getTableSchema(tableName2);

				LOG.info("ready?{}", Command.getShardCount(client, schema));
				RecordInputFormat recordFormat = new RecordInputFormat(exportContext, schema);
				ImportContext importContext = client.importData(Importer.newBuilder(schema2).setThreadSize(3).build());
				int c = 0;
				try (RecordOutputFormat recordOutputFormat = new RecordOutputFormat(importContext, schema2)) {
					Record r;
					while ((r = recordFormat.getRecord()) != null) {
						//LOG.info("{}", r);
						recordOutputFormat.putRecord(r);
						++c;
						if (c == 500) {
							importContext.cancel();
							recordFormat.cancel();
							break;
						}
					}
				}

				for (int i = 1000; i < 2000; ++i) {
					Put put2 = new Put(schema2);
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

				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName2)) {
						if (rs.next()) {
							Assert.assertEquals(1000, rs.getInt(1));
						}
					}
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName2 + "where id < 1000")) {
						if (rs.next()) {
							Assert.assertEquals(0, rs.getInt(1));
						}
					}
				}

			} catch (Exception e) {
				LOG.error("", e);
			} finally {
				if (exportContext != null) {
					try {
						exportContext.cancel();
					} catch (Exception e) {
						LOG.error("er", e);
					}
				}

				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * check correctness of recordInputFormat.
	 * Method: put(Put put).
	 */
	@Test
	public void bulkScan008() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteThreadSize(5);
		config.setUseFixedFe(false);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_bulkscan_008\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,amount decimal(12,2), t text,ts timestamptz, ba bytea,t_a text[],i_a int[], primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			ExportContext exportContext = null;
			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < 10; ++i) {
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

				exportContext = client.exportData(Exporter.newBuilder(schema).build());

				LOG.info("ready?{}", Command.getShardCount(client, schema));
				RecordInputFormat recordFormat = new RecordInputFormat(exportContext, schema);

				Record r;
				int c = 0;
				while ((r = recordFormat.getRecord()) != null) {
					LOG.info("{}", r);
					++c;
				}
				if (c > 0) {
					LOG.info("RecordInputStream:{}", c);
					LOG.info("result:{}", exportContext.getRowCount().get());
				}

				long rowCount = exportContext.getRowCount().get();
				Assert.assertEquals(10, c);
				Assert.assertEquals(c, rowCount);

			} catch (Exception e) {
				LOG.error("", e);
			} finally {
				if (exportContext != null) {
					try {
						exportContext.cancel();
					} catch (Exception e) {
						LOG.error("er", e);
					}
				}

				//execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * bulkScan kill connection.
	 * Method: put(Put put).
	 */
	@Test
	public void bulkScan009() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteThreadSize(5);
		config.setUseFixedFe(false);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_bulkscan_009\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null, col int)";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			//杀copy 连接
			Runnable pgTerminateCopyInTask = new Runnable() {
				@Override
				public void run() {
					try {
						try (Connection conn = buildConnection()) {
							try (Statement statement = conn.createStatement()) {
								String sql = "select pid from pg_stat_activity where backend_type = 'client backend' and query like '%COPY%holO_client_bulkscan_009%FROM%' and application_name like '%holo-client%'";
								Integer pid;
								while (true) {
									try (ResultSet rs = statement.executeQuery(sql)) {
										if (rs.next()) {
											pid = rs.getInt(1);
											break;
										}
									}
								}
								statement.execute("select pg_terminate_backend(" + pid + ")");
							}
						}
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			};
			Thread thread1 = new Thread(pgTerminateCopyInTask);
			thread1.start();

			try {
				TableSchema schema = client.getTableSchema(tableName);
				ImportContext importContext = null;
				try (PipedOutputStream os = new PipedOutputStream();) {
					PipedInputStream is = new PipedInputStream(os);
					importContext = client.importData(Importer.newBuilder(schema).setInputStream(is).build());
					Assert.assertThrows(IOException.class, () -> {
						try {
							while (true) {
								os.write((0 + ",12\n").getBytes());
								os.flush();
							}
						} catch (Exception e) {
							LOG.error("catch exception", e);
							Assert.assertTrue(e.getMessage().contains("Pipe closed"));
							throw e;
						}
					});
				}

				ImportContext finalImportContext = importContext;
				Assert.assertThrows(ExecutionException.class, () -> {
					try {
						finalImportContext.getRowCount().get();
					} catch (ExecutionException e) {
						LOG.error("catch exception", e);
						Assert.assertTrue(e.getMessage().contains("Database connection failed when ending copy") || e.getMessage().contains("Database connection failed when writing to copy"));
						throw e;
					}
				});
			} catch (Exception e) {
				LOG.error("", e);
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * bulkScan.
	 * Method: put(Put put).
	 */
	@Test
	public void bulkScan200() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteThreadSize(5);
		config.setUseFixedFe(false);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "test_schema.\"holO_client_bulkscan_200\"";
			String tableName2 = "test_schema.\"holO_client_bulkscan_200_002\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName + ";drop table if exists " + tableName2;
			String createSql = "create table " + tableName + "(id int not null,amount decimal(12,2), t text,ts timestamptz, ba bytea,t_a text[],i_a int[], primary key(id));create table " + tableName2 + "(id int not null,amount decimal(12,2), t text,ts timestamptz, ba bytea,t_a text[],i_a int[], primary key(id))";

			execute(conn, new String[]{createSchema, dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				TableSchema schema2 = client.getTableSchema(tableName2);

				ImportContext importContext = null;
				try (PipedOutputStream os = new PipedOutputStream();) {
					PipedInputStream is = new PipedInputStream(os);
					importContext = client.importData(Importer.newBuilder(schema2).setInputStream(is).build());
					for (int i = 0; i < 10000; ++i) {
						os.write((i + ",12\n").getBytes());
						os.flush();
					}
				} catch (Exception e) {
					LOG.error("-----------------------------------------------------", e);
				}

				LOG.info("go!");
				long count = importContext.getRowCount().get();
				LOG.info("write {}", count);

				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName2)) {
						if (rs.next()) {
							Assert.assertEquals(10000, rs.getInt(1));
						}
					}
				}

			} catch (Exception e) {
				LOG.error("", e);
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

}
