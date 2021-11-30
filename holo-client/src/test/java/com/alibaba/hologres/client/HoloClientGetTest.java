package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.WriteMode;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.model.TableSchema;

import java.sql.Connection;
import java.util.concurrent.ExecutionException;

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
}
