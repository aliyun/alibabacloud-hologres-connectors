/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.HoloClientWithDetailsException;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.hologres.client.utils.DataTypeTestUtil;
import org.postgresql.core.BaseConnection;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static com.alibaba.hologres.client.utils.DataTypeTestUtil.ALL_TYPE_DATA;
import static com.alibaba.hologres.client.utils.DataTypeTestUtil.ALL_TYPE_DATA_WITH_RECORD;
import static com.alibaba.hologres.client.utils.DataTypeTestUtil.EXCEPTION_ALL_TYPE_DATA;
import static com.alibaba.hologres.client.utils.DataTypeTestUtil.FIXED_PLAN_TYPE_DATA;
import static com.alibaba.hologres.client.utils.DataTypeTestUtil.FIXED_PLAN_TYPE_DATA_WITH_RECORD;

/**
 * 特殊类型的支持测试.
 * 测试库需要已经安装了postgis和roaringbitmap.
 */
public class HoloClientTypesTest extends HoloClientTestBase {

	@DataProvider(name = "typeCaseData")
	public Object[][] createData() {
		HoloConfig config = buildConfig();
		DataTypeTestUtil.TypeCaseData[] typeToTest;
		// fixed fe只测试fixed plan支持的类型
		if (config.isUseFixedFe()) {
			typeToTest = FIXED_PLAN_TYPE_DATA;
		} else {
			typeToTest = ALL_TYPE_DATA;
		}
		Object[][] ret = new Object[typeToTest.length][];
		for (int i = 0; i < typeToTest.length; ++i) {
			ret[i] = new Object[]{typeToTest[i]};
		}
		return ret;
	}

	@DataProvider(name = "typeCaseDataWithRecord")
	public Object[][] createDataForGet() {
		HoloConfig config = buildConfig();
		DataTypeTestUtil.TypeCaseDataWithRecord[] typeToTest;
		// fixed fe只测试fixed plan支持的类型
		if (config.isUseFixedFe()) {
			typeToTest = FIXED_PLAN_TYPE_DATA_WITH_RECORD;
		} else {
			typeToTest = ALL_TYPE_DATA_WITH_RECORD;
		}
		Object[][] ret = new Object[typeToTest.length][];
		for (int i = 0; i < typeToTest.length; ++i) {
			ret[i] = new Object[]{typeToTest[i]};
		}
		return ret;
	}

	@DataProvider(name = "dirtyCaseData")
	public Object[][] createExceptionData() {
		Object[][] ret = new Object[EXCEPTION_ALL_TYPE_DATA.length][];
		for (int i = 0; i < EXCEPTION_ALL_TYPE_DATA.length; ++i) {
			ret[i] = new Object[]{EXCEPTION_ALL_TYPE_DATA[i]};
		}
		return ret;
	}

	/**
	 * GIS.
	 * Method: put(Put put).
	 */
	@Test
	public void testGIS() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		if (config.isUseFixedFe()) {
			return;
		}
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_gis";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int PRIMARY KEY, b geography(POINT,4326))";

			execute(conn, new String[]{dropSql, createSql});

			try {
				TableSchema schema = client.getTableSchema(tableName);
				System.out.println(schema.getColumn(1).getTypeName());
				Put put = new Put(schema);
				put.setObject(0, 0);
				put.setObject(1, "POINT(-110 90)");
				client.put(put);

				client.flush();

				String ret = client.sql((con) -> {
					try (Statement stat = con.createStatement()) {
						try (ResultSet rs = stat.executeQuery("select ST_AsText(b) from " + tableName + " where id=0")) {
							if (rs.next()) {
								return rs.getString(1);
							}
						}
					}
					return null;
				}).get();
				Assert.assertEquals("POINT(-110 90)", ret);
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * allType.
	 * Method: put(Put put).
	 */
	@Test(dataProvider = "typeCaseData")
	public void testALLTypeInsert(DataTypeTestUtil.TypeCaseData typeCaseData) throws Exception {
		if (properties == null) {
			return;
		}

		final int totalCount = 10;
		final int nullPkId = 5;
		String typeName = typeCaseData.getName();

		HoloConfig config = buildConfig();
		// fixed fe not support jsonb and roaringbitmap now
		if ((typeName.equals("roaringbitmap") || typeName.equals("jsonb")) && config.isUseFixedFe()) {
			return;
		}
		config.setAppName("testALLTypeInsert");
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);

		// config.setUseLegacyPutHandler(true);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "\"" + "holo_client_type_" + typeName + "\"";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id " + typeCaseData.getColumnType() + ", pk int primary key)";
			execute(conn, new String[]{dropSql, createSql});
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

				int count = 0;
				try (Statement stat = conn.createStatement()) {
					LOG.info("current type:{}", typeName);
					String sql = "select * from " + tableName;
					if ("roaringbitmap".equals(typeName)) {
						sql = "select rb_cardinality(id), pk from " + tableName;
					}

					try (ResultSet rs = stat.executeQuery(sql)) {
						while (rs.next()) {
							int i = rs.getInt(2);
							if (i == nullPkId) {
								Assert.assertNull(rs.getObject(1));
							} else {
								typeCaseData.getPredicate().run(i, rs);
							}
							++count;
						}
					}
					Assert.assertEquals(count, totalCount);
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * allType.
	 * times of get > 5, to test statement name is saved
	 * Method: get(Get get).
	 */
	@Test(dataProvider = "typeCaseDataWithRecord")
	public void testALLTypeGet(DataTypeTestUtil.TypeCaseDataWithRecord typeCaseData) throws Exception {
		if (properties == null) {
			return;
		}

		final int totalCount = 10;
		final int nullPkId = 5;
		String typeName = typeCaseData.getName();

		HoloConfig config = buildConfig();
		// fixed fe not support jsonb and roaringbitmap now
		if ((typeName.equals("roaringbitmap") || typeName.equals("jsonb")) && config.isUseFixedFe()) {
			return;
		}
		config.setAppName("testALLTypeGet");
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);

		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "\"" + "holo_client_type_get_" + typeName + "\"";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id " + typeCaseData.getColumnType() + ", pk int primary key)";
			execute(conn, new String[]{dropSql, createSql});
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

				for (int i = 0; i < 10; i++) {
					Record r = client.get(Get.newBuilder(schema).setPrimaryKey("pk", i).build()).get();
					if (i == nullPkId) {
						Assert.assertNull(r.getObject(0));
					} else {
						typeCaseData.getPredicate().run(i, r);
					}
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * allType.
	 * Method: put(Put put).
	 */
	@Test(dataProvider = "dirtyCaseData")
	public void testALLTypeInsertDirtyDataError(DataTypeTestUtil.TypeCaseData typeCaseData) throws Exception {
		if (properties == null) {
			return;
		}

		final int totalCount = 10;
		String typeName = typeCaseData.getName();

		HoloConfig config = buildConfig();
		// fixed fe not support jsonb and roaringbitmap now
		if ((typeName.equals("roaringbitmap") || typeName.equals("jsonb")) && config.isUseFixedFe()) {
			return;
		}
		config.setAppName("testALLTypeInsert");
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setRemoveU0000InTextColumnValue(false);

		// config.setUseLegacyPutHandler(true);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "\"" + "holo_client_type_dirty_data_" + typeName + "\"";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id " + typeCaseData.getColumnType() + ", pk int primary key)";
			execute(conn, new String[]{"CREATE EXTENSION if not exists roaringbitmap", dropSql, createSql});
			try {
				TableSchema schema = client.getTableSchema(tableName, true);

				for (int i = 0; i < totalCount; ++i) {
					Record record = Record.build(schema);
					if (i < totalCount - 2) {
						record.setObject(0, null);
					} else {
						record.setObject(0, typeCaseData.getSupplier().apply(i, conn.unwrap(BaseConnection.class)));
					}
					record.setObject(1, i);
					client.put(new Put(record));
				}

				try {
					client.flush();
					// bytea写入时，如果传过来不是byte[]，会转string然后getBytes，好像没法mock出脏数据
					if (!typeName.equals("bytea")) {
						Assert.fail("should throw dirty data exception");
					}
				} catch (HoloClientWithDetailsException e) {
					if (!e.getCode().isDirtyDataException()) {
						throw e;
					}
					System.out.println(e.getMessage());
				}
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

}
