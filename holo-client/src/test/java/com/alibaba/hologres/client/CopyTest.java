/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.copy.CopyInOutputStream;
import com.alibaba.hologres.client.copy.CopyUtil;
import com.alibaba.hologres.client.copy.RecordBinaryOutputStream;
import com.alibaba.hologres.client.copy.RecordOutputStream;
import com.alibaba.hologres.client.copy.RecordTextOutputStream;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.hologres.client.utils.RecordChecker;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.PgConnection;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.function.BiFunction;

/**
 * Fixed Copy测试用例.
 */
public class CopyTest extends HoloClientTestBase {

	private static final TypeCaseData[] ALL_TYPE_DATA = new TypeCaseData[]{
			new TypeCaseData("bigint", (i, conn) -> (long) i, (i, rs) -> Assert.assertEquals(rs.getLong(1), i.longValue())),
			new TypeCaseData("smallint", (i, conn) -> i.shortValue(), (i, rs) -> Assert.assertEquals(rs.getShort(1), i.shortValue())),
			new TypeCaseData("decimal", "numeric(6,5)", (i, conn) -> new BigDecimal(String.valueOf(i)), (i, rs) -> Assert.assertEquals(rs.getString(1), i + ".00000")),
			new TypeCaseData("bool", (i, conn) -> i % 2 == 0, (i, rs) -> Assert.assertEquals(rs.getBoolean(1), i % 2 == 0)),
			new TypeCaseData("float4", (i, conn) -> i.floatValue(), (i, rs) -> Assert.assertEquals(rs.getFloat(1), i.floatValue())),
			new TypeCaseData("float8", (i, conn) -> i.doubleValue(), (i, rs) -> Assert.assertEquals(rs.getDouble(1), i.doubleValue())),
			new TypeCaseData("timestamp", (i, conn) -> new Timestamp(i * 1000L + 123L), (i, rs) -> Assert.assertEquals(rs.getTimestamp(1), new Timestamp(i * 1000L + 123L))),
			new TypeCaseData("timestamptz", (i, conn) -> new Timestamp(i * 1000L + 123L), (i, rs) -> Assert.assertEquals(rs.getTimestamp(1), new Timestamp(i * 1000L + 123L))),
			new TypeCaseData("date", (i, conn) -> Date.valueOf(LocalDate.ofEpochDay(2)), (i, rs) -> Assert.assertEquals(rs.getString(1), Date.valueOf(LocalDate.ofEpochDay(2)).toString())),
			new TypeCaseData("json", (i, conn) -> "{\"a\":\"" + i + "\"}", (i, rs) -> Assert.assertEquals(rs.getString(1), "{\"a\":\"" + i + "\"}")),
			new TypeCaseData("jsonb", (i, conn) -> "{\"a\":\"" + i + "\"}", (i, rs) -> Assert.assertEquals(rs.getString(1), "{\"a\": \"" + i + "\"}")),
			new TypeCaseData("bytea", (i, conn) -> new byte[]{i.byteValue(), (byte) (i.byteValue() + 1), (byte) (i + 2)}, (i, rs) -> Assert.assertEquals(rs.getBytes(1), new byte[]{i.byteValue(), (byte) (i.byteValue() + 1), (byte) (i + 2)})),
			new TypeCaseData("char", "char(5)", (i, conn) -> i.toString(), (i, rs) -> Assert.assertEquals(rs.getString(1), (i + "         ").substring(0, 5))),
			new TypeCaseData("varchar", "varchar(20)", (i, conn) -> i.toString(), (i, rs) -> Assert.assertEquals(rs.getString(1), i.toString())),
			new TypeCaseData("text", (i, conn) -> i.toString(), (i, rs) -> Assert.assertEquals(rs.getString(1), i.toString())),
			new TypeCaseData("_int", "int[]", (i, conn) -> new int[]{i, i + 1}, (i, rs) -> Assert.assertEquals(rs.getString(1), "{" + i + "," + (i + 1) + "}")),
			new TypeCaseData("_int8", "int8[]", (i, conn) -> new long[]{i, i + 1}, (i, rs) -> Assert.assertEquals(rs.getString(1), "{" + i + "," + (i + 1) + "}")),
			new TypeCaseData("_float4", "float4[]", (i, conn) -> new float[]{i, i + 1}, (i, rs) -> Assert.assertEquals(rs.getString(1), "{" + i + "," + (i + 1) + "}")),
			new TypeCaseData("_float8", "float8[]", (i, conn) -> new double[]{i, i + 1}, (i, rs) -> Assert.assertEquals(rs.getString(1), "{" + i + "," + (i + 1) + "}")),
			new TypeCaseData("_bool", "bool[]", (i, conn) -> new boolean[]{i % 2 == 0, i % 2 != 0}, (i, rs) -> Assert.assertEquals(rs.getString(1), "{" + (i % 2 == 0 ? "t" : "f") + "," + (i % 2 != 0 ? "t" : "f") + "}")),
			new TypeCaseData("_text", "text[]", (i, conn) -> new String[]{i.toString(), String.valueOf(i + 1)}, (i, rs) -> Assert.assertEquals(rs.getString(1), "{" + i + "," + (i + 1) + "}")),
			new TypeCaseData("_varchar", "varchar[]", (i, conn) -> new String[]{String.valueOf(i), String.valueOf(i + 1)}, (i, rs) -> Assert.assertEquals(rs.getString(1), "{" + i + "," + (i + 1) + "}")),
			new TypeCaseData("roaringbitmap", (i, conn) -> new byte[]{58, 48, 0, 0, 1, 0, 0, 0, 0, 0, 2, 0, 16, 0, 0, 0, 1, 0, 4, 0, 5, 0} /*{1,4,5}*/, (i, rs) -> Assert.assertEquals(rs.getLong(1), 3L))
	};


	private static final TypeCaseData[] EXCEPTION_ALL_TYPE_DATA = new TypeCaseData[]{
			new TypeCaseData("bigint", (i, conn) -> "abc", null),
			new TypeCaseData("smallint", (i, conn) -> "abc", null),
			new TypeCaseData("decimal_overflow", "numeric(6,5)", (i, conn) -> new BigDecimal("12.5"), null),
			new TypeCaseData("decimal_inalid_type", "numeric(6,5)", (i, conn) -> "", null),
			new TypeCaseData("bool", (i, conn) -> "abc", null),
			new TypeCaseData("float4", (i, conn) -> "abc", null),
			new TypeCaseData("float8", (i, conn) -> "abc", null),
			new TypeCaseData("timestamp", (i, conn) -> "abc", null),
			new TypeCaseData("timestamptz", (i, conn) -> "abc", null),
			new TypeCaseData("date", (i, conn) -> "abc", null),
			new TypeCaseData("json_u0000_case0", "json", (i, conn) -> "{\"a\":\"" + i + "\\u0000\"}", null),
			new TypeCaseData("json_u0000_case1", "json", (i, conn) -> "{\"a\":\"" + i + "\u0000\"}", null),
			new TypeCaseData("jsonb_u0000_case0", "jsonb", (i, conn) -> "{\"a\":\"" + i + "\\u0000\"}", null),
			new TypeCaseData("jsonb_u0000_case1", "jsonb", (i, conn) -> "{\"a\":\"" + i + "\u0000\"}", null),
			new TypeCaseData("bytea", (i, conn) -> "aaaa", null),
			new TypeCaseData("char", "char(5)", (i, conn) -> "123456", null),
			new TypeCaseData("varchar", "varchar(5)", (i, conn) -> "123456", null),
			new TypeCaseData("char_u0000", "char(5)", (i, conn) -> "1\u0000", null),
			new TypeCaseData("varchar_u0000", "varchar(5)", (i, conn) -> "1\u0000", null),
			new TypeCaseData("text", (i, conn) -> "1\u0000", null),
			new TypeCaseData("_int", "int[]", (i, conn) -> new Integer[]{i, i + 1}, null),
			new TypeCaseData("_int8", "int8[]", (i, conn) -> new Long[]{}, null),
			new TypeCaseData("_float4", "float4[]", (i, conn) -> new Float[]{}, null),
			new TypeCaseData("_float8", "float8[]", (i, conn) -> new Double[]{}, null),
			new TypeCaseData("_bool", "bool[]", (i, conn) -> new Double[]{}, null),
			new TypeCaseData("_text", "text[]", (i, conn) -> new String[]{"\u0000123"}, null),
			new TypeCaseData("_varchar", "varchar(5)[]", (i, conn) -> new String[]{"1234567"}, null),
			new TypeCaseData("_varchar_u0000", "varchar[]", (i, conn) -> new String[]{"\u00001"}, null)
	};
	//不支持的类型
	// , "INTERVAL"
	// , "TIMETZ"
	// , "TIME"
	//, "INET"
	// , "MONEY" // 难搞，不支持了
	//, "OID"
	//, "UUID"
	//, "BIT"
	//, "VARBIT"

	interface PredicateWithException<T, U, E extends Exception> {
		void run(T t, U u) throws E;

	}

	static class TypeCaseData {
		String name;
		String columnType;
		PredicateWithException<Integer, ResultSet, SQLException> predicate;
		BiFunction<Integer, BaseConnection, Object> supplier;

		public TypeCaseData(String name, BiFunction<Integer, BaseConnection, Object> supplier, PredicateWithException<Integer, ResultSet, SQLException> predicate) {
			this.name = name;
			this.columnType = name;
			this.predicate = predicate;
			this.supplier = supplier;
		}

		public TypeCaseData(String name, String columnType, BiFunction<Integer, BaseConnection, Object> supplier, PredicateWithException<Integer, ResultSet, SQLException> predicate) {
			this.name = name;
			this.columnType = columnType;
			this.predicate = predicate;
			this.supplier = supplier;
		}

		public String getName() {
			return name;
		}

		public String getColumnType() {
			return columnType;
		}

		public PredicateWithException<Integer, ResultSet, SQLException> getPredicate() {
			return predicate;
		}

		public BiFunction<Integer, BaseConnection, Object> getSupplier() {
			return supplier;
		}

		@Override
		public String toString() {
			return "TypeCaseData{" +
					"name='" + name + '\'' +
					'}';
		}
	}

	@DataProvider(name = "typeCaseData")
	public Object[][] createData() {
		Object[][] ret = new Object[ALL_TYPE_DATA.length * 2][];
		for (int i = 0; i < ALL_TYPE_DATA.length; ++i) {
			ret[2 * i] = new Object[]{ALL_TYPE_DATA[i], true};
			ret[2 * i + 1] = new Object[]{ALL_TYPE_DATA[i], false};
		}
		return ret;
	}

	@DataProvider(name = "exceptionTypeCaseData")
	public Object[][] createExceptionData() {
		Object[][] ret = new Object[EXCEPTION_ALL_TYPE_DATA.length][];
		for (int i = 0; i < EXCEPTION_ALL_TYPE_DATA.length; ++i) {
			ret[i] = new Object[]{EXCEPTION_ALL_TYPE_DATA[i], true};
		}
		return ret;
	}

	/**
	 * data type test.
	 */
	@Test(dataProvider = "typeCaseData")
	public void testCopy001(TypeCaseData typeCaseData, boolean binary) throws Exception {

		if (properties == null) {
			return;
		}

		final int totalCount = 10;
		final int nullPkId = 5;
		String typeName = typeCaseData.getName();
		try (Connection conn = buildConnection()) {
			String tableName = "\"holo_client_copy_type_001_" + typeName + "_" + binary + "\"";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id " + typeCaseData.getColumnType() + ", pk int primary key)";
			try {
				LOG.info("current type {}, binary {}", typeName, binary);

				execute(conn, new String[]{"CREATE EXTENSION if not exists roaringbitmap", dropSql, createSql});

				PgConnection pgConn = conn.unwrap(PgConnection.class);
				TableName tn = TableName.valueOf(tableName);

				HoloVersion version = ConnectionUtil.getHoloVersion(pgConn);
				ConnectionUtil.checkMeta(pgConn, version, tn.getFullName(), 120);

				TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);
				CopyManager copyManager = new CopyManager(conn.unwrap(PgConnection.class));
				String copySql = CopyUtil.buildCopyInSql(schema, binary, WriteMode.INSERT_OR_REPLACE);
				LOG.info("copySql : {}", copySql);

				try (OutputStream os = new CopyInOutputStream(copyManager.copyIn(copySql))) {
					RecordOutputStream ros = binary ?
							new RecordBinaryOutputStream(os, schema, pgConn.unwrap(BaseConnection.class), 1024 * 1024 * 10) :
							new RecordTextOutputStream(os, schema, pgConn.unwrap(BaseConnection.class), 1024 * 1024 * 10);
					//插入10条，id=5的插空值
					for (int i = 0; i < totalCount; ++i) {
						Record record = Record.build(schema);
						if (i == nullPkId) {
							record.setObject(0, null);
						} else {
							record.setObject(0, typeCaseData.getSupplier().apply(i, conn.unwrap(BaseConnection.class)));
						}
						record.setObject(1, i);
						ros.putRecord(record);
					}
				}

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
	 * buildCopyInSql from record test.
	 */
	@Test
	public void testCopy002() throws Exception {
		if (properties == null) {
			return;
		}
		boolean[] binaryList = new boolean[]{false, true};
		try (Connection conn = buildConnection()) {
			for (boolean binary : binaryList) {
				String tableName = "\"holo_client_copy_sql_002_" + binary + "\"";
				String dropSql = "drop table if exists " + tableName;
				String createSql = "create table " + tableName
						+ "(id int not null,name text not null,address text,primary key(id))";
				try {
					execute(conn, new String[]{dropSql, createSql});

					try (Connection pgConn = buildConnection().unwrap(PgConnection.class)) {
						HoloVersion version = ConnectionUtil.getHoloVersion(pgConn);
						TableName tn = TableName.valueOf(tableName);
						ConnectionUtil.checkMeta(pgConn, version, tn.getFullName(), 120);

						TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);
						CopyManager copyManager = new CopyManager(pgConn.unwrap(PgConnection.class));
						String copySql = null;
						OutputStream os = null;
						RecordOutputStream ros = null;
						for (int i = 0; i < 10; ++i) {
							Record record = new Record(schema);
							record.setObject(0, i);
							record.setObject(1, "name0");
							if (ros == null) {
								copySql = CopyUtil.buildCopyInSql(record, binary, WriteMode.INSERT_OR_UPDATE);
								LOG.info("copySql : {}", copySql);
								os = new CopyInOutputStream(copyManager.copyIn(copySql));
								ros = binary ?
										new RecordBinaryOutputStream(os, schema, pgConn.unwrap(PgConnection.class), 1024 * 1024 * 10) :
										new RecordTextOutputStream(os, schema, pgConn.unwrap(PgConnection.class), 1024 * 1024 * 10);
							}
							RecordChecker.check(record);
							// this record does not contain the third field address
							ros.putRecord(record);
						}
						ros.close();
					}

					int count = 0;
					try (Statement stat = conn.createStatement()) {
						try (ResultSet rs = stat.executeQuery("select * from " + tableName + " order by id")) {
							while (rs.next()) {
								Assert.assertEquals(count, rs.getInt(1));
								Assert.assertEquals("name0", rs.getString(2));
								++count;
							}
							Assert.assertEquals(10, count);
						}
					}
				} finally {
					execute(conn, new String[]{dropSql});
				}
			}
		}
	}

	@Test(dataProvider = "typeCaseData")
	public void testRecordChecker001(TypeCaseData typeCaseData, boolean binary) throws Exception {
		if (properties == null) {
			return;
		}
		try (Connection conn = buildConnection()) {
			{
				String tableName = "\"holo_client_record_checker_sql_001_" + binary + "\"";
				String dropSql = "drop table if exists " + tableName;
				String createSql = "create table " + tableName;

				createSql += "(c0" + " " + typeCaseData.getColumnType();

				createSql += ",id int not null,primary key(id))";
				try {

					execute(conn, new String[]{dropSql, createSql});

					PgConnection pgConn = conn.unwrap(PgConnection.class);

					HoloVersion version = ConnectionUtil.getHoloVersion(pgConn);
					TableName tn = TableName.valueOf(tableName);
					ConnectionUtil.checkMeta(pgConn, version, tn.getFullName(), 120);

					TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);
					CopyManager copyManager = new CopyManager(conn.unwrap(PgConnection.class));
					String copySql = null;
					OutputStream os = null;
					RecordOutputStream ros = null;
					{

						Record record = new Record(schema);
						Object obj = typeCaseData.getSupplier().apply(0, conn.unwrap(BaseConnection.class));
						record.setObject(0, obj);
						record.setObject(1, 1);
						RecordChecker.check(record);
						if (ros == null) {
							copySql = CopyUtil.buildCopyInSql(record, binary, WriteMode.INSERT_OR_UPDATE);
							LOG.info("copySql : {}", copySql);
							os = new CopyInOutputStream(copyManager.copyIn(copySql));
							ros = binary ?
									new RecordBinaryOutputStream(os, schema, pgConn.unwrap(PgConnection.class), 1024 * 1024 * 10) :
									new RecordTextOutputStream(os, schema, pgConn.unwrap(PgConnection.class), 1024 * 1024 * 10);
						}
						ros.putRecord(record);
					}
					ros.close();
					int count = 0;
					try (Statement stat = conn.createStatement()) {
						String sql = "select * from " + tableName;
						if ("roaringbitmap".equals(typeCaseData.getColumnType())) {
							sql = "select rb_cardinality(c0), id from " + tableName;
						}
						try (ResultSet rs = stat.executeQuery(sql)) {
							while (rs.next()) {
								typeCaseData.getPredicate().run(0, rs);
								++count;
							}
							Assert.assertEquals(1, count);
						}
					}

				} finally {
					execute(conn, new String[]{dropSql});
				}
			}
		}
	}

	@Test(dataProvider = "exceptionTypeCaseData")
	public void testRecordWriter002(TypeCaseData typeCaseData, boolean binary) throws Exception {
		if (properties == null) {
			return;
		}
		try (Connection conn = buildConnection()) {
			{
				String tableName = "\"holo_client_record_checker_sql_002_" + binary + "\"";
				String dropSql = "drop table if exists " + tableName;
				String createSql = "create table " + tableName;

				createSql += "(c0" + " " + typeCaseData.getColumnType();

				createSql += ",id int not null,primary key(id))";
				try {

					execute(conn, new String[]{dropSql, createSql});

					PgConnection pgConn = conn.unwrap(PgConnection.class);

					HoloVersion version = ConnectionUtil.getHoloVersion(pgConn);
					TableName tn = TableName.valueOf(tableName);
					ConnectionUtil.checkMeta(pgConn, version, tn.getFullName(), 120);

					TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);

					Record record = new Record(schema);
					Object obj = typeCaseData.getSupplier().apply(0, conn.unwrap(BaseConnection.class));
					record.setObject(0, obj);
					record.setObject(1, 1);
					Assert.expectThrows(HoloClientException.class, () -> RecordChecker.check(record));
				} finally {
					execute(conn, new String[]{dropSql});
				}
			}
		}
	}
}
