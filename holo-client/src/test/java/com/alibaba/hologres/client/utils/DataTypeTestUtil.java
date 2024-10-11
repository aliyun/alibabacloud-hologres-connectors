package com.alibaba.hologres.client.utils;

import com.alibaba.hologres.client.model.Record;
import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.PgArray;
import org.postgresql.util.PGInterval;
import org.testng.Assert;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.UUID;
import java.util.function.BiFunction;

import static org.postgresql.jdbc.ArrayUtil.arrayConcat;

/**
 * util for data type test.
 */
public class DataTypeTestUtil {
	// fixed plan支持的数据类型, 结果与ResultSet对比，用于写入测试，如 fixed copy
	public static final TypeCaseData[] FIXED_PLAN_TYPE_DATA = new TypeCaseData[]{
			new TypeCaseData("int", (i, conn) -> i, (i, rs) -> Assert.assertEquals(rs.getInt(1), i.intValue())),
			new TypeCaseData("bigint", (i, conn) -> (long) i, (i, rs) -> Assert.assertEquals(rs.getLong(1), i.longValue())),
			new TypeCaseData("smallint", (i, conn) -> i.shortValue(), (i, rs) -> Assert.assertEquals(rs.getShort(1), i.shortValue())),
			new TypeCaseData("decimal", "numeric(6,5)", (i, conn) -> new BigDecimal(String.valueOf(i)), (i, rs) -> Assert.assertEquals(rs.getString(1), i + ".00000")),
			new TypeCaseData("bool", (i, conn) -> i % 2 == 0, (i, rs) -> Assert.assertEquals(rs.getBoolean(1), i % 2 == 0)),
			new TypeCaseData("float4", (i, conn) -> i.floatValue(), (i, rs) -> Assert.assertEquals(rs.getFloat(1), i.floatValue())),
			new TypeCaseData("float8", (i, conn) -> i.doubleValue(), (i, rs) -> Assert.assertEquals(rs.getDouble(1), i.doubleValue())),
			new TypeCaseData("timestamp", (i, conn) -> new Timestamp(i * 1000L + 123L), (i, rs) -> Assert.assertEquals(rs.getTimestamp(1), new Timestamp(i * 1000L + 123L))),
			new TypeCaseData("timestamptz", (i, conn) -> new Timestamp(i * 1000L + 123L), (i, rs) -> Assert.assertEquals(rs.getTimestamp(1), new Timestamp(i * 1000L + 123L))),
			new TypeCaseData("timestamp", (i, conn) -> LocalDateTime.ofEpochSecond((long) i, 123 * 1000000, ZoneOffset.systemDefault().getRules().getOffset(LocalDateTime.now())), (i, rs) -> Assert.assertEquals(rs.getTimestamp(1), new Timestamp(i * 1000L + 123L))),
			new TypeCaseData("timestamptz", (i, conn) -> LocalDateTime.ofEpochSecond((long) i, 123 * 1000000, ZoneOffset.systemDefault().getRules().getOffset(LocalDateTime.now())), (i, rs) -> Assert.assertEquals(rs.getTimestamp(1), new Timestamp(i * 1000L + 123L))),
			new TypeCaseData("date", (i, conn) -> java.sql.Date.valueOf("1900-01-02"), (i, rs) -> Assert.assertEquals(rs.getString(1), Date.valueOf("1900-01-02").toString())),
			new TypeCaseData("time", (i, conn) -> new Time(i * 1000L + 1234567L), (i, rs) -> Assert.assertEquals(rs.getTime(1), new Time(i * 1000L + 1234567L))),
			new TypeCaseData("timetz", (i, conn) -> new Time(i * 1000L + 1234567L), (i, rs) -> Assert.assertEquals(rs.getTime(1), new Time(i * 1000L + 1234567L))),
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
			new TypeCaseData("_int", "int[]", (i, conn) -> new Integer[]{i, i + 1}, (i, rs) -> Assert.assertEquals(rs.getString(1), "{" + i + "," + (i + 1) + "}")),
			new TypeCaseData("_int8", "int8[]", (i, conn) -> new Long[]{(long) i, (long) (i + 1)}, (i, rs) -> Assert.assertEquals(rs.getString(1), "{" + i + "," + (i + 1) + "}")),
			new TypeCaseData("_float4", "float4[]", (i, conn) -> new Float[]{(float) i, (float) (i + 1)}, (i, rs) -> Assert.assertEquals(rs.getString(1), "{" + i + "," + (i + 1) + "}")),
			new TypeCaseData("_float8", "float8[]", (i, conn) -> new Double[]{(double) i, (double) i + 1}, (i, rs) -> Assert.assertEquals(rs.getString(1), "{" + i + "," + (i + 1) + "}")),
			new TypeCaseData("_bool", "bool[]", (i, conn) -> new Boolean[]{i % 2 == 0, i % 2 != 0}, (i, rs) -> Assert.assertEquals(rs.getString(1), "{" + (i % 2 == 0 ? "t" : "f") + "," + (i % 2 != 0 ? "t" : "f") + "}")),
			new TypeCaseData("_text", "text[]", (i, conn) -> new String[]{i.toString(), String.valueOf(i + 1)}, (i, rs) -> Assert.assertEquals(rs.getString(1), "{" + i + "," + (i + 1) + "}")),
			new TypeCaseData("_varchar", "varchar[]", (i, conn) -> new String[]{String.valueOf(i), String.valueOf(i + 1)}, (i, rs) -> Assert.assertEquals(rs.getString(1), "{" + i + "," + (i + 1) + "}")),
			new TypeCaseData("roaringbitmap", (i, conn) -> new byte[]{58, 48, 0, 0, 1, 0, 0, 0, 0, 0, 2, 0, 16, 0, 0, 0, 1, 0, 4, 0, 5, 0} /*{1,4,5}*/, (i, rs) -> Assert.assertEquals(rs.getLong(1), 3L))
	};

	// 所有Holo-client支持的数据类型，结果与ResultSet对比，用于写入测试，如 put
	public static final TypeCaseData[] ALL_TYPE_DATA = arrayConcat(FIXED_PLAN_TYPE_DATA, new TypeCaseData[]{
			new TypeCaseData("interval", (i, conn) -> i + " hours", (i, rs) -> Assert.assertEquals(rs.getString(1), intervalDF(i))),
			new TypeCaseData("inet", (i, conn) -> "127.0.0." + i, (i, rs) -> Assert.assertEquals(rs.getString(1), "127.0.0." + i)),
			new TypeCaseData("oid", (i, conn) -> i, (i, rs) -> Assert.assertEquals(rs.getInt(1), i.intValue())),
			new TypeCaseData("uuid", (i, conn) -> "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1" + i, (i, rs) -> Assert.assertEquals(rs.getString(1), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1" + i)),
			// 注意bit(n)和varbit(n)会截断
			new TypeCaseData("bit", "bit(3)", (i, conn) -> "101111", (i, rs) -> Assert.assertEquals(rs.getString(1), "101")),
			new TypeCaseData("varbit", "varbit(3)", (i, conn) -> "101111", (i, rs) -> Assert.assertEquals(rs.getString(1), "101"))
			//不支持的类型
			// , "MONEY" // 难搞，不支持了
	});

	// fixed plan支持的数据类型, 结果与Holo-client Record对比，用于读取测试，如 get
	public static final TypeCaseDataWithRecord[] FIXED_PLAN_TYPE_DATA_WITH_RECORD = new TypeCaseDataWithRecord[]{
			new TypeCaseDataWithRecord("int", (i, conn) -> i, (i, r) -> Assert.assertEquals(r.getObject(0).toString(), String.valueOf(i))),
			new TypeCaseDataWithRecord("bigint", (i, conn) -> (long) i, (i, r) -> Assert.assertEquals(r.getObject(0).toString(), String.valueOf(i))),
			new TypeCaseDataWithRecord("smallint", (i, conn) -> i.shortValue(), (i, r) -> Assert.assertEquals(r.getObject(0).toString(), String.valueOf(i))),
			new TypeCaseDataWithRecord("decimal", "numeric(6,5)", (i, conn) -> new BigDecimal(String.valueOf(i)), (i, r) -> Assert.assertEquals(r.getObject(0).toString(), i + ".00000")),
			new TypeCaseDataWithRecord("bool", (i, conn) -> i % 2 == 0, (i, r) -> Assert.assertEquals(r.getObject(0), i % 2 == 0)),
			new TypeCaseDataWithRecord("float4", (i, conn) -> i.floatValue(), (i, r) -> Assert.assertEquals(r.getObject(0), (float) (i))),
			new TypeCaseDataWithRecord("float8", (i, conn) -> i.doubleValue(), (i, r) -> Assert.assertEquals(r.getObject(0), (double) (i))),
			new TypeCaseDataWithRecord("timestamp", (i, conn) -> createTimestampWithNanos(i), (i, r) -> Assert.assertEquals(r.getObject(0), createTimestampWithNanos(i))),
			new TypeCaseDataWithRecord("timestamptz", (i, conn) -> new Timestamp(i * 1000L + 123L), (i, r) -> Assert.assertEquals(r.getObject(0), new Timestamp(i * 1000L + 123L))),
			new TypeCaseDataWithRecord("date", (i, conn) -> java.sql.Date.valueOf("1900-01-02"), (i, r) -> Assert.assertEquals(r.getObject(0).toString(), Date.valueOf("1900-01-02").toString())),
			new TypeCaseDataWithRecord("json", (i, conn) -> "{\"a\":\"" + i + "\"}", (i, r) -> Assert.assertEquals(r.getObject(0).toString(), "{\"a\":\"" + i + "\"}")),
			new TypeCaseDataWithRecord("jsonb", (i, conn) -> "{\"a\":\"" + i + "\"}", (i, r) -> Assert.assertEquals(r.getObject(0).toString(), "{\"a\": \"" + i + "\"}")),
			new TypeCaseDataWithRecord("bytea", (i, conn) -> new byte[]{i.byteValue(), (byte) (i.byteValue() + 1), (byte) (i + 2)}, (i, r) -> Assert.assertEquals(r.getObject(0), new byte[]{i.byteValue(), (byte) (i.byteValue() + 1), (byte) (i + 2)})),
			new TypeCaseDataWithRecord("char", "char(5)", (i, conn) -> i.toString(), (i, r) -> Assert.assertEquals(r.getObject(0).toString(), (i + "         ").substring(0, 5))),
			new TypeCaseDataWithRecord("varchar", "varchar(20)", (i, conn) -> i.toString(), (i, r) -> Assert.assertEquals(r.getObject(0).toString(), String.valueOf(i))),
			new TypeCaseDataWithRecord("text", (i, conn) -> i.toString(), (i, r) -> Assert.assertEquals(r.getObject(0).toString(), String.valueOf(i))),
			new TypeCaseDataWithRecord("_int", "int[]", (i, conn) -> new int[]{i, i + 1}, (i, r) -> Assert.assertEquals((int[]) unPickPgArray(r.getObject(0)), new int[]{i, i + 1})),
			new TypeCaseDataWithRecord("_int8", "int8[]", (i, conn) -> new long[]{i, i + 1}, (i, r) -> Assert.assertEquals((long[]) unPickPgArray(r.getObject(0)), new long[]{(long) i, (long) (i + 1)})),
			new TypeCaseDataWithRecord("_float4", "float4[]", (i, conn) -> new float[]{i, i + 1}, (i, r) -> Assert.assertEquals((float[]) unPickPgArray(r.getObject(0)), new float[]{(float) i, (float) (i + 1)})),
			new TypeCaseDataWithRecord("_float8", "float8[]", (i, conn) -> new double[]{i, i + 1}, (i, r) -> Assert.assertEquals((double[]) unPickPgArray(r.getObject(0)), new double[]{(double) i, (double) (i + 1)})),
			new TypeCaseDataWithRecord("_bool", "bool[]", (i, conn) -> new boolean[]{i % 2 == 0, i % 2 != 0}, (i, r) -> Assert.assertEquals((boolean[]) unPickPgArray(r.getObject(0)), new boolean[]{i % 2 == 0, i % 2 != 0})),
			new TypeCaseDataWithRecord("_text", "text[]", (i, conn) -> new String[]{i.toString(), String.valueOf(i + 1)}, (i, r) -> Assert.assertEquals((String[]) unPickPgArray(r.getObject(0)), new String[]{String.valueOf(i), String.valueOf(i + 1)})),
			new TypeCaseDataWithRecord("_varchar", "varchar[]", (i, conn) -> new String[]{String.valueOf(i), String.valueOf(i + 1)}, (i, r) -> Assert.assertEquals((String[]) unPickPgArray(r.getObject(0)), new String[]{String.valueOf(i), String.valueOf(i + 1)})),
			new TypeCaseDataWithRecord("roaringbitmap", (i, conn) -> new byte[]{58, 48, 0, 0, 1, 0, 0, 0, 0, 0, 2, 0, 16, 0, 0, 0, 1, 0, 4, 0, 5, 0} /*{1,4,5}*/, (i, r) -> Assert.assertEquals(r.getObject(0), new byte[]{58, 48, 0, 0, 1, 0, 0, 0, 0, 0, 2, 0, 16, 0, 0, 0, 1, 0, 4, 0, 5, 0}))
	};

	// 所有Holo-client支持的数据类型，结果与Holo-client Record对比，用于读取测试，如 get
	public static final TypeCaseDataWithRecord[] ALL_TYPE_DATA_WITH_RECORD = arrayConcat(FIXED_PLAN_TYPE_DATA_WITH_RECORD, new TypeCaseDataWithRecord[]{
			new TypeCaseDataWithRecord("interval", (i, conn) -> i + " hours", (i, r) -> {
				PGInterval interval = new PGInterval();
				interval.setHours(i);
				Assert.assertEquals(r.getObject(0), interval);
			}),
			new TypeCaseDataWithRecord("inet", (i, conn) -> "127.0.0." + i, (i, r) -> Assert.assertEquals(r.getObject(0).toString(), "127.0.0." + i)),
			new TypeCaseDataWithRecord("timetz", (i, conn) -> new Time(i * 1000L + 123L), (i, r) -> Assert.assertEquals(r.getObject(0), new Time(i * 1000L + 123L))),
			new TypeCaseDataWithRecord("time", (i, conn) -> new Time(i * 1000L + 123L), (i, r) -> Assert.assertEquals(r.getObject(0), new Time(i * 1000L + 123L))),
			new TypeCaseDataWithRecord("oid", (i, conn) -> i, (i, r) -> Assert.assertEquals(r.getObject(0).toString(), String.valueOf(i))),
			new TypeCaseDataWithRecord("uuid", (i, conn) -> "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1" + i, (i, r) -> Assert.assertEquals(r.getObject(0), UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1" + i))),
			new TypeCaseDataWithRecord("bit", (i, conn) -> "0", (i, r) -> Assert.assertEquals(r.getObject(0), false)),
			new TypeCaseDataWithRecord("varbit", "varbit(3)", (i, conn) -> "101", (i, r) -> Assert.assertEquals(r.getObject(0).toString(), "101"))
			// , "MONEY" // 不支持的类型
	});

	public static final TypeCaseData[] EXCEPTION_ALL_TYPE_DATA = new TypeCaseData[]{
			new TypeCaseData("bigint_number_format", "bigint", (i, conn) -> "abc", null),
			new TypeCaseData("smallint_number_format", "smallint", (i, conn) -> "abc", null),
			new TypeCaseData("decimal_overflow", "numeric(6,5)", (i, conn) -> new BigDecimal("12.5"), null),
			new TypeCaseData("decimal_inalid_type", "numeric(6,5)", (i, conn) -> "", null),
			new TypeCaseData("bool_cannot_coerce", "bool", (i, conn) -> "abc", null),
			new TypeCaseData("float4_number_format", "real", (i, conn) -> "abc", null),
			new TypeCaseData("float8_number_format", "float8", (i, conn) -> "abc", null),
			new TypeCaseData("timestamp_datetime_format", "timestamp", (i, conn) -> "abc", null),
			new TypeCaseData("timestamptz_datetime_format", "timestamptz", (i, conn) -> "abc", null),
			new TypeCaseData("date_datetime_format", "date", (i, conn) -> "abc", null),
			new TypeCaseData("time_datetime_format", "time", (i, conn) -> "abc", null),
			new TypeCaseData("json_u0000", "json", (i, conn) -> "{\"a\":\"" + i + "\u0000\"}", null),
			new TypeCaseData("jsonb_u0000_case0", "jsonb", (i, conn) -> "{\"a\":\"" + i + "\\u0000\"}", null),
			new TypeCaseData("jsonb_u0000_case1", "jsonb", (i, conn) -> "{\"a\":\"" + i + "\u0000\"}", null),
			new TypeCaseData("bytea", (i, conn) -> "aaaa-1", null),
			new TypeCaseData("char_too_long", "char(5)", (i, conn) -> "123456", null),
			new TypeCaseData("varchar_too_long", "varchar(5)", (i, conn) -> "123456", null),
			new TypeCaseData("char_u0000", "char(5)", (i, conn) -> "1\u0000", null),
			new TypeCaseData("varchar_u0000", "varchar(5)", (i, conn) -> "1\u0000", null),
			new TypeCaseData("text_u0000", "text", (i, conn) -> "1\u0000", null),
			new TypeCaseData("_text_u0000", "text[]", (i, conn) -> new String[]{"\u0000123"}, null),
			new TypeCaseData("_int_element_type", "int[]", (i, conn) -> new String[]{"abc"}, null),
			new TypeCaseData("_varchar_element_too_long", "varchar(5)[]", (i, conn) -> new String[]{"1234567"}, null),
			new TypeCaseData("_varchar_u0000", "varchar[]", (i, conn) -> new String[]{"\u00001"}, null)
	};


	/**
	 * PredicateWithException.
	 */
	public interface PredicateWithException<T, U, E extends Exception> {
		void run(T t, U u) throws E;

	}

	/**
	 * TypeCaseData，用于数据类型测试.
	 * 为某个数据类型准备写入的值，结果和通过jdbc查询出来的ResultSet比较
	 */
	public static class TypeCaseData {
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

	/**
	 * TypeCaseDataWithRecord，用于数据类型测试.
	 * 为某个数据类型准备写入的值，结果和holo-client查询出来的Record比较
	 */
	public static class TypeCaseDataWithRecord {
		String name;
		String columnType;
		PredicateWithException<Integer, Record, SQLException> predicate;
		BiFunction<Integer, BaseConnection, Object> supplier;

		public TypeCaseDataWithRecord(String name, BiFunction<Integer, BaseConnection, Object> supplier, PredicateWithException<Integer, Record, SQLException> predicate) {
			this.name = name;
			this.columnType = name;
			this.predicate = predicate;
			this.supplier = supplier;
		}

		public TypeCaseDataWithRecord(String name, String columnType, BiFunction<Integer, BaseConnection, Object> supplier, PredicateWithException<Integer, Record, SQLException> predicate) {
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

		public PredicateWithException<Integer, Record, SQLException> getPredicate() {
			return predicate;
		}

		public BiFunction<Integer, BaseConnection, Object> getSupplier() {
			return supplier;
		}

		@Override
		public String toString() {
			return "TypeCaseDataWithRecord{" +
					"name='" + name + '\'' +
					'}';
		}
	}

	private static String intervalDF(int i) throws SQLException {
		DateFormat intervalDF = new SimpleDateFormat("HH:mm:ss");
		long intervalBase = 0L;
		try {
			intervalBase = intervalDF.parse("00:00:00").getTime();
		} catch (ParseException e) {
			throw new SQLException(e);
		}
		return intervalDF.format(new java.util.Date(intervalBase + i * 3600000L));
	}

	// 将pgArray转为基本类型数组，方便Get测试和读Binlog测试可以复用TypeCaseDataWithRecord
	private static Object unPickPgArray(Object object) throws SQLException {
		if (object instanceof PgArray) {
			object = ((PgArray) object).getArray();
			if (object instanceof Integer[]) {
				return Arrays.stream((Integer[]) object)
						.map(v -> v == null ? 0 : v)
						.mapToInt(Integer::intValue)
						.toArray();
			}
			if (object instanceof Long[]) {
				return Arrays.stream((Long[]) object)
						.map(v -> v == null ? 0 : v)
						.mapToLong(Long::longValue)
						.toArray();
			}
			if (object instanceof Float[]) {
				Float[] in = (Float[]) object;
				float[] out = new float[in.length];
				for (int i = 0; i < in.length; i++) {
					out[i] = (in[i] == null) ? 0 : in[i];
				}
				return out;
			}
			if (object instanceof Double[]) {
				return Arrays.stream((Double[]) object)
						.map(v -> v == null ? 0 : v)
						.mapToDouble(Double::doubleValue)
						.toArray();
			}
			if (object instanceof Boolean[]) {
				Boolean[] in = (Boolean[]) object;
				boolean[] out = new boolean[in.length];
				for (int i = 0; i < in.length; i++) {
					out[i] = in[i] != null && in[i];
				}
				return out;
			}
		}

		return object;
	}

	private static Timestamp createTimestampWithNanos(int i) {
		Timestamp timestamp = new Timestamp(i * 1000L + 123L);
		// holo的timestamp类型精度到微秒
		timestamp.setNanos(123456 * 1000);
		return timestamp;
	}
}
