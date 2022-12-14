/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.utils;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Date;

/**
 * check Input Value is valid.
 * - CONSTRAINT VIOLATION
 */
public class RecordChecker {
	public static void check(Record record) throws HoloClientException {
		TableSchema schema = record.getSchema();
		for (int i = 0; i < schema.getColumnSchema().length; ++i) {
			checkObject(schema, schema.getColumn(i), record.getObject(i));
		}
	}

	private static void throwConstraintViolationException(TableSchema schema, Column column, Object value, String msg, Exception e) throws HoloClientException {
		StringBuilder sb = new StringBuilder();
		sb.append("invalid value [").append(value).append("]");
		if (value != null) {
			sb.append("(").append(value.getClass().getName()).append(")");
		}
		sb.append(" for column ").append(column.getName()).append(" of table ").append(schema.getTableName());
		if (msg != null) {
			sb.append(", reason=").append(msg);
		}
		HoloClientException ret = null;
		if (e != null) {
			ret = new HoloClientException(ExceptionCode.CONSTRAINT_VIOLATION, sb.toString(), e);
		} else {
			ret = new HoloClientException(ExceptionCode.CONSTRAINT_VIOLATION, sb.toString());
		}
		throw ret;

	}

	private static void checkObject(TableSchema schema, Column column, Object value) throws HoloClientException {
		if (value == null) {
			checkNull(schema, column);
		} else if (value instanceof Short) {
			checkShort(schema, column, (Short) value);
		} else if (value instanceof Integer) {
			checkInteger(schema, column, (Integer) value);
		} else if (value instanceof Long) {
			checkLong(schema, column, (Long) value);
		} else if (value instanceof BigDecimal) {
			checkBigDecimal(schema, column, (BigDecimal) value);
		} else if (value instanceof Float) {
			checkFloat(schema, column, (Float) value);
		} else if (value instanceof Double) {
			checkDouble(schema, column, (Double) value);
		} else if (value instanceof Boolean) {
			checkBoolean(schema, column, (Boolean) value);
		} else if (value instanceof Timestamp) { // Timestamp must before Date.
			checkTimeStamp(schema, column, (Timestamp) value);
		} else if (value instanceof Date) {
			checkDate(schema, column, (Date) value);
		} else if (value instanceof String[]) {
			checkStringArray(schema, column, (String[]) value);
		} else if (value instanceof int[]) {
			checkIntegerArray(schema, column, (int[]) value);
		} else if (value instanceof long[]) {
			checkLongArray(schema, column, (long[]) value);
		} else if (value instanceof float[]) {
			checkFloatArray(schema, column, (float[]) value);
		} else if (value instanceof double[]) {
			checkDoubleArray(schema, column, (double[]) value);
		} else if (value instanceof boolean[]) {
			checkBooleanArray(schema, column, (boolean[]) value);
		} else if (value instanceof byte[]) {
			switch (column.getType()) {
				case Types.OTHER:
					if ("roaringbitmap".equals(column.getTypeName())) {
						checkRoaringbitmap(schema, column, (byte[]) value);
						break;
					}
				case Types.BINARY:
					checkBytea(schema, column, (byte[]) value);
				default:
			}
		} else if (value instanceof String) {
			switch (column.getType()) {
				case Types.OTHER:
					if ("json".equals(column.getTypeName())) {
						checkJson(schema, column, (String) value);
						break;
					} else if ("jsonb".equals(column.getTypeName())) {
						checkJsonb(schema, column, (String) value);
						break;
					}
				default:
					checkString(schema, column, value.toString());
			}

		} else {
			checkString(schema, column, value.toString());
		}
	}

	private static void checkNull(TableSchema schema, Column column) throws HoloClientException {
		if (!column.getAllowNull() && column.getDefaultValue() == null) {
			throwConstraintViolationException(schema, column, null, "not allow null value", null);
		}
	}

	private static void checkShort(TableSchema schema, Column column, short value) throws HoloClientException {
		switch (column.getType()) {
			case Types.SMALLINT:
			case Types.INTEGER:
			case Types.BIGINT:
				break;
			default:
				throwConstraintViolationException(schema, column, value, "unsupported type " + column.getTypeName() + " for setShort method", null);
		}
	}

	private static void checkInteger(TableSchema schema, Column column, int value) throws HoloClientException {
		switch (column.getType()) {
			case Types.INTEGER:
			case Types.BIGINT:
				break;
			default:
				throwConstraintViolationException(schema, column, value, "unsupported type " + column.getTypeName() + " for setInteger method", null);
		}
	}

	private static void checkLong(TableSchema schema, Column column, long value) throws HoloClientException {
		switch (column.getType()) {
			case Types.BIGINT:
				break;
			default:
				throwConstraintViolationException(schema, column, value, "unsupported type " + column.getTypeName() + " for setLong method", null);
		}
	}

	private static void checkBigDecimal(TableSchema schema, Column column, BigDecimal value) throws HoloClientException {
		if (value == null) {
			checkNull(schema, column);
			return;
		}
		switch (column.getType()) {
			case Types.NUMERIC:
				if (column.getPrecision() > 0 && column.getPrecision() >= column.getScale()) {
					if (value.precision() - value.scale() > column.getPrecision() - column.getScale()) {
						throwConstraintViolationException(schema, column, value, "A field with precision " + column.getPrecision() + ", scale " + column.getScale() + " must round to an absolute value less than 10^" + (column.getPrecision() - column.getScale()), null);
					}
				}
				break;
			default:
				throwConstraintViolationException(schema, column, value, "unsupported type " + column.getTypeName() + " for setBigDecimal method", null);
		}
	}

	private static void checkBoolean(TableSchema schema, Column column, boolean value) throws HoloClientException {
		switch (column.getType()) {
			case Types.BOOLEAN:
				break;
			case Types.BIT:
				if ("bool".equals(column.getTypeName())) {
					break;
				}
			default:
				throwConstraintViolationException(schema, column, value, "unsupported type " + column.getTypeName() + " for setBoolean method", null);
		}
	}

	private static void checkFloat(TableSchema schema, Column column, float value) throws HoloClientException {
		switch (column.getType()) {
			case Types.REAL:
				break;
			default:
				throwConstraintViolationException(schema, column, value, "unsupported type " + column.getTypeName() + " for setFloat method", null);
		}
	}

	private static void checkDouble(TableSchema schema, Column column, double value) throws HoloClientException {
		switch (column.getType()) {
			case Types.DOUBLE:
				break;
			default:
				throwConstraintViolationException(schema, column, value, "unsupported type " + column.getTypeName() + " for setDouble method", null);
		}
	}

	private static void checkDate(TableSchema schema, Column column, Date value) throws HoloClientException {
		if (value == null) {
			checkNull(schema, column);
		}
		switch (column.getType()) {
			case Types.DATE:
				break;
			default:
				throwConstraintViolationException(schema, column, value, "unsupported type " + column.getTypeName() + " for setDate method", null);
		}
	}

	private static void checkTimeStamp(TableSchema schema, Column column, Timestamp value) throws HoloClientException {
		if (value == null) {
			checkNull(schema, column);
		}
		switch (column.getType()) {
			case Types.TIMESTAMP:
			case Types.TIMESTAMP_WITH_TIMEZONE:
				break;
			default:
				throwConstraintViolationException(schema, column, value, "unsupported type " + column.getTypeName() + " for setTimeStamp method", null);
		}
	}

	private static void checkString(TableSchema schema, Column column, String value) throws HoloClientException {
		if (value == null) {
			checkNull(schema, column);
		}
		if (value.contains("\u0000")) {
			throwConstraintViolationException(schema, column, value.replaceAll("\u0000", "\\\\u0000"), "invalid byte sequence for encoding \"UTF8\": 0x00", null);
		}
		switch (column.getType()) {
			case Types.VARCHAR:
			case Types.CHAR:
				if (column.getPrecision() > 0) {
					int precision = column.getPrecision();
					if (value.length() > precision) {
						throwConstraintViolationException(schema, column, value, "value too long for type character varying(" + precision + ")", null);
					}
				}
				break;
			default:
				throwConstraintViolationException(schema, column, value, "unsupported type " + column.getTypeName() + " for setString method", null);
		}
	}

	private static void checkJson(TableSchema schema, Column column, String value) throws HoloClientException {
		if (value == null) {
			checkNull(schema, column);
		}
		if (value.contains("\u0000")) {
			throwConstraintViolationException(schema, column, value.replaceAll("\u0000", "\\\\u0000"), "invalid byte sequence for encoding \"UTF8\": 0x00", null);
		}
		if (value.contains("\\u0000")) {
			throwConstraintViolationException(schema, column, value, "invalid byte sequence for encoding \"UTF8\": 0x00", null);
		}
		switch (column.getType()) {
			case Types.OTHER:
				if ("json".equals(column.getTypeName())) {
					break;
				}
			default:
				throwConstraintViolationException(schema, column, value, "unsupported type " + column.getTypeName() + " for setJson method", null);
		}
	}

	private static void checkJsonb(TableSchema schema, Column column, String value) throws HoloClientException {
		if (value == null) {
			checkNull(schema, column);
		}
		if (value.contains("\u0000")) {
			throwConstraintViolationException(schema, column, value.replaceAll("\u0000", "\\\\u0000"), "invalid byte sequence for encoding \"UTF8\": 0x00", null);
		}
		if (value.contains("\\u0000")) {
			throwConstraintViolationException(schema, column, value, "invalid byte sequence for encoding \"UTF8\": 0x00", null);
		}
		switch (column.getType()) {
			case Types.OTHER:
				if ("jsonb".equals(column.getTypeName())) {
					break;
				}
			default:
				throwConstraintViolationException(schema, column, value, "unsupported type " + column.getTypeName() + " for setJsonb method", null);
		}
	}

	private static void checkBytea(TableSchema schema, Column column, byte[] value) throws HoloClientException {
		if (value == null) {
			checkNull(schema, column);
		}

		switch (column.getType()) {
			case Types.BINARY:
				break;
			default:
				throwConstraintViolationException(schema, column, value, "unsupported type " + column.getTypeName() + " for setBytea method", null);
		}
	}

	private static void checkRoaringbitmap(TableSchema schema, Column column, byte[] value) throws HoloClientException {
		if (value == null) {
			checkNull(schema, column);
		}

		switch (column.getType()) {
			case Types.OTHER:
				if ("roaringbitmap".equals(column.getTypeName())) {
					break;
				}
			default:
				throwConstraintViolationException(schema, column, value, "unsupported type " + column.getTypeName() + " for setRoaringbitmap method", null);
		}
	}

	private static void checkStringArray(TableSchema schema, Column column, String[] value) throws HoloClientException {
		if (value == null) {
			checkNull(schema, column);
			return;
		}
		for (String str : value) {
			if (str == null) {
				throwConstraintViolationException(schema, column, Arrays.toString(value), "Not support null value in array", null);
			} else if (str.contains("\u0000")) {
				throwConstraintViolationException(schema, column, Arrays.toString(value).replaceAll("\u0000", "\\\\u0000"), "invalid byte sequence for encoding \"UTF8\": 0x00", null);
			}
		}
		switch (column.getType()) {
			case Types.ARRAY:
				if ("_text".equals(column.getTypeName())) {
					break;
				} else if ("_varchar".equals(column.getTypeName())) {
					if (column.getPrecision() > 0) {
						int precision = column.getPrecision();
						for (String str : value) {
							if (str.length() > precision) {
								throwConstraintViolationException(schema, column, Arrays.toString(value), "value too long for type character varying(" + precision + ")", null);
							}
						}
					}
					break;
				}
			default:
				throwConstraintViolationException(schema, column, value, "unsupported type " + column.getTypeName() + " for setStringArray method", null);
		}
	}

	private static void checkBooleanArray(TableSchema schema, Column column, boolean[] value) throws HoloClientException {
		if (value == null) {
			checkNull(schema, column);
			return;
		}
		switch (column.getType()) {
			case Types.ARRAY:
				if ("_bool".equals(column.getTypeName())) {
					break;
				}
			default:
				throwConstraintViolationException(schema, column, value, "unsupported type " + column.getTypeName() + " for setBooleanArray method", null);
		}
	}

	private static void checkFloatArray(TableSchema schema, Column column, float[] value) throws HoloClientException {
		if (value == null) {
			checkNull(schema, column);
			return;
		}
		switch (column.getType()) {
			case Types.ARRAY:
				if ("_float4".equals(column.getTypeName())) {
					break;
				}
			default:
				throwConstraintViolationException(schema, column, value, "unsupported type " + column.getTypeName() + " for setFloatArray method", null);
		}
	}

	private static void checkDoubleArray(TableSchema schema, Column column, double[] value) throws HoloClientException {
		if (value == null) {
			checkNull(schema, column);
			return;
		}
		switch (column.getType()) {
			case Types.ARRAY:
				if ("_float8".equals(column.getTypeName())) {
					break;
				}
			default:
				throwConstraintViolationException(schema, column, value, "unsupported type " + column.getTypeName() + " for setDoubleArray method", null);
		}
	}

	private static void checkIntegerArray(TableSchema schema, Column column, int[] value) throws HoloClientException {
		if (value == null) {
			checkNull(schema, column);
			return;
		}
		switch (column.getType()) {
			case Types.ARRAY:
				if ("_int4".equals(column.getTypeName())) {
					break;
				}
			default:
				throwConstraintViolationException(schema, column, value, "unsupported type " + column.getTypeName() + " for setIntegerArray method", null);
		}
	}

	private static void checkLongArray(TableSchema schema, Column column, long[] value) throws HoloClientException {
		if (value == null) {
			checkNull(schema, column);
			return;
		}
		switch (column.getType()) {
			case Types.ARRAY:
				if ("_int8".equals(column.getTypeName())) {
					break;
				}
			default:
				throwConstraintViolationException(schema, column, value, "unsupported type " + column.getTypeName() + " for setLongArray method", null);
		}
	}
}
