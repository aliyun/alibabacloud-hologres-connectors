/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.copy;

import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.ArrayUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Record转pg binary流.
 */
public class RecordBinaryOutputStream extends RecordOutputStream {


	boolean fillHeader = false;

	public RecordBinaryOutputStream(OutputStream os, TableSchema schema, BaseConnection conn, int maxCellBufferSize) {
		super(os, schema, conn, maxCellBufferSize);
	}

	private void fillHeader() throws IOException {
		write("PGCOPY\n".getBytes(UTF8));
		write(0xFF);
		write("\r\n".getBytes(UTF8));
		write(0);
		writeInt(0);
		writeInt(0);
	}

	@Override
	protected void fillByteBuffer(Record record) throws IOException {
		if (!fillHeader) {
			fillHeader = true;
			fillHeader();
		}

		writeShort((short) record.getBitSet().cardinality());
		int index = 0;
		for (Column column : record.getSchema().getColumnSchema()) {
			try {
				if (record.isSet(index)) {
					fillByteBuffer(
							record.getObject(index),
							column);
				}
			} catch (Exception e) {
				throw new IOException(
						"fail to convert column "
								+ column.getName()
								+ " type "
								+ column.getTypeName()
								+ " value "
								+ record.getObject(index)
								+ " to binary",
						e);
			}
			++index;
		}
	}

	private void fillByteBuffer(Object obj, Column column)
			throws IOException {
		if (obj == null) {
			writeInt(-1);
			return;
		}
		int type = column.getType();
		String typeName = column.getTypeName();
		String columnName = column.getName();
		switch (type) {
			case Types.SMALLINT:
				if (obj instanceof Number) {
					writeInt(2);
					writeShort(((Number) obj).shortValue());
					break;
				} else {
					throw new IOException("unsupported class for int2 : " + obj.getClass().getName());
				}
			case Types.INTEGER:
				if (obj instanceof Number) {
					writeInt(4);
					writeInt(((Number) obj).intValue());
					break;
				} else {
					throw new IOException("unsupported class for int4 : " + obj.getClass().getName());
				}
			case Types.BIGINT:
				if (obj instanceof Number) {
					writeInt(8);
					writeLong(((Number) obj).longValue());
					break;
				} else {
					throw new IOException("unsupported class for int8 : " + obj.getClass().getName());
				}
			case Types.VARCHAR:
			case Types.CHAR:
				byte[] bytes = obj.toString().getBytes(UTF8);
				writeInt(bytes.length);
				write(bytes);
				break;
			case Types.BIT:
				if ("bool".equals(typeName)) {
					if (obj instanceof Boolean) {
						writeInt(1);
						write((Boolean) obj ? 1 : 0);
					} else if (obj instanceof Number) {
						writeInt(1);
						write(((Number) obj).intValue() > 0 ? 1 : 0);
					} else {
						throw new IOException("unsupported class for bool : " + obj.getClass().getName());
					}
				} else {
					throw new IOException("unsupported type:" + typeName);
				}
				break;

			case Types.REAL:
				if ("float4".equals(typeName)) {
					if (obj instanceof Float) {
						writeInt(4);
						writeFloat((Float) obj);
					} else if (obj instanceof Number) {
						writeInt(4);
						writeFloat(((Number) obj).floatValue());
					} else {
						throw new IOException("unsupported class for bool : " + obj.getClass().getName());
					}
				} else {
					throw new IOException("unsupported type:" + typeName);
				}
				break;
			case Types.DOUBLE:
				if ("float8".equals(typeName)) {
					if (obj instanceof Double) {
						writeInt(8);
						writeDouble((Double) obj);
					} else if (obj instanceof Number) {
						writeInt(8);
						writeDouble(((Number) obj).doubleValue());
					} else {
						throw new IOException("unsupported class for bool : " + obj.getClass().getName());
					}
				} else {
					throw new IOException("unsupported type:" + typeName);
				}
				break;
			case Types.DATE:
				byte[] val = new byte[4];
				try {
					if (obj instanceof java.sql.Date) {
						timestampUtils.toBinDate(null, val, (Date) obj);
					} else if (obj instanceof java.util.Date) {
						Date tmpd = new java.sql.Date(((java.util.Date) obj).getTime());
						timestampUtils.toBinDate(null, val, tmpd);
					} else if (obj instanceof String) {
						timestampUtils.toBinDate(null, val, timestampUtils.toDate(null, (String) obj));
					} else {
						throw new IOException("unsupported class for date : " + obj.getClass().getName());
					}
					writeInt(4);
					write(val);
				} catch (SQLException e) {
					throw new IOException(e);
				}
				break;
			case Types.TIMESTAMP:
				if (obj instanceof Timestamp) {
					Timestamp ts = (Timestamp) obj;
					long seconds = javaEpochToPg(ts.getTime() / 1000, TimeUnit.SECONDS);
					// Convert to micros rounding nanoseconds
					long micros =
							TimeUnit.SECONDS.toMicros(seconds)
									+ TimeUnit.NANOSECONDS.toMicros(ts.getNanos() + 500);
					if ("timestamp".equals(typeName)) {
						micros += TimeZone.getDefault().getRawOffset() * 1000L;
					}
					writeInt(8);
					writeLong(micros);
				} else if (obj instanceof String) {
					OffsetDateTime dateTime =
							OffsetDateTime.parse((String) obj, DATE_TIME_FORMATTER);
					long seconds = javaEpochToPg(dateTime.toEpochSecond(), TimeUnit.SECONDS);
					// Convert to micros rounding nanoseconds
					long micros =
							TimeUnit.SECONDS.toMicros(seconds)
									+ TimeUnit.NANOSECONDS.toMicros(dateTime.getNano() + 500);
					writeInt(8);
					writeLong(micros);
				} else if (obj instanceof Number) {
					long ms = ((Number) obj).longValue();
					long seconds = javaEpochToPg(ms / 1000L, TimeUnit.SECONDS);
					// Convert to micros rounding nanoseconds
					long micros =
							TimeUnit.SECONDS.toMicros(seconds)
									+ TimeUnit.NANOSECONDS.toMicros((ms % 1000) * 1000000L + 500);
					writeInt(8);
					writeLong(micros);
				} else if (obj instanceof java.util.Date) {
					long ms = ((java.util.Date) obj).getTime();
					long seconds = javaEpochToPg(ms / 1000L, TimeUnit.SECONDS);
					// Convert to micros rounding nanoseconds
					long micros =
							TimeUnit.SECONDS.toMicros(seconds)
									+ TimeUnit.NANOSECONDS.toMicros((ms % 1000) * 1000000L + 500);
					writeInt(8);
					writeLong(micros);
				} else {
					throw new RuntimeException(
							"unsupported type for timestamp " + obj.getClass().getName());
				}
				break;
			case Types.BINARY:
				if (obj instanceof byte[]) {
					byte[] binary = (byte[]) obj;
					writeInt(binary.length);
					write(binary);
				}
				break;
			case Types.NUMERIC:
				BigDecimal decimal;
				if (obj instanceof String) {
					decimal = new BigDecimal((String) obj);
				} else if (obj instanceof BigDecimal) {
					decimal = (BigDecimal) obj;
				} else if (obj instanceof Integer) {
					decimal = new BigDecimal((Integer) obj);
				} else if (obj instanceof Long) {
					decimal = new BigDecimal((Long) obj);
				} else {
					throw new RuntimeException(
							"unsupported type for numeric " + obj.getClass().getName());
				}
				decimal = decimal.setScale(column.getScale(), RoundingMode.HALF_UP);
				String num = decimal.toPlainString();

				short[] info = new short[3];
				short[] digits = encodeFromString(num, info);
				writeInt((4 + digits.length) * 2);
				writeShort((short) digits.length);

				writeShort(info[0]); //weight
				writeShort(info[1]); //sign
				writeShort(info[2]); //displayScale

				for (short digit : digits) {
					writeShort(digit);
				}
				break;

			case Types.OTHER:
				if ("jsonb".equals(typeName)) {
					byte[] jsonBytes = String.valueOf(obj).getBytes(UTF8);
					writeInt(jsonBytes.length + 1);
					write(1);
					write(jsonBytes);
					break;
				} else if ("json".equals(typeName)) {
					byte[] jsonBytes = String.valueOf(obj).getBytes(UTF8);
					writeInt(jsonBytes.length);
					write(jsonBytes);
					break;
				} else if ("roaringbitmap".equals(typeName)) {
					if (obj instanceof byte[]) {
						byte[] rbBytes = (byte[]) obj;
						writeInt(rbBytes.length);
						write(rbBytes);
					} else {
						throw new RuntimeException(
								"unsupported type for roaringbitmap " + obj.getClass().getName());
					}
					break;
				} else {
					throw new IOException("unsupported type:" + typeName + "(" + type + ")");
				}
			case Types.ARRAY:
				if (conn == null) {
					throw new IOException("unsupported type:" + typeName + "(" + type + "). Please call RecordBinaryOutputSteam constructor with BaseConnection Param");
				}
				try {
					byte[] arrayBytes = ArrayUtil.arrayToBinary(conn, obj, column.getTypeName());
					writeInt(arrayBytes.length);
					write(arrayBytes);
				} catch (SQLException e) {
					throw new IOException(e);
				}
				break;
			default:
				throw new IOException("unsupported type:" + typeName + "(" + type + ")");
		}
	}

	private static final short NUMERIC_POS = (short) 0x0000;
	private static final short NUMERIC_NEG = (short) 0x4000;
	private static final short DEC_DIGITS = 4;

	private static short[] encodeFromString(String num, short[] info) {

		char[] numChars = num.toCharArray();
		byte[] numDigs = new byte[numChars.length - 1 + DEC_DIGITS * 2];
		int ch = 0;
		int digs = DEC_DIGITS;
		boolean haveDP = false;

		//Swallow leading zeros
		while (ch < numChars.length && numChars[ch] == '0') {
			ch++;
		}

		short sign = NUMERIC_POS;
		short displayWeight = -1;
		short displayScale = 0;

		if (ch < numChars.length && numChars[ch] == '-') {
			sign = NUMERIC_NEG;
			++ch;
		}

		/*
		 * Copy to array of single byte digits
		 */

		while (ch < numChars.length) {

			if (numChars[ch] == '.') {

				haveDP = true;
				ch++;
			} else {

				numDigs[digs++] = (byte) (numChars[ch++] - '0');
				if (!haveDP) {
					displayWeight++;
				} else {
					displayScale++;
				}
			}

		}

		digs -= DEC_DIGITS;

		/*
		 * Pack into NBASE format
		 */

		short weight;

		if (displayWeight >= 0) {
			weight = (short) ((displayWeight + 1 + DEC_DIGITS - 1) / DEC_DIGITS - 1);
		} else {
			weight = (short) -((-displayWeight - 1) / DEC_DIGITS + 1);
		}

		int offset = (weight + 1) * DEC_DIGITS - (displayWeight + 1);
		int digitCount = (digs + offset + DEC_DIGITS - 1) / DEC_DIGITS;

		int i = DEC_DIGITS - offset;
		short[] digits = new short[digitCount];
		int d = 0;

		while (digitCount-- > 0) {
			digits[d++] = (short) (((numDigs[i] * 10 + numDigs[i + 1]) * 10 + numDigs[i + 2]) * 10 + numDigs[i + 3]);
			i += DEC_DIGITS;
		}

		info[0] = weight;
		info[1] = sign;
		info[2] = displayScale;
		return digits;
	}

	public static final DateTimeFormatter DATE_TIME_FORMATTER =
			new DateTimeFormatterBuilder()
					.optionalStart()
					.append(DateTimeFormatter.ISO_LOCAL_DATE)
					.optionalEnd()
					.optionalStart()
					.appendLiteral(' ')
					.optionalEnd()
					.optionalStart()
					.appendLiteral('T')
					.optionalEnd()
					.appendPattern("HH:mm:ss")
					.appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
					.optionalStart()
					.appendOffset("+HH", "Z")
					.optionalEnd()
					.toFormatter();

	private static final long PG_EPOCH_SECS = 946684800L;

	static long javaEpochToPg(long value, TimeUnit timeUnit) {

		return value - timeUnit.convert(PG_EPOCH_SECS, TimeUnit.SECONDS);
	}
}
