/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.copy;

import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import org.postgresql.jdbc.ArrayUtil;
import org.postgresql.jdbc.TimestampUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;

/**
 * Record转成pg Text流.
 */
public class RecordTextOutputStream extends RecordOutputStream {

	public static final int QUOTA = '"';
	public static final int ESCAPE = '\\';
	public static final int NULL = 'N';
	public static final int DELIMITER = ',';
	public static final int NEWLINE = '\n';

	private static final String ESCAPE_STR;
	private static final String ESCAPE_REPLACE_STR;

	private static final String QUOTA_STR;
	private static final String QUOTA_REPLACE_STR;

	static {
		ESCAPE_STR = '\\' + String.valueOf((char) ESCAPE);
		ESCAPE_REPLACE_STR = ESCAPE_STR + ESCAPE_STR;
		QUOTA_STR = String.valueOf((char) QUOTA);
		QUOTA_REPLACE_STR = ESCAPE_STR + QUOTA_STR;
	}

	public RecordTextOutputStream(OutputStream os, TableSchema schema, TimestampUtils timestampUtils, int maxCellBufferSize) {
		super(os, schema, timestampUtils, maxCellBufferSize);
	}

	private String formatString(String input) {
		return input.replaceAll(ESCAPE_STR, ESCAPE_REPLACE_STR)
				.replaceAll(QUOTA_STR, QUOTA_REPLACE_STR);
	}

	@Override
	protected void fillByteBuffer(Record record) throws IOException {

		for (int i = 0; i < record.getSchema().getColumnSchema().length; ++i) {
			if (i > 0) {
				write(DELIMITER);
			}
			Column column = record.getSchema().getColumn(i);
			int type = column.getType();
			Object obj = record.getObject(i);
			if (obj == null) {
				write(NULL);
			} else {
				boolean quota = false;
				byte[] temp = null;
				String text = null;
				try {
					switch (type) {
						case Types.CHAR:
						case Types.NCHAR:
						case Types.CLOB:
						case Types.NCLOB:
						case Types.VARCHAR:
						case Types.LONGVARCHAR:
						case Types.NVARCHAR:
						case Types.LONGNVARCHAR:
							text = String.valueOf(obj);
							quota = true;
							text = formatString(text);
							break;
						case Types.TIME:
						case Types.TIME_WITH_TIMEZONE:
						case Types.DATE:
						case Types.TIMESTAMP:
						case Types.TIMESTAMP_WITH_TIMEZONE:
							if (obj instanceof Time) {
								text = timestampUtils.toString(null, (Time) obj);
							} else if (obj instanceof Timestamp) {
								text = timestampUtils.toString(null, (Timestamp) obj);
							} else if (obj instanceof Date) {
								text = timestampUtils.toString(null, (Date) obj);
							} else if (obj instanceof java.util.Date) {
								text =
										timestampUtils.toString(
												null,
												new Timestamp(((java.util.Date) obj).getTime()));
							} else {
								text = String.valueOf(obj);
							}
							break;
						case Types.ARRAY:
							if (obj.getClass().getComponentType() != null) {
								text = ArrayUtil.arrayToString(obj);
							} else {
								text = String.valueOf(obj);
							}
							text =
									text.replaceAll(ESCAPE_STR, ESCAPE_REPLACE_STR)
											.replaceAll(QUOTA_STR, QUOTA_REPLACE_STR);
							quota = true;
							break;
						case Types.SMALLINT:
						case Types.INTEGER:
						case Types.BIGINT:
						case Types.NUMERIC:
						case Types.DECIMAL:
						case Types.FLOAT:
						case Types.REAL:
						case Types.DOUBLE:
							text = String.valueOf(obj);
							break;
						case Types.BINARY:
						case Types.VARBINARY:
						case Types.BLOB:
						case Types.LONGVARBINARY:
						case Types.OTHER:
							if (obj instanceof byte[]) {
								byte[] bytes = (byte[]) obj;
								StringBuffer sb = new StringBuffer(2 + bytes.length);
								sb.append("\\x");
								for (int j = 0; j < bytes.length; j++) {
									String hex = Integer.toHexString(bytes[j] & 0xFF);
									if (hex.length() < 2) {
										sb.append(0);
									}
									sb.append(hex);
								}
								text = sb.toString();
							} else {
								text = String.valueOf(obj);
								text = formatString(text);
								quota = true;
							}
							break;
						case Types.BOOLEAN:
						case Types.BIT:
							text = String.valueOf(obj);
							break;
						default:
							throw new IOException(
									"unsupported type "
											+ type
											+ " type name:"
											+ column.getTypeName());
					}
				} catch (Exception e) {
					throw new IOException(
							"fill byteBuffer "
									+ column.getName()
									+ " fail.index:"
									+ currentRecord
									+ " record:"
									+ currentRecord
									+ ", bytes:"
									+ (temp != null ? Arrays.toString(temp) : "null")
									+ ", text:"
									+ text,
							e);
				}
				if (quota) {
					write(QUOTA);
					write(text.getBytes(UTF8));
					write(QUOTA);
				} else {
					write(text.getBytes(UTF8));
				}
			}
		}
		write(NEWLINE);
	}
}
