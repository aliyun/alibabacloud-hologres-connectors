/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.copy;

import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.ArrayUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Types;

import static com.alibaba.hologres.client.copy.CopyUtil.ESCAPE;
import static com.alibaba.hologres.client.copy.CopyUtil.NULL;

/**
 * Record转成pg Text流.
 */
public class RecordTextOutputStream extends RecordOutputStream {

	private final StringWriter stringWriter;
	private final CSVPrinter printer;

	public RecordTextOutputStream(OutputStream os, TableSchema schema, BaseConnection baseConnection, int maxCellBufferSize) throws  IOException {
		super(os, schema, baseConnection, maxCellBufferSize);
		stringWriter = new StringWriter(maxCellBufferSize);
		printer = new CSVPrinter(stringWriter, CSVFormat.POSTGRESQL_CSV.withEscape(ESCAPE).withQuoteMode(QuoteMode.MINIMAL));
	}

	@Override
	protected void fillByteBuffer(Record record) throws IOException {
		// 清除StringWriter的内容
		stringWriter.getBuffer().setLength(0);
		for (int i = 0; i < record.getSchema().getColumnSchema().length; ++i) {
			if (!record.isSet(i)) {
				continue;
			}

			Column column = record.getSchema().getColumn(i);
			int type = column.getType();
			Object obj = record.getObject(i);
			if (obj == null) {
				printer.print(NULL);
			} else {
				try {
					switch (type) {
						case Types.ARRAY:
							String text;
							if (obj.getClass().getComponentType() != null) {
								Array array = ArrayUtil.objectToArray(conn, obj, column.getTypeName());
								text = ArrayUtil.arrayToString(array != null ? ArrayUtil.objectToArray(conn, obj, column.getTypeName()) : obj);
							} else {
								text = String.valueOf(obj);
							}
							printer.print(text);
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
								printer.print(sb.toString());
							} else {
								printer.print(String.valueOf(obj));
							}
							break;
						case Types.TIME:
							// java.sql.time 默认的toString方法格式为HH:mm:ss,需要加上.SSS
							if (obj instanceof java.sql.Time) {
                                java.sql.Time timeObj = (java.sql.Time) obj;
                                long msSuffix = timeObj.getTime() % 1000;
								printer.print(timeObj + "." + msSuffix);
                            } else {
								printer.print(String.valueOf(obj));
							}
							break;
						default:
							printer.print(record.getObject(i));
					}
				} catch (Exception e) {
					throw new IOException(
							"fill byteBuffer "
									+ column.getName()
									+ " fail.index:"
									+ i
									+ " record:"
									+ currentRecord
									+ ", text:"
									+ obj,
							e);
				}
			}
		}

		printer.println();
		printer.flush();
		write(stringWriter.toString().getBytes(Charset.forName("utf-8")));
	}

}
