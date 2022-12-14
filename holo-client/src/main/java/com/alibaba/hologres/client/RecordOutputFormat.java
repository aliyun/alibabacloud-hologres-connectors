/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.impl.collector.shard.DistributionKeyShardPolicy;
import com.alibaba.hologres.client.impl.collector.shard.ShardPolicy;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.ImportContext;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import org.postgresql.jdbc.ArrayUtil;
import org.postgresql.jdbc.TimestampUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;

/**
 * RecordOutputStream.
 */
public class RecordOutputFormat implements Closeable {

	private static final int DEFAULT_MAX_CELL_BUFFER_SIZE = 2 * 1024 * 1024;
	private static final int QUOTE = '"';
	private static final int ESCAPE = '\\';
	private static final int NULL = 'N';
	private static final int DELIMITER = ',';
	private static final int NEWLINE = '\n';
	private static final Charset UTF8 = Charset.forName("utf-8");
	private final TableSchema schema;

	private static final String ESCAPE_STR;
	private static final String ESCAPE_REPLACE_STR;

	private static final String QUOTE_STR;
	private static final String QUOTE_REPLACE_STR;

	static {
		ESCAPE_STR = (char) ESCAPE + String.valueOf((char) ESCAPE);
		ESCAPE_REPLACE_STR = ESCAPE_STR + ESCAPE_STR;
		QUOTE_STR = String.valueOf((char) QUOTE);
		QUOTE_REPLACE_STR = ESCAPE_STR + QUOTE_STR;
	}

	private final int maxCellBufferSize;

	private final TimestampUtils timestampUtils;

	private final ImportContext importContext;

	private ShardPolicy policy;

	public RecordOutputFormat(ImportContext importContext, TableSchema schema) throws IOException {
		this(importContext, schema, DEFAULT_MAX_CELL_BUFFER_SIZE);
	}

	public RecordOutputFormat(ImportContext importContext, TableSchema schema, int maxCellBufferSize) throws IOException {
		this.importContext = importContext;
		this.schema = schema;
		this.maxCellBufferSize = maxCellBufferSize;
		this.timestampUtils = importContext.getTimestampUtils();
		this.policy = new DistributionKeyShardPolicy();
		policy.init(importContext.getShardCount());
	}

	boolean closed = false;
	ByteBuffer cellBuffer = ByteBuffer.allocate(1024);
	Record currentRecord = null;
	int currentColumnIndex;

	@Override
	public void close() throws IOException {
		closed = true;
		importContext.closeOstreams();
	}

	public void putRecord(Record record) throws IOException {
		if (closed) {
			throw new IOException("RecordOutputFormat already closed");
		}
		int shardId = policy.locate(record);
		fillByteBuffer(record);
		cellBuffer.flip();
		importContext.getOutputStream(shardId).write(cellBuffer.array(), cellBuffer.position(), cellBuffer.remaining());
		cellBuffer.clear();
	}

	private void fillByteBuffer(Record record) throws IOException {

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
				boolean quote = false;
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
							quote = true;
							text = text.replaceAll(ESCAPE_STR, ESCAPE_REPLACE_STR).replaceAll(QUOTE_STR, QUOTE_REPLACE_STR);
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
								text = timestampUtils.toString(null, new Timestamp(((java.util.Date) obj).getTime()));
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
							text = text.replaceAll(ESCAPE_STR, ESCAPE_REPLACE_STR).replaceAll(QUOTE_STR, QUOTE_REPLACE_STR);
							quote = true;
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
							}
							break;
						case Types.BOOLEAN:
						case Types.BIT:
							text = String.valueOf(obj);
							break;
						default:
							throw new IOException("unsupported type " + type + " type name:" + column.getTypeName());
					}
				} catch (Exception e) {
					throw new IOException("fill byteBuffer " + column.getName() + " fail.index:" + currentRecord + " record:" + currentRecord + ", bytes:" + (temp != null ? Arrays.toString(temp) : "null") + ", text:" + text, e);
				}
				if (quote) {
					write(QUOTE);
					write(text.getBytes(UTF8));
					write(QUOTE);
				} else {
					write(text.getBytes(UTF8));
				}
			}

		}
		write(NEWLINE);
	}

	private void write(int r) throws IOException {
		if (cellBuffer.remaining() == 0) {
			if (cellBuffer.position() < DEFAULT_MAX_CELL_BUFFER_SIZE) {
				int target = Math.min(cellBuffer.position() * 2, maxCellBufferSize);
				ByteBuffer temp = ByteBuffer.allocate(target);
				temp.put(cellBuffer);
				cellBuffer.clear();
				cellBuffer = temp;
			} else {
				throw new IOException("RecordInputStream cellBuffer exceed max cell size " + maxCellBufferSize + " for column " + schema.getColumn(currentColumnIndex).getName());
			}
		} else {
			cellBuffer.put((byte) (r & 0xFF));
		}
	}

	private void write(byte[] bytes) throws IOException {
		if (cellBuffer.remaining() < bytes.length) {
			if (cellBuffer.position() + bytes.length < maxCellBufferSize) {
				int target = Math.max(cellBuffer.position() + bytes.length, Math.min(cellBuffer.position() * 2, maxCellBufferSize));
				ByteBuffer temp = ByteBuffer.allocate(target);
				temp.put(cellBuffer);
				cellBuffer.clear();
				cellBuffer = temp;
			} else {
				throw new IOException("RecordInputStream cellBuffer exceed max cell size " + maxCellBufferSize + " for column " + schema.getColumn(currentColumnIndex).getName());
			}
		} else {
			cellBuffer.put(bytes);
		}
	}
}
