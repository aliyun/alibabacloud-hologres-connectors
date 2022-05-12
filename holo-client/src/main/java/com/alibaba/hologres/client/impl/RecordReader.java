package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import org.postgresql.jdbc.TimestampUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Types;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RecordReader.
 */
public class RecordReader implements Runnable{
	public static final Logger LOGGER = LoggerFactory.getLogger(RecordReader.class);

	private static final int DEFAULT_MAX_CELL_BUFFER_SIZE = 2 * 1024 * 1024;
	private static final int QUOTA = '"';
	private static final int ESCAPE = '\\';
	private static final int DELIMITER = ',';
	private static final int NEWLINE = '\n';
	private static final String NULL = "N";
	private static final Charset UTF8 = Charset.forName("utf-8");
	private final TableSchema schema;

	private final int maxCellBufferSize;

	private final TimestampUtils timestampUtils;

	private final BlockingQueue<Record> queue;
	private final InputStream is;

	public RecordReader(InputStream is, TableSchema schema, BlockingQueue<Record> queue, AtomicInteger numOpened, TimestampUtils timestampUtils) {
		this(is, schema, queue, numOpened, timestampUtils, DEFAULT_MAX_CELL_BUFFER_SIZE);
	}

	public RecordReader(InputStream is, TableSchema schema, BlockingQueue<Record> queue, AtomicInteger numOpened, TimestampUtils timestampUtils, int maxCellBufferSize) {
		this.is = is;
		this.schema = schema;
		this.queue = queue;
		this.maxCellBufferSize = maxCellBufferSize;
		this.numOpened = numOpened;
		this.inputBuffer = new byte[1024];
		this.timestampUtils = timestampUtils;
	}

	AtomicInteger numOpened;
	byte[] inputBuffer;
	int currentPos = 0;
	int bufferLen = 0;
	boolean closed = false;
	ByteBuffer cellBuffer = ByteBuffer.allocate(1024);
	Record currentRecord = null;
	int currentColumnIndex;


	boolean isInQuota = false;
	boolean isEscapeBefore = false;
	boolean isNull = false;

	public Record getRecord() throws IOException {
		if (closed) {
			return null;
		}
		int r = readByte();
		Record ret = null;

		if (r == -1) {
			closed = true;
			numOpened.getAndDecrement();
			return null;
		} else {
			boolean recordEnd = false;
			while (!recordEnd) {

				if (isEscapeBefore) {
					write(r);
					isEscapeBefore = false;
				} else if (isInQuota) {
					switch (r) {
						case ESCAPE:
							isEscapeBefore = true;
							break;
						case QUOTA:
							isInQuota = false;
							break;
						default:
							write(r);
					}
				} else {
					switch (r) {
						case ESCAPE:
							isEscapeBefore = true;
							break;
						case QUOTA:
							isInQuota = true;
							break;
						case DELIMITER:
							fillRecord();
							break;
						case NEWLINE:
							fillRecord();
							recordEnd = true;
							break;
						default:
							write(r);
					}
				}
				if (!recordEnd) {
					r = readByte();
				}
			}
			Record temp = currentRecord;
			currentRecord = new Record(schema);
			currentColumnIndex = 0;
			return temp;
		}
	}

	private void fillRecord() throws IOException {
		cellBuffer.flip();
		if (currentRecord == null) {
			currentRecord = new Record(schema);
		}
		Column column = schema.getColumn(currentColumnIndex);
		int type = column.getType();
		if (cellBuffer.remaining() == 0) {
			switch (type) {
				case Types.CHAR:
				case Types.NCHAR:
				case Types.CLOB:
				case Types.NCLOB:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
				case Types.NVARCHAR:
				case Types.LONGNVARCHAR:
					currentRecord.setObject(currentColumnIndex, "");
					break;
				default:
					currentRecord.setObject(currentColumnIndex, null);
			}
		} else {
			byte[] temp = null;
			String text = null;
			try {

				temp = new byte[cellBuffer.remaining()];
				cellBuffer.get(temp);
				text = new String(temp, UTF8);
				if (text.equals(NULL)) {
					currentRecord.setObject(currentColumnIndex, null);
				}
				else {
					switch (type) {
						case Types.CHAR:
						case Types.NCHAR:
						case Types.CLOB:
						case Types.NCLOB:
						case Types.VARCHAR:
						case Types.LONGVARCHAR:
						case Types.NVARCHAR:
						case Types.LONGNVARCHAR:
						case Types.ARRAY:
							currentRecord.setObject(currentColumnIndex, text);
							break;
						case Types.TIME:
						case Types.TIME_WITH_TIMEZONE:
							currentRecord.setObject(currentColumnIndex, timestampUtils.toTime(null, text));
							break;
						case Types.DATE:
							currentRecord.setObject(currentColumnIndex, timestampUtils.toDate(null, text));
							break;
						case Types.TIMESTAMP:
						case Types.TIMESTAMP_WITH_TIMEZONE:
							currentRecord.setObject(currentColumnIndex, timestampUtils.toTimestamp(null, text));
							break;
						case Types.SMALLINT:
							currentRecord.setObject(currentColumnIndex, Short.parseShort(text));
							break;
						case Types.INTEGER:
							currentRecord.setObject(currentColumnIndex, Integer.parseInt(text));
							break;
						case Types.BIGINT:
							currentRecord.setObject(currentColumnIndex, Long.parseLong(text));
							break;
						case Types.NUMERIC:
						case Types.DECIMAL:
							currentRecord.setObject(currentColumnIndex, new BigDecimal(text));
							break;
						case Types.FLOAT:
						case Types.REAL:
							currentRecord.setObject(currentColumnIndex, Float.parseFloat(text));
							break;
						case Types.DOUBLE:
							currentRecord.setObject(currentColumnIndex, Double.parseDouble(text));
							break;
						case Types.BINARY:
						case Types.VARBINARY:
						case Types.BLOB:
						case Types.LONGVARBINARY:
						case Types.OTHER:
							byte[] temp2 = new byte[temp.length];
							System.arraycopy(temp, 0, temp2, 0, temp.length);
							currentRecord.setObject(currentColumnIndex, temp2);
							break;
						case Types.BOOLEAN:
						case Types.BIT:
							currentRecord.setObject(currentColumnIndex, Boolean.parseBoolean(text));
							break;
						default:
							throw new IOException("unsupported type " + type + " type name:" + column.getTypeName());
					}
				}
			} catch (Exception e) {
				throw new IOException("fill column " + column.getName() + " fail.index:" + currentRecord + " record:" + currentRecord + ", bytes:" + (temp != null ? Arrays.toString(temp) : "null") + ", text:" + text, e);
			}
		}
		cellBuffer.clear();
		++currentColumnIndex;
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

	private int readByte() throws IOException {
		byte content;
		if (currentPos >= bufferLen) {
			bufferLen = is.read(inputBuffer, 0, 1024);
			currentPos = 0;
			if (bufferLen == -1) {
				return -1;
			}
		}
		content = inputBuffer[currentPos++];
		return content & 0xFF;
	}

	public void run() {
		Record r;
		try {
			while ((r = getRecord()) != null) {
				queue.put(r);
			}
		} catch (Exception e) {
			LOGGER.error("", e);
		}
	}
}
