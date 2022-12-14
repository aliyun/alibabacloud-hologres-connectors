package com.alibaba.hologres.client.copy;

import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.TimestampUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Record转成copy流.
 */
public abstract class RecordOutputStream implements Closeable {

	private static final int DEFAULT_MAX_CELL_BUFFER_SIZE = 2 * 1024 * 1024;

	protected static final Charset UTF8 = Charset.forName("utf-8");
	protected final TableSchema schema;

	private final int maxCellBufferSize;

	protected final BaseConnection conn;
	protected final TimestampUtils timestampUtils;

	private final OutputStream os;

	public RecordOutputStream(
			OutputStream os, TableSchema schema, BaseConnection conn, int maxCellBufferSize) {
		this.schema = schema;
		this.os = os;
		this.maxCellBufferSize = maxCellBufferSize;
		this.conn = conn;
		this.timestampUtils = conn.getTimestampUtils();

	}

	boolean closed = false;
	ByteBuffer cellBuffer = ByteBuffer.allocate(10);
	Record currentRecord = null;
	int currentColumnIndex;

	public long getResult() {
		if (os instanceof WithCopyResult) {
			return ((WithCopyResult) os).getResult();
		} else {
			return -1;
		}
	}

	@Override
	public void close() throws IOException {
		if (!closed) {
			closed = true;
			os.close();
		}
	}

	public void putRecord(Record record) throws IOException {
		if (closed) {
			throw new IOException("RecordOutputFormat already closed");
		}
		fillByteBuffer(record);
		cellBuffer.flip();
		os.write(cellBuffer.array(), cellBuffer.position(), cellBuffer.remaining());

		cellBuffer.clear();
	}

	protected abstract void fillByteBuffer(Record record) throws IOException;

	private void mayIncBuffer(int size) throws IOException {
		if (cellBuffer.remaining() < size) {
			if (cellBuffer.position() + size < maxCellBufferSize) {
				int target = Math.min(Math.max(cellBuffer.position() + size, cellBuffer.position() * 2), maxCellBufferSize);
				ByteBuffer temp = ByteBuffer.allocate(target);
				cellBuffer.flip();
				temp.put(cellBuffer);
				cellBuffer.clear();
				cellBuffer = temp;
			} else {
				throw new IOException(
						"RecordInputStream cellBuffer exceed max cell size "
								+ maxCellBufferSize
								+ " for column "
								+ schema.getColumn(currentColumnIndex).getName());
			}
		}
	}

	/**
	 * write byte.
	 *
	 * @param r
	 * @throws IOException
	 */
	protected void write(int r) throws IOException {
		mayIncBuffer(1);
		cellBuffer.put((byte) (r & 0xFF));
	}

	protected void writeShort(short r) throws IOException {
		mayIncBuffer(2);
		cellBuffer.putShort(r);
	}

	protected void writeInt(int r) throws IOException {
		mayIncBuffer(4);
		cellBuffer.putInt(r);
	}

	protected void writeFloat(float r) throws IOException {
		mayIncBuffer(4);
		cellBuffer.putFloat(r);
	}

	protected void writeDouble(double r) throws IOException {
		mayIncBuffer(8);
		cellBuffer.putDouble(r);
	}

	protected void writeLong(long r) throws IOException {
		mayIncBuffer(8);
		cellBuffer.putLong(r);
	}

	protected void write(byte[] bytes) throws IOException {
		mayIncBuffer(bytes.length);
		cellBuffer.put(bytes);
	}
}
