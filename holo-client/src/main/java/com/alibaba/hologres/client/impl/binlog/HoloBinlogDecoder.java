package com.alibaba.hologres.client.impl.binlog;

import com.alibaba.blink.dataformat.BinaryArray;
import com.alibaba.blink.dataformat.BinaryRow;
import com.alibaba.blink.memory.MemorySegment;
import com.alibaba.blink.memory.MemorySegmentFactory;
import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.BinlogEventType;
import com.alibaba.hologres.client.model.Record;
import org.postgresql.jdbc.ArrayUtil;
import org.postgresql.model.Column;
import org.postgresql.model.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

/**
 * 将Hologres的binlog解析为Holo-client的Record格式.
 */
public class HoloBinlogDecoder {
	public static final int BINLOG_PROTOCOL_VERSION = 0;
	public static final int BINLOG_HEADER_LEN = 24;
	public static final long ONE_DAY_IN_MILLIES = 24 * 60 * 60 * 1000;
	public static final long TIMEZONE_OFFSET = TimeZone.getDefault().getRawOffset();
	public static final Logger LOGGER = LoggerFactory.getLogger(HoloBinlogDecoder.class);

	private HoloClient client = null;
	private final String tableName;
	private int shardId;

	private MemorySegment segment;
	private List<BinaryRow> rows;
	private Column[] columns;
	private int columnCount;
	private long tableVersion = -1;
	private TableSchema schema;
	private Boolean binlogIgnoreBeforeUpdate = false;
	private Boolean binlogIgnoreDelete = false;

	public HoloBinlogDecoder(HoloClient client, TableSchema schema, int shardId, Boolean binlogIgnoreDelete, Boolean binlogIgnoreBeforeUpdate) throws HoloClientException {
		this(client, schema);
		this.shardId = shardId;
		this.binlogIgnoreDelete = binlogIgnoreDelete;
		this.binlogIgnoreBeforeUpdate = binlogIgnoreBeforeUpdate;
	}

	/** 传入HoloClient 可以支持在使用中刷新schema，比如增删表列等情况. */
	public HoloBinlogDecoder(HoloClient client, TableSchema schema) throws HoloClientException {
		this.client = client;
		this.tableName = schema.getTableNameObj().getFullName();
		this.schema = client.getTableSchema(tableName, true);
		this.columns = schema.getColumnSchema();
		this.columnCount = columns.length;
	}

	/** 不需要在使用中刷新schema. */
	public HoloBinlogDecoder(TableSchema schema) throws HoloClientException {
		this.tableName = schema.getTableNameObj().getFullName();
		this.schema = schema;
		this.columns = schema.getColumnSchema();
		this.columnCount = columns.length;
	}

	/**
	 * 如果table version 发生变化（alter table），重新获取table scheme.
	 */
	private void reFlushDecoder() throws HoloClientException {
		this.schema = client.getTableSchema(tableName, true);
		this.columns = schema.getColumnSchema();
		this.columnCount = columns.length;
	}

	private BinaryRow getRow(int index) {
		return this.rows.get(index);
	}

	private int size() {
		return this.rows.size();
	}

	private void deserialize(byte[] headerBytes, byte[] dataBytes) throws HoloClientException {
		LongBuffer longBuffer = ByteBuffer.wrap(headerBytes).order(ByteOrder.BIG_ENDIAN).asLongBuffer();
		long binlogProtocolVersion = longBuffer.get(0);
		long currentTableVersion = longBuffer.get(1);
		if (BINLOG_PROTOCOL_VERSION != binlogProtocolVersion) {
			throw new IllegalStateException(
					"binlog version mismatch, expected: " + BINLOG_PROTOCOL_VERSION + ", actual: " + binlogProtocolVersion);
		}
		if (tableVersion == -1) {
			tableVersion = currentTableVersion;
		} else if (currentTableVersion != tableVersion) {
			String s = 	String.format("Table %s have been altered, old table version id is %s, new table version id is %s.",
					tableName, tableVersion, currentTableVersion);
			if (client != null){
				LOGGER.info(s);
				tableVersion = currentTableVersion;
				reFlushDecoder();
			} else {
				throw new RuntimeException(s + "You could use \" HoloBinlogDecoder(HoloClient client, TableSchema schema)\" to avoid it.");
			}
		}

		IntBuffer buffer = ByteBuffer.wrap(dataBytes).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
		int rowCount = buffer.get(1);
		this.segment = MemorySegmentFactory.wrap(dataBytes);
		this.rows = new ArrayList();
		for (int i = 0; i < rowCount; ++i) {
			int offset = buffer.get(2 + i);
			int offsetNext = i == rowCount - 1 ? dataBytes.length : buffer.get(3 + i);
			if (offset > offsetNext) {
				throw new IllegalStateException(
						"invalid offset in pos " + i + ", offset=" + offset + ", offsetNext=" + offsetNext);
			}

			BinaryRow row = new BinaryRow(this.columnCount + 3);
			row.pointTo(this.segment, offset, offsetNext - offset);
			this.rows.add(row);
		}
	}

	private void convertBinaryRowToRecord(Column column, BinaryRow currentRow, Record currentRecord, int index)
			throws IOException {
		int offsetIndex = index + 3;
		if (currentRow.isNullAt(offsetIndex)) {
			currentRecord.setObject(index, null);
			return;
		}
		switch (column.getType()) {
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.OTHER:
				currentRecord.setObject(index, currentRow.getString(offsetIndex));
				break;
			case Types.DATE:
				currentRecord.setObject(index, new Date(currentRow.getLong(offsetIndex) * ONE_DAY_IN_MILLIES));
				break;
			case Types.TIME:
			case Types.TIME_WITH_TIMEZONE:
				if ("timetz".equals(column.getTypeName())) {
					long time = ByteBuffer.wrap(currentRow.getByteArray(offsetIndex)).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().get(0);
					int zoneOffset = ByteBuffer.wrap(currentRow.getByteArray(offsetIndex)).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(2);
					currentRecord.setObject(index, new Time(time / 1000L + zoneOffset * 1000L));
				} else {
					currentRecord.setObject(index, new Time(currentRow.getLong(offsetIndex) / 1000L - TIMEZONE_OFFSET));
				}
				break;
			case Types.TIMESTAMP:
			case Types.TIMESTAMP_WITH_TIMEZONE:
				if ("timestamptz".equals(column.getTypeName())) {
					currentRecord.setObject(index, new Timestamp(currentRow.getLong(offsetIndex)));
				} else {
					currentRecord.setObject(index, new Timestamp(currentRow.getLong(offsetIndex) / 1000L - TIMEZONE_OFFSET));
				}
				break;
			case Types.SMALLINT:
				currentRecord.setObject(index, currentRow.getShort(offsetIndex));
				break;
			case Types.INTEGER:
				currentRecord.setObject(index, currentRow.getInt(offsetIndex));
				break;
			case Types.BIGINT:
				currentRecord.setObject(index, currentRow.getLong(offsetIndex));
				break;
			case Types.NUMERIC:
			case Types.DECIMAL:
				int scale = column.getScale();
				byte[] value = currentRow.getByteArray(offsetIndex);
				ArrayUtil.reverse(value);
				BigInteger bigInteger = new BigInteger(value);
				BigDecimal bigDecimal = new BigDecimal(bigInteger);
				bigDecimal = bigDecimal.movePointLeft(scale);
				bigDecimal = bigDecimal.setScale(scale, BigDecimal.ROUND_DOWN);
				currentRecord.setObject(index, bigDecimal);
				break;
			case Types.FLOAT:
			case Types.REAL:
				currentRecord.setObject(index, currentRow.getFloat(offsetIndex));
				break;
			case Types.DOUBLE:
				currentRecord.setObject(index, currentRow.getDouble(offsetIndex));
				break;
			case Types.BINARY:
			case Types.VARBINARY:
				currentRecord.setObject(index, currentRow.getByteArray(offsetIndex));
				break;
			case Types.ARRAY:
				switch (column.getTypeName()) {
					case "_int4":
						currentRecord.setObject(index, currentRow.getArray(offsetIndex).toIntArray());
						break;
					case "_int8":
						currentRecord.setObject(index, currentRow.getArray(offsetIndex).toLongArray());
						break;
					case "_float4":
						currentRecord.setObject(index, currentRow.getArray(offsetIndex).toFloatArray());
						break;
					case "_float8":
						currentRecord.setObject(index, currentRow.getArray(offsetIndex).toDoubleArray());
						break;
					case "_bool":
						currentRecord.setObject(index, currentRow.getArray(offsetIndex).toBooleanArray());
						break;
					case "_text":
						BinaryArray binaryArray = currentRow.getArray(offsetIndex);
						String[] stringArrays = new String[binaryArray.numElements()];
						for (int i = 0; i < binaryArray.numElements(); i++) {
							stringArrays[i] = binaryArray.getString(i);
						}
						currentRecord.setObject(index, stringArrays);
						break;
					default:
				}
				break;
			case Types.BOOLEAN:
			case Types.BIT:
				currentRecord.setObject(index, currentRow.getBoolean(offsetIndex));
				break;
			default:
				throw new IOException("unsupported type " + column.getType() + " type name:" + column.getTypeName());
		}
	}

	/**
	 * @param byteBuffer 包含header和data两部分
	 *		<p>
	 * 		header部分为前 16 byte，结构如下:
	 *		0  -  7: binlog_protocol version, (long)
	 *		8  - 15: table version, (long)
	 *		<p>
	 *		data部分为header部分之后，结构如下:
	 *		0  -  4: binlog version, (int)
	 *		5  -  8: row count,  (int)
	 *		9  -   : each row‘s offset, (int)
	 */
	public List<Record> decode(ByteBuffer byteBuffer) throws IOException, HoloClientException, IllegalStateException {

		if (byteBuffer.limit() < BINLOG_HEADER_LEN) {
			throw new IllegalStateException("Invalid ByteBuffer");
		}

		byte[] headerBytes = new byte[16];
		byte[] dataBytes = new byte[byteBuffer.limit() - 16];
		System.arraycopy(byteBuffer.array(), byteBuffer.arrayOffset(), headerBytes, 0, 16);
		System.arraycopy(byteBuffer.array(), byteBuffer.arrayOffset() + 16, dataBytes, 0, byteBuffer.limit() - 16);
		deserialize(headerBytes, dataBytes);

		List<Record> records = new ArrayList<>();
		for (int i = 0; i < size(); ++i) {
			BinaryRow currentRow = getRow(i);
			Record currentRecord = new Record(schema, true);
			long lsn = currentRow.getLong(0);
			long type = currentRow.getLong(1);
			long timestamp = currentRow.getLong(2);
			if (binlogIgnoreDelete && type == BinlogEventType.DELETE.getValue()) {
				continue;
			}
			if (binlogIgnoreBeforeUpdate && type == BinlogEventType.BEFORE_UPDATE.getValue()) {
				continue;
			}
			currentRecord.setBinlogParams(lsn, type, timestamp);
			if (shardId >= 0){
				currentRecord.setShardId(shardId);
			}
			for (int index = 0; index < columnCount; ++index) {
				convertBinaryRowToRecord(columns[index], currentRow, currentRecord, index);
			}
			records.add(currentRecord);
		}
		return records;
	}
}
