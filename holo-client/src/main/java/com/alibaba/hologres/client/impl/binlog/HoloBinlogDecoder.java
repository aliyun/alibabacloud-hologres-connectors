package com.alibaba.hologres.client.impl.binlog;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.binlog.BinlogRecord;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.postgresql.jdbc.ArrayUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.security.InvalidParameterException;
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

	private Column[] columns;
	private int columnCount;
	private long tableVersion = -1;
	private TableSchema schema;
	private Boolean binlogIgnoreBeforeUpdate = false;
	private Boolean binlogIgnoreDelete = false;
	private TableSchemaSupplier tableSchemaSupplier;

	public HoloBinlogDecoder(TableSchema schema, Boolean binlogIgnoreDelete, Boolean binlogIgnoreBeforeUpdate) throws HoloClientException {
		this.binlogIgnoreDelete = binlogIgnoreDelete;
		this.binlogIgnoreBeforeUpdate = binlogIgnoreBeforeUpdate;
		init(schema);
	}

	public HoloBinlogDecoder(TableSchemaSupplier supplier, Boolean binlogIgnoreDelete, Boolean binlogIgnoreBeforeUpdate) throws HoloClientException {
		this.tableSchemaSupplier = supplier;
		this.binlogIgnoreDelete = binlogIgnoreDelete;
		this.binlogIgnoreBeforeUpdate = binlogIgnoreBeforeUpdate;
		init(supplier.apply());
	}

	public HoloBinlogDecoder(TableSchema schema) throws HoloClientException {
		this(schema, false, false);
	}

	public HoloBinlogDecoder(TableSchemaSupplier supplier) throws HoloClientException {
		this(supplier, false, false);
	}

	private static long parseSchemaVersion(TableSchema schema) throws HoloClientException {
		try {
			return Long.parseLong(schema.getSchemaVersion());
		} catch (Exception e) {
			throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, String.format("parse schema version fail for table %s, schema version %s", schema.getTableNameObj().getFullName(), schema.getSchemaVersion()), e);
		}
	}

	private void init(TableSchema schema) throws HoloClientException {
		this.schema = schema;
		this.columns = schema.getColumnSchema();
		this.columnCount = columns.length;
		this.tableVersion = parseSchemaVersion(schema);
	}

	public TableSchemaSupplier getTableSchemaSupplier() {
		return tableSchemaSupplier;
	}

	public void setTableSchemaSupplier(TableSchemaSupplier tableSchemaSupplier) {
		this.tableSchemaSupplier = tableSchemaSupplier;
	}

	public TableSchema getSchema() {
		return schema;
	}

	private List<BinaryRowData> deserialize(int shardId, byte[] headerBytes, byte[] dataBytes) throws HoloClientException {
		LongBuffer longBuffer = ByteBuffer.wrap(headerBytes).order(ByteOrder.BIG_ENDIAN).asLongBuffer();
		long binlogProtocolVersion = longBuffer.get(0);
		long currentTableVersion = longBuffer.get(1);
		if (BINLOG_PROTOCOL_VERSION != binlogProtocolVersion) {
			throw new IllegalStateException(
					"binlog version mismatch, expected: " + BINLOG_PROTOCOL_VERSION + ", actual: " + binlogProtocolVersion);
		}
		if (currentTableVersion != tableVersion) {
			LOGGER.warn("Table {} have been altered, current client table version id is {}, binlog table version id is {}.",
					schema.getTableNameObj().getFullName(), tableVersion, currentTableVersion);
			if (tableSchemaSupplier != null) {
				// TODO 先写死，正常来说不应该发生retry
				int tryCount = 3;
				while (tableVersion < currentTableVersion && --tryCount > 0) {
					init(tableSchemaSupplier.apply());
				}
				if (tableVersion != currentTableVersion) {
					throw new HoloClientException(ExceptionCode.META_NOT_MATCH, String.format("binlog table version for table %s is %s but client table version is %s after refresh", schema.getTableNameObj().getFullName(), currentTableVersion, schema.getSchemaVersion()));
				} else {
					LOGGER.info("Table {} have been altered, update shardId [{}] current client table version id to {}.",
							schema.getTableNameObj().getFullName(), shardId, tableVersion);
				}
			} else {
				throw new HoloClientException(ExceptionCode.META_NOT_MATCH, String.format("binlog table version for table %s is %s but client table version is %s ", schema.getTableNameObj().getFullName(), currentTableVersion, schema.getSchemaVersion()));
			}
		}

		IntBuffer buffer = ByteBuffer.wrap(dataBytes).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
		int rowCount = buffer.get(1);
		MemorySegment segment = MemorySegmentFactory.wrap(dataBytes);
		List<BinaryRowData> rows = new ArrayList();
		for (int i = 0; i < rowCount; ++i) {
			int offset = buffer.get(2 + i);
			int offsetNext = i == rowCount - 1 ? dataBytes.length : buffer.get(3 + i);
			if (offset > offsetNext) {
				throw new IllegalStateException(
						"invalid offset in pos " + i + ", offset=" + offset + ", offsetNext=" + offsetNext);
			}

			BinaryRowData row = new BinaryRowData(this.columnCount + 3);
			row.pointTo(segment, offset, offsetNext - offset);
			rows.add(row);
		}
		return rows;
	}

	private void convertBinaryRowToRecord(Column column, BinaryRowData currentRow, Record currentRecord, int index)
			throws HoloClientException {
		int offsetIndex = index + 3;
		if (currentRow.isNullAt(offsetIndex)) {
			currentRecord.setObject(index, null);
			return;
		}
		switch (column.getType()) {
			case Types.CHAR:
				currentRecord.setObject(index, String.format("%-" + column.getPrecision() + "s", currentRow.getString(offsetIndex)));
				break;
			case Types.VARCHAR:
				currentRecord.setObject(index, currentRow.getString(offsetIndex));
				break;
			case Types.OTHER:
				if ("roaringbitmap".equals(column.getTypeName())) {
					currentRecord.setObject(index, currentRow.getBinary(offsetIndex));
				} else {
					currentRecord.setObject(index, currentRow.getString(offsetIndex));
				}
				break;
			case Types.DATE:
				currentRecord.setObject(index, new Date(currentRow.getInt(offsetIndex) * ONE_DAY_IN_MILLIES));
				break;
			case Types.TIME:
			case Types.TIME_WITH_TIMEZONE:
				if ("timetz".equals(column.getTypeName())) {
					long time = ByteBuffer.wrap(currentRow.getBinary(offsetIndex)).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().get(0);
					int zoneOffset = ByteBuffer.wrap(currentRow.getBinary(offsetIndex)).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(2);
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
					long microseconds = currentRow.getLong(offsetIndex);
					Timestamp timestamp = new Timestamp(microseconds / 1000L - TIMEZONE_OFFSET);
					timestamp.setNanos((int) ((microseconds % 1_000_000L) * 1_000));
					currentRecord.setObject(index, timestamp);
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
				byte[] value = currentRow.getBinary(offsetIndex);
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
				currentRecord.setObject(index, currentRow.getBinary(offsetIndex));
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
					case "_varchar":
						ArrayData binaryArray = currentRow.getArray(offsetIndex);
						String[] stringArrays = new String[binaryArray.size()];
						for (int i = 0; i < binaryArray.size(); i++) {
							stringArrays[i] = binaryArray.getString(i).toString();
						}
						currentRecord.setObject(index, stringArrays);
						break;
					default:
						throw new HoloClientException(ExceptionCode.DATA_TYPE_ERROR, "unsupported array type " + column.getType() + " type name:" + column.getTypeName());
				}
				break;
			case Types.BOOLEAN:
			case Types.BIT:
				currentRecord.setObject(index, currentRow.getBoolean(offsetIndex));
				break;
			default:
				throw new HoloClientException(ExceptionCode.DATA_TYPE_ERROR, "unsupported type " + column.getType() + " type name:" + column.getTypeName());
		}
	}

	/**
	 * @param byteBuffer 包含header和data两部分
	 *                   <p>
	 *                   header部分为前 16 byte，结构如下:
	 *                   0  -  7: binlog_protocol version, (long)
	 *                   8  - 15: table version, (long)
	 *                   <p>
	 *                   data部分为header部分之后，结构如下:
	 *                   0  -  4: binlog version, (int)
	 *                   5  -  8: row count,  (int)
	 *                   9  -   : each row‘s offset, (int)
	 */
	public List<BinlogRecord> decode(int shardId, ByteBuffer byteBuffer) throws HoloClientException {
		ArrayBuffer<BinlogRecord> array = new ArrayBuffer<>(10, BinlogRecord[].class);
		decode(shardId, byteBuffer, array);
		List<BinlogRecord> list = new ArrayList<>();
		array.beginRead();
		while (array.remain() > 0) {
			list.add(array.pop());
		}
		return list;
	}

	public void decode(int shardId, ByteBuffer byteBuffer, ArrayBuffer<BinlogRecord> array) throws HoloClientException {

		if (byteBuffer.limit() < BINLOG_HEADER_LEN) {
			throw new IllegalStateException("Invalid ByteBuffer");
		}

		byte[] headerBytes = new byte[16];
		byte[] dataBytes = new byte[byteBuffer.limit() - 16];
		System.arraycopy(byteBuffer.array(), byteBuffer.arrayOffset(), headerBytes, 0, 16);
		System.arraycopy(byteBuffer.array(), byteBuffer.arrayOffset() + 16, dataBytes, 0, byteBuffer.limit() - 16);
		List<BinaryRowData> list = deserialize(shardId, headerBytes, dataBytes);

		List<BinlogRecord> records = new ArrayList<>();
		for (BinaryRowData currentRow : list) {
			long lsn = currentRow.getLong(0);
			long eventType = currentRow.getLong(1);
			long timestamp = currentRow.getLong(2);

			BinlogEventType type = null;
			try {
				type = BinlogEventType.of(eventType);
			} catch (InvalidParameterException e) {
				throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "unknow binlog eventtype " + eventType, e);
			}
			BinlogRecord currentRecord = new BinlogRecord(schema, lsn, type, timestamp);
			currentRecord.setShardId(shardId);
			// 只是跳过解析，我们需要每一条数据，否则
			// 1 返回一个size==0的就不知道是消费完了还是正在消费中
			// 2 checkpoint也没法做了
			if (binlogIgnoreDelete && type == BinlogEventType.DELETE) {
				records.add(currentRecord);
				continue;
			}
			if (binlogIgnoreBeforeUpdate && type == BinlogEventType.BEFORE_UPDATE) {
				records.add(currentRecord);
				continue;
			}
			for (int index = 0; index < columnCount; ++index) {
				try {
					convertBinaryRowToRecord(columns[index], currentRow, currentRecord, index);
				} catch (Exception e) {
					throw new HoloClientException(ExceptionCode.DATA_VALUE_ERROR, String.format(
							"convert binlog BinaryRow to holo-client Record failed, \nthe original BinaryRow is %s , \ncurrent Record is %s",
							currentRow, currentRecord), e);

				}
			}
			array.add(currentRecord);
		}
	}
}
