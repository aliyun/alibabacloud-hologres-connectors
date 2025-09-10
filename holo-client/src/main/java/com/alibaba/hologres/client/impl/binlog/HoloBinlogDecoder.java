package com.alibaba.hologres.client.impl.binlog;

import com.alibaba.blink.dataformat.BinaryArray;
import com.alibaba.blink.dataformat.BinaryRow;
import com.alibaba.blink.memory.MemorySegment;
import com.alibaba.blink.memory.MemorySegmentFactory;
import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.binlog.BinlogRecord;
import com.alibaba.hologres.client.utils.Tuple;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.postgresql.jdbc.ArrayUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.security.InvalidParameterException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

/** 将Hologres的binlog解析为Holo-client的Record格式. */
public class HoloBinlogDecoder {
    public static final int BINLOG_PROTOCOL_VERSION = 0;
    public static final int BINLOG_HEADER_LEN = 24;
    public static final long TIMEZONE_OFFSET = TimeZone.getDefault().getRawOffset();
    public static final Logger LOGGER = LoggerFactory.getLogger(HoloBinlogDecoder.class);

    private final boolean enableCompression;
    private long totalCompressedSize = 0;
    private long totalDecompressedSize = 0;
    private final String[] projectionColumnNames;
    private Column[] columns;
    private int columnCount;
    private long tableVersion = -1;
    private TableSchema schema;
    private Boolean binlogIgnoreBeforeUpdate = false;
    private Boolean binlogIgnoreDelete = false;
    private TableSchemaSupplier tableSchemaSupplier;

    public HoloBinlogDecoder(
            TableSchema schema,
            String[] projectionColumnNames,
            boolean enableCompression,
            Boolean binlogIgnoreDelete,
            Boolean binlogIgnoreBeforeUpdate)
            throws HoloClientException {
        this.projectionColumnNames = projectionColumnNames;
        this.binlogIgnoreDelete = binlogIgnoreDelete;
        this.binlogIgnoreBeforeUpdate = binlogIgnoreBeforeUpdate;
        this.enableCompression = enableCompression;
        init(schema);
    }

    public HoloBinlogDecoder(
            TableSchemaSupplier supplier,
            String[] projectionColumnNames,
            boolean enableCompression,
            Boolean binlogIgnoreDelete,
            Boolean binlogIgnoreBeforeUpdate)
            throws HoloClientException {
        this.tableSchemaSupplier = supplier;
        this.projectionColumnNames = projectionColumnNames;
        this.binlogIgnoreDelete = binlogIgnoreDelete;
        this.binlogIgnoreBeforeUpdate = binlogIgnoreBeforeUpdate;
        this.enableCompression = enableCompression;
        init(supplier.apply());
    }

    public HoloBinlogDecoder(TableSchema schema) throws HoloClientException {
        this(schema, new String[0], false, false, false);
    }

    public HoloBinlogDecoder(TableSchemaSupplier supplier) throws HoloClientException {
        this(supplier, new String[0], false, false, false);
    }

    private static long parseSchemaVersion(TableSchema schema) throws HoloClientException {
        try {
            return Long.parseLong(schema.getSchemaVersion());
        } catch (Exception e) {
            throw new HoloClientException(
                    ExceptionCode.INTERNAL_ERROR,
                    String.format(
                            "parse schema version fail for table %s, schema version %s",
                            schema.getTableNameObj().getFullName(), schema.getSchemaVersion()),
                    e);
        }
    }

    private void init(TableSchema schema) throws HoloClientException {
        this.schema = schema;
        if (projectionColumnNames != null && projectionColumnNames.length > 0) {
            Set<String> columnNameSet = new HashSet<>(Arrays.asList(projectionColumnNames));
            this.columns = new Column[columnNameSet.size()];
            int i = 0;
            // 消费部分列,列的顺序和schema保持一致
            for (Column column : schema.getColumnSchema()) {
                if (columnNameSet.contains(column.getName())) {
                    this.columns[i++] = column;
                }
            }
            if (i != columnNameSet.size()) {
                throw new HoloClientException(
                        ExceptionCode.INVALID_REQUEST,
                        String.format(
                                "projection column names %s is not subset of table columns %s",
                                Arrays.toString(projectionColumnNames),
                                Arrays.toString(schema.getColumnSchema())));
            }
        } else {
            this.columns = schema.getColumnSchema();
        }
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

    private List<BinaryRow> deserialize(int shardId, byte[] headerBytes, byte[] dataBytes)
            throws HoloClientException {
        LongBuffer longBuffer =
                ByteBuffer.wrap(headerBytes).order(ByteOrder.BIG_ENDIAN).asLongBuffer();
        long binlogProtocolVersion = longBuffer.get(0);
        long currentTableVersion = longBuffer.get(1);
        if (BINLOG_PROTOCOL_VERSION != binlogProtocolVersion) {
            throw new IllegalStateException(
                    "binlog version mismatch, expected: "
                            + BINLOG_PROTOCOL_VERSION
                            + ", actual: "
                            + binlogProtocolVersion);
        }
        if (currentTableVersion != tableVersion) {
            LOGGER.warn(
                    "Table {} have been altered, current client table version id is {}, binlog table version id is {}.",
                    schema.getTableNameObj().getFullName(),
                    tableVersion,
                    currentTableVersion);
            if (tableSchemaSupplier != null) {
                // TODO 先写死，正常来说不应该发生retry
                int tryCount = 3;
                while (tableVersion < currentTableVersion && --tryCount > 0) {
                    init(tableSchemaSupplier.apply());
                }
                if (tableVersion != currentTableVersion) {
                    throw new HoloClientException(
                            ExceptionCode.META_NOT_MATCH,
                            String.format(
                                    "binlog table version for table %s is %s but client table version is %s after refresh",
                                    schema.getTableNameObj().getFullName(),
                                    currentTableVersion,
                                    schema.getSchemaVersion()));
                } else {
                    LOGGER.info(
                            "Table {} have been altered, update shardId [{}] current client table version id to {}.",
                            schema.getTableNameObj().getFullName(),
                            shardId,
                            tableVersion);
                }
            } else {
                throw new HoloClientException(
                        ExceptionCode.META_NOT_MATCH,
                        String.format(
                                "binlog table version for table %s is %s but client table version is %s ",
                                schema.getTableNameObj().getFullName(),
                                currentTableVersion,
                                schema.getSchemaVersion()));
            }
        }

        IntBuffer buffer = ByteBuffer.wrap(dataBytes).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        int rowCount = buffer.get(1);
        MemorySegment segment = MemorySegmentFactory.wrap(dataBytes);
        List<BinaryRow> rows = new ArrayList();
        int emptyRowCount = 0;
        for (int i = 0; i < rowCount; ++i) {
            int offset = buffer.get(2 + i);
            int offsetNext = i == rowCount - 1 ? dataBytes.length : buffer.get(3 + i);
            if (offset > offsetNext) {
                throw new IllegalStateException(
                        "invalid offset in pos "
                                + i
                                + ", offset="
                                + offset
                                + ", offsetNext="
                                + offsetNext);
            }

            BinaryRow row = new BinaryRow(this.columnCount + 3);
            row.pointTo(segment, offset, offsetNext - offset);
            if (row.empty()) {
                emptyRowCount++;
                // empty row, may be filtered out by the predicate
                continue;
            }
            rows.add(row);
        }
        if (emptyRowCount > 0) {
            LOGGER.debug(
                    "Table {} have {} empty rows in this batch, total rows {}",
                    schema.getTableNameObj().getFullName(),
                    emptyRowCount,
                    rowCount);
        }
        return rows;
    }

    private void convertBinaryRowToRecord(
            Column column, BinaryRow currentRow, Record currentRecord, int index)
            throws HoloClientException {
        int offsetIndex = index + 3;
        int columnIndex = schema.getColumnIndex(column.getName());
        if (currentRow.isNullAt(offsetIndex)) {
            currentRecord.setObject(index, null);
            return;
        }
        switch (column.getType()) {
            case Types.CHAR:
                currentRecord.setObject(
                        columnIndex,
                        String.format(
                                "%-" + column.getPrecision() + "s",
                                currentRow.getString(offsetIndex)));
                break;
            case Types.VARCHAR:
                currentRecord.setObject(columnIndex, currentRow.getString(offsetIndex));
                break;
            case Types.OTHER:
                if ("roaringbitmap".equals(column.getTypeName())) {
                    currentRecord.setObject(columnIndex, currentRow.getByteArray(offsetIndex));
                } else {
                    currentRecord.setObject(columnIndex, currentRow.getString(offsetIndex));
                }
                break;
            case Types.DATE:
                currentRecord.setObject(
                        columnIndex,
                        Date.valueOf(LocalDate.ofEpochDay(currentRow.getInt(offsetIndex))));
                break;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                if ("timetz".equals(column.getTypeName())) {
                    long time =
                            ByteBuffer.wrap(currentRow.getByteArray(offsetIndex))
                                    .order(ByteOrder.LITTLE_ENDIAN)
                                    .asLongBuffer()
                                    .get(0);
                    int zoneOffset =
                            ByteBuffer.wrap(currentRow.getByteArray(offsetIndex))
                                    .order(ByteOrder.LITTLE_ENDIAN)
                                    .asIntBuffer()
                                    .get(2);
                    currentRecord.setObject(
                            columnIndex, new Time(time / 1000L + zoneOffset * 1000L));
                } else {
                    currentRecord.setObject(
                            columnIndex,
                            new Time(currentRow.getLong(offsetIndex) / 1000L - TIMEZONE_OFFSET));
                }
                break;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                if ("timestamptz".equals(column.getTypeName())) {
                    currentRecord.setObject(
                            columnIndex, new Timestamp(currentRow.getLong(offsetIndex)));
                } else {
                    long microseconds = currentRow.getLong(offsetIndex);
                    Timestamp timestamp =
                            new Timestamp(
                                    microseconds / 1000L
                                            - TimeZone.getDefault()
                                                    .getOffset(microseconds / 1000L));
                    timestamp.setNanos((int) ((microseconds % 1_000_000L) * 1_000));
                    currentRecord.setObject(columnIndex, timestamp);
                }
                break;
            case Types.SMALLINT:
                currentRecord.setObject(columnIndex, currentRow.getShort(offsetIndex));
                break;
            case Types.INTEGER:
                currentRecord.setObject(columnIndex, currentRow.getInt(offsetIndex));
                break;
            case Types.BIGINT:
                currentRecord.setObject(columnIndex, currentRow.getLong(offsetIndex));
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                int scale = column.getScale();
                byte[] value = currentRow.getByteArray(offsetIndex);
                ArrayUtil.reverse(value);
                BigInteger bigInteger = new BigInteger(value);
                BigDecimal bigDecimal = new BigDecimal(bigInteger);
                bigDecimal = bigDecimal.movePointLeft(scale);
                bigDecimal = bigDecimal.setScale(scale, RoundingMode.DOWN);
                currentRecord.setObject(columnIndex, bigDecimal);
                break;
            case Types.FLOAT:
            case Types.REAL:
                currentRecord.setObject(columnIndex, currentRow.getFloat(offsetIndex));
                break;
            case Types.DOUBLE:
                currentRecord.setObject(columnIndex, currentRow.getDouble(offsetIndex));
                break;
            case Types.BINARY:
            case Types.VARBINARY:
                currentRecord.setObject(columnIndex, currentRow.getByteArray(offsetIndex));
                break;
            case Types.ARRAY:
                switch (column.getTypeName()) {
                    case "_int4":
                        currentRecord.setObject(
                                columnIndex, currentRow.getArray(offsetIndex).toIntArray());
                        break;
                    case "_int8":
                        currentRecord.setObject(
                                columnIndex, currentRow.getArray(offsetIndex).toLongArray());
                        break;
                    case "_float4":
                        currentRecord.setObject(
                                columnIndex, currentRow.getArray(offsetIndex).toFloatArray());
                        break;
                    case "_float8":
                        currentRecord.setObject(
                                columnIndex, currentRow.getArray(offsetIndex).toDoubleArray());
                        break;
                    case "_bool":
                        currentRecord.setObject(
                                columnIndex, currentRow.getArray(offsetIndex).toBooleanArray());
                        break;
                    case "_text":
                    case "_varchar":
                        BinaryArray binaryArray = currentRow.getArray(offsetIndex);
                        String[] stringArrays = new String[binaryArray.numElements()];
                        for (int i = 0; i < binaryArray.numElements(); i++) {
                            stringArrays[i] = binaryArray.getString(i);
                        }
                        currentRecord.setObject(columnIndex, stringArrays);
                        break;
                    default:
                        throw new HoloClientException(
                                ExceptionCode.DATA_TYPE_ERROR,
                                "unsupported array type "
                                        + column.getType()
                                        + " type name:"
                                        + column.getTypeName());
                }
                break;
            case Types.BOOLEAN:
            case Types.BIT:
                currentRecord.setObject(columnIndex, currentRow.getBoolean(offsetIndex));
                break;
            default:
                throw new HoloClientException(
                        ExceptionCode.DATA_TYPE_ERROR,
                        "unsupported type "
                                + column.getType()
                                + " type name:"
                                + column.getTypeName());
        }
    }

    /**
     * @param byteBuffer 包含header和data两部分
     *     <p>header部分为前 16 byte，结构如下: 0 - 7: binlog_protocol version, (long) 8 - 15: table version,
     *     (long)
     *     <p>data部分为header部分之后，结构如下: 0 - 4: binlog version, (int) 5 - 8: row count, (int) 9 - :
     *     each row‘s offset, (int)
     */
    public List<BinlogRecord> decode(int shardId, ByteBuffer byteBuffer)
            throws HoloClientException {
        ArrayBuffer<BinlogRecord> array = new ArrayBuffer<>(10, BinlogRecord[].class);
        decode(shardId, byteBuffer, array);
        List<BinlogRecord> list = new ArrayList<>();
        array.beginRead();
        while (array.remain() > 0) {
            list.add(array.pop());
        }
        return list;
    }

    public void decode(int shardId, ByteBuffer byteBuffer, ArrayBuffer<BinlogRecord> array)
            throws HoloClientException {
        if (enableCompression) {
            byteBuffer = decompress(byteBuffer);
        }
        if (byteBuffer.limit() < BINLOG_HEADER_LEN) {
            throw new IllegalStateException(
                    String.format(
                            "Invalid ByteBuffer, limit %s < %s.",
                            byteBuffer.limit(), BINLOG_HEADER_LEN));
        }
        byte[] headerBytes = new byte[16];
        byte[] dataBytes = new byte[byteBuffer.limit() - 16];
        System.arraycopy(byteBuffer.array(), byteBuffer.arrayOffset(), headerBytes, 0, 16);
        System.arraycopy(
                byteBuffer.array(),
                byteBuffer.arrayOffset() + 16,
                dataBytes,
                0,
                byteBuffer.limit() - 16);
        List<BinaryRow> list = deserialize(shardId, headerBytes, dataBytes);

        List<BinlogRecord> records = new ArrayList<>();
        for (BinaryRow currentRow : list) {
            long lsn = currentRow.getLong(0);
            long eventType = currentRow.getLong(1);
            long timestamp = currentRow.getLong(2);

            BinlogEventType type = null;
            try {
                type = BinlogEventType.of(eventType);
            } catch (InvalidParameterException e) {
                throw new HoloClientException(
                        ExceptionCode.INTERNAL_ERROR, "unknow binlog eventtype " + eventType, e);
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
                    throw new HoloClientException(
                            ExceptionCode.DATA_VALUE_ERROR,
                            String.format(
                                    "convert binlog BinaryRow to holo-client Record failed, \nthe original BinaryRow is %s , \ncurrent Record is %s",
                                    currentRow, currentRecord),
                            e);
                }
            }
            array.add(currentRecord);
        }
    }

    private ByteBuffer decompress(ByteBuffer compressedData) throws HoloClientException {
        int decompressedSize = compressedData.getInt();
        LZ4SafeDecompressor decompressor = LZ4Factory.fastestInstance().safeDecompressor();
        ByteBuffer decompressed = ByteBuffer.wrap(new byte[decompressedSize]);
        try {
            decompressor.decompress(
                    compressedData,
                    Integer.BYTES,
                    compressedData.remaining(),
                    decompressed,
                    0,
                    decompressedSize);
        } catch (Exception e) {
            throw new HoloClientException(
                    ExceptionCode.DATA_VALUE_ERROR,
                    "decompress data failed, may be choose read binlog not enable compression.",
                    e);
        }
        totalCompressedSize += compressedData.limit();
        totalDecompressedSize += decompressedSize;
        return decompressed;
    }

    public Tuple<Long, Long> trace() {
        if (enableCompression) {
            LOGGER.info(
                    "binlog read compressed data, total compressed data size: {}, total decompressed data size: {}",
                    totalCompressedSize,
                    totalDecompressedSize);
            return new Tuple<>(totalCompressedSize, totalDecompressedSize);
        }
        return new Tuple<>(0L, 0L);
    }
}
