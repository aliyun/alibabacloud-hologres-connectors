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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

    /**
     * binlog中值是UTF-8文本、可以正确按字符串解析的{@link Types#OTHER}类型的typeName.
     *
     * <p>json本身在存储里就是文本；jsonb依赖服务端GUC {@code hg_experimental_enable_binlog_jsonb}
     * （默认打开）在写binlog前转成JSON文本。该GUC被关闭时jsonb会退化成内部二进制，此时解析结果是脏数据， 属于已知残留缺口（客户端无法感知该GUC）。
     */
    private static final Set<String> BINLOG_TEXT_ENCODED_OTHER_TYPE_NAMES =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList("json", "jsonb")));

    /**
     * binlog中值是可移植的二进制序列化结果、按byte[]返回对用户有意义的{@link Types#OTHER}类型的typeName.
     *
     * <p>roaringbitmap的binlog字节就是标准的RoaringBitmap序列化格式（已验证与服务端 {@code col::bytea}
     * 完全一致），用户拿到byte[]可以直接反序列化。
     *
     * <p>geometry/geography不在此列：它们的binlog字节是PostGIS内部的GSERIALIZED，既不是WKB/EWKB也不是WKT，
     * JTS/GeoTools都解析不了，返回byte[]没有实际价值。
     */
    private static final Set<String> BINLOG_BYTES_ENCODED_OTHER_TYPE_NAMES =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList("roaringbitmap")));

    /*
     * Types.OTHER 的完整处理策略、实测依据和取舍，见 decodeOtherTypeAsText 的注释，
     * 避免同一份结论出现两处描述而互相漂移。
     *
     * 后续项：uuid/interval 有明确的惯用 Java 表示（UUID / PGInterval），应该单独实现真正的转换，
     * 而不是继续返回一段无意义的字符串。
     */

    /** 已经打过WARN的列，避免每行都打日志刷爆日志. */
    private final Set<String> warnedOtherColumns = new HashSet<>();

    /** 已经打过ERROR的列，与WARN分开记录，避免更重要的ERROR被先打的WARN吞掉. */
    private final Set<String> erroredOtherColumns = new HashSet<>();

    /**
     * 严格模式的UTF-8解码器，用于判断{@link Types#OTHER}列的字节是否真的是合法文本。
     *
     * <p>decoder实例是每个shard一个、单线程使用（见BinlogActionHandler#doHandle），所以可以复用同一个
     * CharsetDecoder，避免每行每列都新建一个。使用前必须reset。
     */
    private final CharsetDecoder strictUtf8Decoder =
            StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);

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

    /**
     * 按文本解析{@link Types#OTHER}列，解析不出来时降级为"该列不写入"而不是抛异常。
     *
     * <p>会落到{@link Types#OTHER}的列类型在 Hologres 5.1 上实测共 18 个：json、jsonb、roaringbitmap、
     * geometry、geography、uuid、interval、inet、varbit、tsvector、tsquery、point、line、lseg、box、path、
     * circle、polygon。其中只有 json/jsonb 在 binlog 里真的是文本，roaringbitmap 走 byte[]，其余类型都是各自的 内部二进制表示。
     *
     * <p>之所以不对"不支持的类型"直接报错：这些类型能否按字符串解析，取决于**该行字节是否恰好是合法 UTF-8**， 而不是类型固定行为。已在 origin/master 实测
     * {@code interval '1 day'}、{@code point(0,0)}、全 0x01 的 uuid、{@code inet '0.0.0.0'}、{@code
     * varbit B'00000001'} 都能"成功"返回一段控制字符脏字符串。 直接报错会把这些"当前恰好能跑、且下游并不关心该列"的存量作业变成致命失败 （{@link
     * HoloClientException} 在 BinlogActionHandler 里不重试、直接终止该 shard 的消费）。
     *
     * <p>所以这里的策略是：
     *
     * <ul>
     *   <li>字节是合法 UTF-8 → 按字符串写入，与改动前一致；若该类型不在文本白名单内，额外按列打一次 WARN， 提示解析结果很可能无意义、可用 projection 排除；
     *   <li>字节不是合法 UTF-8 → 按列打一次 ERROR，并且**不写入该列**（保持 unset），让消费继续。 保持 unset 而不是写
     *       null，是为了不让"解析失败"被下游误当成"源端真的是 NULL"—— 回写场景下 {@link Record#getBitSet()} 决定哪些列进 SQL，unset
     *       的列会被跳过而不是用 NULL 覆盖原值。
     * </ul>
     *
     * <p>注意这里刻意不 catch {@code getByteArray} 自身的异常：BinaryRow 真的损坏（offset/len 越界）时应该照旧
     * 向上抛出可定位的错误，不能被误诊成"非法 UTF-8"后静默跳过。
     *
     * @param columnIndex 列在完整schema中的下标，用于写入Record
     * @param offsetIndex 列在BinaryRow中的字段下标（已含3个binlog元信息字段偏移）
     */
    private void decodeOtherTypeAsText(
            Column column,
            BinaryRow currentRow,
            Record currentRecord,
            int columnIndex,
            int offsetIndex) {
        // 直接取字节自己做严格 UTF-8 解码，而不是靠 catch getString 的异常来判断，
        // 这样才能把"非法 UTF-8"和"BinaryRow 损坏"区分开
        byte[] raw = currentRow.getByteArray(offsetIndex);
        String text;
        try {
            text = strictUtf8Decoder.reset().decode(ByteBuffer.wrap(raw)).toString();
        } catch (CharacterCodingException e) {
            if (erroredOtherColumns.add(logKey(column))) {
                LOGGER.error(
                        "Table {} column \"{}\" of type {} can not be decoded from binlog: its binlog value is the "
                                + "type's internal binary representation and is not valid UTF-8. This column is left "
                                + "unset so that consuming can continue and downstream writers will not overwrite it "
                                + "with NULL. Please exclude this column from the subscription by projection "
                                + "(Subscribe.newStartTimeBuilder/newOffsetBuilder#addProjectionColumnNamesToSubscribe).",
                        schema.getTableNameObj().getFullName(),
                        column.getName(),
                        column.getTypeName(),
                        e);
            }
            return;
        }
        currentRecord.setObject(columnIndex, text);
        if (!BINLOG_TEXT_ENCODED_OTHER_TYPE_NAMES.contains(column.getTypeName())
                && warnedOtherColumns.add(logKey(column))) {
            LOGGER.warn(
                    "Table {} column \"{}\" has type {} which is not supported by binlog decoding: its binlog value "
                            + "is the type's internal binary representation, so the decoded value is very likely "
                            + "meaningless. Please exclude this column from the subscription by projection "
                            + "(Subscribe.newStartTimeBuilder/newOffsetBuilder#addProjectionColumnNamesToSubscribe) "
                            + "if you do not need it.",
                    schema.getTableNameObj().getFullName(),
                    column.getName(),
                    column.getTypeName());
        }
    }

    /** 日志去重的 key，带上 typeName，避免 ALTER 改类型后新类型的告警被旧记录吞掉. */
    private static String logKey(Column column) {
        return column.getName() + "/" + column.getTypeName();
    }

    private void convertBinaryRowToRecord(
            Column column, BinaryRow currentRow, Record currentRecord, int index)
            throws HoloClientException {
        int offsetIndex = index + 3;
        int columnIndex = schema.getColumnIndex(column.getName());
        if (currentRow.isNullAt(offsetIndex)) {
            currentRecord.setObject(columnIndex, null);
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
                if (BINLOG_BYTES_ENCODED_OTHER_TYPE_NAMES.contains(column.getTypeName())) {
                    currentRecord.setObject(columnIndex, currentRow.getByteArray(offsetIndex));
                } else {
                    decodeOtherTypeAsText(
                            column, currentRow, currentRecord, columnIndex, offsetIndex);
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
                    long millis = Math.floorDiv(microseconds, 1000L);
                    Timestamp timestamp =
                            new Timestamp(millis - TimeZone.getDefault().getOffset(millis));
                    timestamp.setNanos((int) (Math.floorMod(microseconds, 1_000_000L) * 1_000));
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
                                        + column.getTypeName()
                                        + " column:"
                                        + column.getName());
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
                                + column.getTypeName()
                                + " column:"
                                + column.getName());
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
                } catch (HoloClientException e) {
                    // convertBinaryRowToRecord 主动抛出的"类型不支持"等错误本身已经足够明确，
                    // 再套一层整行 dump 只会把可操作的信息埋掉，这里直接向上抛
                    throw e;
                } catch (Exception e) {
                    throw new HoloClientException(
                            ExceptionCode.DATA_VALUE_ERROR,
                            String.format(
                                    "convert binlog BinaryRow to holo-client Record failed on column %s(type %s), \nthe original BinaryRow is %s , \ncurrent Record is %s",
                                    columns[index].getName(),
                                    columns[index].getTypeName(),
                                    currentRow,
                                    currentRecord),
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
