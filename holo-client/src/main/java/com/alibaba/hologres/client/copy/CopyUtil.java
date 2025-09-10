/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.copy;

import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.IdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** CopyUtil. */
public class CopyUtil {
    public static final Logger LOGGER = LoggerFactory.getLogger(CopyUtil.class);
    public static final char QUOTE = '"';
    public static final char ESCAPE = '\\';
    public static final char NULL = 'N';
    public static final char DELIMITER = ',';
    public static final char NEWLINE = '\n';

    private static final int WARN_SKIP_COUNT = 10000;
    static long warnCount = WARN_SKIP_COUNT;

    static final DateTimeFormatter DATE_TIME_FORMATTER =
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
    static final DateTimeFormatter[] DATE_TIME_FORMATTERS = {
        DATE_TIME_FORMATTER, DateTimeFormatter.ISO_DATE_TIME
    };

    private static void logWarnSeldom(String s, Object... obj) {
        if (++warnCount > WARN_SKIP_COUNT) {
            LOGGER.warn(s, obj);
            warnCount = 0;
        }
    }

    public static String buildCopyInSql(
            String tableName,
            List<String> columns,
            CopyFormat format,
            boolean withPk,
            OnConflictAction onConflictAction,
            CopyMode copyMode) {
        StringBuilder sb = new StringBuilder();
        sb.append("copy ").append(tableName).append("(");
        boolean first = true;
        for (String column : columns) {
            if (!first) {
                sb.append(",");
            }
            first = false;
            sb.append(IdentifierUtil.quoteIdentifier(column, true));
        }
        sb.append(")");
        sb.append(" from stdin with(");
        if (copyMode == CopyMode.STREAM) {
            sb.append("stream_mode true");
            if (format.equals(CopyFormat.BINARY)) {
                sb.append(", format binary");
            } else if (format.equals(CopyFormat.CSV)) {
                sb.append(", format csv, DELIMITER '")
                        .append(DELIMITER)
                        .append("', ESCAPE '")
                        .append(ESCAPE)
                        .append("', QUOTE '")
                        .append(QUOTE)
                        .append("', NULL '")
                        .append(NULL)
                        .append("'");
            } else if (format.equals(CopyFormat.BINARYROW)) {
                sb.append(", format binaryrow");
            }
        } else {
            // 目前hologres普通copy（非stream_mode）只支持format csv
            sb.append("format csv, DELIMITER '")
                    .append(DELIMITER)
                    .append("', ESCAPE '")
                    .append(ESCAPE)
                    .append("', QUOTE '")
                    .append(QUOTE)
                    .append("', NULL '")
                    .append(NULL)
                    .append("'");
        }
        // bulkLoad 自Hologres 2.2.25版本起支持on_conflict, 为了兼容历史版本, 分为两种模式
        if (withPk && (copyMode == CopyMode.STREAM || copyMode == CopyMode.BULK_LOAD_ON_CONFLICT)) {
            sb.append(", on_conflict ")
                    .append(
                            onConflictAction == OnConflictAction.INSERT_OR_IGNORE
                                    ? "ignore"
                                    : "update");
        }
        sb.append(")");
        return sb.toString();
    }

    public static String buildCopyInSql(
            TableSchema schema, CopyFormat format, OnConflictAction action) {
        return buildCopyInSql(schema, format, action, CopyMode.STREAM);
    }

    public static String buildCopyInSql(
            TableSchema schema, CopyFormat format, OnConflictAction action, boolean streamMode) {
        return buildCopyInSql(
                schema, format, action, streamMode ? CopyMode.STREAM : CopyMode.BULK_LOAD);
    }

    public static String buildCopyInSql(
            TableSchema schema, CopyFormat format, OnConflictAction action, CopyMode copyMode) {
        List<String> columns =
                Arrays.stream(schema.getColumnSchema())
                        .map(Column::getName)
                        .collect(Collectors.toList());
        return buildCopyInSql(
                schema.getTableNameObj().getFullName(),
                columns,
                format,
                schema.getPrimaryKeys() != null && schema.getPrimaryKeys().length > 0,
                action,
                copyMode);
    }

    public static String buildCopyInSql(
            Record jdbcRecord, CopyFormat format, OnConflictAction action) {
        return buildCopyInSql(jdbcRecord, format, action, CopyMode.STREAM);
    }

    public static String buildCopyInSql(
            Record jdbcRecord, CopyFormat format, OnConflictAction action, boolean streamMode) {
        return buildCopyInSql(
                jdbcRecord, format, action, streamMode ? CopyMode.STREAM : CopyMode.BULK_LOAD);
    }

    public static String buildCopyInSql(
            Record jdbcRecord, CopyFormat format, OnConflictAction action, CopyMode copyMode) {
        TableSchema schema = jdbcRecord.getSchema();
        List<String> columns = new ArrayList<>();
        for (int i = 0; i < schema.getColumnSchema().length; ++i) {
            if (jdbcRecord.isSet(i)) {
                columns.add(schema.getColumn(i).getName());
            }
        }
        // for partition table, the jdbcRecord.getTableName() is partition child table name.
        return buildCopyInSql(
                jdbcRecord.getTableName().getFullName(),
                columns,
                format,
                schema.getPrimaryKeys() != null && schema.getPrimaryKeys().length > 0,
                action,
                copyMode);
    }

    // for read from holo, a more common scenario may be to directly pass in a query
    public static String buildCopyOutSql(String query, CopyFormat copyFormat) {
        switch (copyFormat) {
            case ARROW:
                return "copy (" + query + ") to stdout with (format arrow)";
            case ARROW_LZ4:
                return "copy (" + query + ") to stdout with (format arrow_lz4)";
            default:
                throw new IllegalArgumentException("unsupported copy out format: " + copyFormat);
        }
    }

    public static String buildCopyOutSql(
            String tableName,
            List<String> columns,
            List<Integer> shards,
            CopyFormat copyFormat,
            String filter,
            Set<String> jsonbColumns) {
        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        boolean first = true;
        boolean where = false;
        for (String column : columns) {
            if (!first) {
                sb.append(",");
            }
            first = false;
            sb.append(IdentifierUtil.quoteIdentifier(column, true));
            // jsonb columns need to be cast to text, otherwise they cannot be decoded
            if (jsonbColumns.contains(column)) {
                sb.append("::text");
            }
        }
        sb.append(" from ").append(tableName);
        if (shards != null && !shards.isEmpty()) {
            where = true;
            sb.append(" where ").append("hg_shard_id in (");
            first = true;
            for (Integer shard : shards) {
                if (!first) {
                    sb.append(",");
                }
                first = false;
                sb.append(shard);
            }
            sb.append(")");
        }
        if (filter != null && !filter.isEmpty()) {
            sb.append(" ");
            if (!filter.startsWith("where")) {
                throw new IllegalArgumentException("filter must start with 'where'");
            }
            if (where) {
                filter = "and" + filter.substring(5);
            }
            sb.append(filter);
        }
        return buildCopyOutSql(sb.toString(), copyFormat);
    }

    public static String buildCopyOutSql(
            TableSchema schema,
            List<Integer> shards,
            CopyFormat copyFormat,
            String filter,
            Set<String> jsonbColumns) {
        List<String> columns =
                Arrays.stream(schema.getColumnSchema())
                        .map(Column::getName)
                        .collect(Collectors.toList());
        return buildCopyOutSql(
                schema.getTableNameObj().getFullName(),
                columns,
                shards,
                copyFormat,
                filter,
                jsonbColumns);
    }

    public static void prepareRecordForCopy(Record record) {
        prepareRecordForCopy(record, true, null);
    }

    public static void prepareRecordForCopy(
            Record record, boolean enableDefaultForNotNullColumn, String defaultTimestampText) {
        if (record.getType() == Put.MutationType.INSERT) {
            for (int i = 0; i < record.getSchema().getColumnSchema().length; ++i) {
                Column column = record.getSchema().getColumnSchema()[i];
                fillDefaultValue(
                        record, column, i, enableDefaultForNotNullColumn, defaultTimestampText);
                fillNotSetValue(record, column, i);
            }
        }
    }

    public static void prepareRecordForCopy(Record record, List<String> copyColumnList) {
        prepareRecordForCopy(record, copyColumnList, true, null);
    }

    /**
     * fixed copy不支持insert or replace. 此commit面向的场景是用户在 dataX 或 flink 中选了insert or replace + fixed
     * copy，但columns只选了部分列. 此时拼CopyIn sql应该拼全列（除serial列）, 并把用户没选（上游record中没有value）的列, 填充上default
     * value或者null.
     *
     * @param record 需要处理的record
     * @param copyColumnList CopyIn sql中包含的columns的列表
     * @param enableDefaultForNotNullColumn 对于没有设置default value的not null字段，是否填充上client的默认值
     * @param defaultTimestampText
     *     enableDefaultForNotNullColumn=true时，date/timestamp/timestamptz的默认值
     */
    public static void prepareRecordForCopy(
            Record record,
            List<String> copyColumnList,
            boolean enableDefaultForNotNullColumn,
            String defaultTimestampText) {
        if (record.getType() == Put.MutationType.INSERT) {
            for (int i = 0; i < record.getSchema().getColumnSchema().length; ++i) {
                Column column = record.getSchema().getColumnSchema()[i];
                // 只处理出现在copyColumnList中的column
                if (copyColumnList.contains(column.getName())) {
                    fillDefaultValue(
                            record, column, i, enableDefaultForNotNullColumn, defaultTimestampText);
                    fillNotSetValue(record, column, i);
                }
            }
        }
    }

    /**
     * 把default值解析为值和类型. 例: text default '-' , ["-",null] timestamptz default '2021-12-12
     * 12:12:12.123'::timestamp , ["2021-12-12 12:12:12.123","timestamp"]
     *
     * @param defaultValue 建表时设置的default值
     * @return 2元组，值，[类型]
     */
    private static String[] handleDefaultValue(String defaultValue) {
        String[] ret = defaultValue.split("::");
        if (ret.length == 1) {
            String[] temp = new String[2];
            temp[0] = ret[0];
            temp[1] = null;
            ret = temp;
        }
        if (ret[0].startsWith("'") && ret[0].endsWith("'") && ret[0].length() > 1) {
            ret[0] = ret[0].substring(1, ret[0].length() - 1);
        }
        return ret;
    }

    // LocalDateTime or Timestamp or original String
    public static Object tryConvertStringToTimestampObject(String columnStr) {
        TemporalAccessor specificAccessor = null;
        for (DateTimeFormatter fmt : DATE_TIME_FORMATTERS) {
            try {
                TemporalAccessor accessor = fmt.parse(columnStr);

                LocalDate ld = accessor.query(TemporalQueries.localDate());
                ZoneOffset offset = accessor.query(TemporalQueries.offset());

                if (ld != null) {
                    if (offset == null) {
                        return accessor.query(LocalDateTime::from);
                    } else {
                        specificAccessor = accessor.query(OffsetDateTime::from);
                        long seconds = specificAccessor.getLong(ChronoField.INSTANT_SECONDS);
                        int nano = specificAccessor.get(ChronoField.NANO_OF_SECOND);
                        Timestamp timestamp = new Timestamp(seconds * 1000L + nano / 1000000L);
                        timestamp.setNanos(nano);
                        return timestamp;
                    }
                }
            } catch (Exception ignored) {

            }
        }
        try {
            return new Timestamp(java.sql.Date.valueOf(columnStr).getTime());
        } catch (Exception ignored) {

        }

        logWarnSeldom("Failed to parse String as TIMESTAMP. str: {}", columnStr);
        return columnStr;
    }

    private static void fillDefaultValue(
            Record record,
            Column column,
            int i,
            boolean enableDefaultForNotNullColumn,
            String defaultTimestampText) {
        // 当列没有被set并且not null时，尝试在客户端填充default值
        if (!record.isSet(i) && !column.getAllowNull()) {
            // 对于serial列和生成列不处理default值
            if (column.isSerial() || column.isGeneratedColumn()) {
                return;
            }
            if (column.getDefaultValue() != null) {
                String[] defaultValuePair =
                        handleDefaultValue(String.valueOf(column.getDefaultValue()));
                String defaultValue = defaultValuePair[0];
                switch (column.getType()) {
                    case Types.SMALLINT:
                        record.setObject(i, Short.parseShort(defaultValue));
                        break;
                    case Types.INTEGER:
                        record.setObject(i, Integer.parseInt(defaultValue));
                        break;
                    case Types.BIGINT:
                        record.setObject(i, Long.parseLong(defaultValue));
                        break;
                    case Types.FLOAT:
                    case Types.REAL:
                        record.setObject(i, Float.parseFloat(defaultValue));
                        break;
                    case Types.DOUBLE:
                        record.setObject(i, Double.parseDouble(defaultValue));
                        break;
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        record.setObject(i, new BigDecimal(defaultValue));
                        break;
                    case Types.BOOLEAN:
                    case Types.BIT:
                        record.setObject(i, Boolean.valueOf(defaultValue));
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                        record.setObject(i, defaultValue);
                        break;
                    case Types.TIMESTAMP:
                    case Types.TIMESTAMP_WITH_TIMEZONE:
                        if ("now()".equalsIgnoreCase(defaultValue)
                                || "current_timestamp".equalsIgnoreCase(defaultValue)) {
                            record.setObject(i, Timestamp.valueOf(LocalDateTime.now()));
                        } else {
                            record.setObject(i, tryConvertStringToTimestampObject(defaultValue));
                        }
                        break;
                    case Types.DATE:
                        if ("now()".equalsIgnoreCase(defaultValue)
                                || "current_timestamp".equalsIgnoreCase(defaultValue)) {
                            record.setObject(i, new Date());
                        } else {
                            record.setObject(i, java.sql.Date.valueOf(defaultValue));
                        }
                        break;
                    default:
                        logWarnSeldom(
                                "unsupported default type,{}({})",
                                column.getType(),
                                column.getTypeName());
                }
            } else if (enableDefaultForNotNullColumn) {
                switch (column.getType()) {
                    case Types.SMALLINT:
                        record.setObject(i, (short) 0);
                        break;
                    case Types.INTEGER:
                        record.setObject(i, 0);
                        break;
                    case Types.BIGINT:
                        record.setObject(i, 0L);
                        break;
                    case Types.FLOAT:
                    case Types.REAL:
                        record.setObject(i, 0F);
                        break;
                    case Types.DOUBLE:
                        record.setObject(i, 0D);
                        break;
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        record.setObject(i, BigDecimal.ZERO);
                        break;
                    case Types.BOOLEAN:
                    case Types.BIT:
                        record.setObject(i, false);
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                        record.setObject(i, "");
                        break;
                    case Types.TIMESTAMP:
                    case Types.TIMESTAMP_WITH_TIMEZONE:
                        if (defaultTimestampText == null) {
                            record.setObject(i, new Timestamp(0L));
                        } else {
                            record.setObject(
                                    i, tryConvertStringToTimestampObject(defaultTimestampText));
                        }
                        break;
                    case Types.DATE:
                        if (defaultTimestampText == null) {
                            record.setObject(i, new Date(0L));
                        } else {
                            Object timestampObject =
                                    tryConvertStringToTimestampObject(defaultTimestampText);
                            if (timestampObject instanceof Timestamp) {
                                record.setObject(i, (Date) timestampObject);
                            } else if (timestampObject instanceof LocalDateTime) {
                                record.setObject(
                                        i,
                                        (Date) Timestamp.valueOf((LocalDateTime) timestampObject));
                            } else {
                                record.setObject(i, java.sql.Date.valueOf(defaultTimestampText));
                            }
                        }
                        break;
                    default:
                        logWarnSeldom(
                                "unsupported default type,{}({})",
                                column.getType(),
                                column.getTypeName());
                }
            }
        }
    }

    private static void fillNotSetValue(Record record, Column column, int i) {
        if (!record.isSet(i)) {
            if (column.isSerial() || column.isGeneratedColumn()) {
                return;
            }
            record.setObject(i, null);
        }
    }
}
