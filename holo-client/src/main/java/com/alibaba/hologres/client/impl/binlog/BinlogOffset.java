package com.alibaba.hologres.client.impl.binlog;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

/** BinlogOffset 用于为每个shard单独设置读取binlog的起始点. */
public class BinlogOffset {

    /**
     * 参考 DateTimeFormatter中ISO_LOCAL_DATE_TIME实现, 主要将日期和时间中的T分隔符调整为空格; Builder相比ofPattern方式更加灵活,
     * 比如ISO_LOCAL_TIME可以匹配0-9位NANO_OF_SECOND.
     */
    public static final DateTimeFormatter SQL_LOCAL_DATE_TIME;

    static {
        SQL_LOCAL_DATE_TIME =
                new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .append(ISO_LOCAL_DATE)
                        .appendLiteral(' ')
                        .append(ISO_LOCAL_TIME)
                        .toFormatter();
    }

    public static final DateTimeFormatter SQL_ZONED_DATE_TIME;

    static {
        SQL_ZONED_DATE_TIME =
                new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .append(SQL_LOCAL_DATE_TIME)
                        .parseLenient()
                        .appendOffsetId()
                        .parseStrict()
                        .toFormatter();
    }

    /** sequence 即 hg_binlog_lsn. */
    private long sequence;

    /** timestamp 对应 hg_binlog_timestamp_us. */
    private long timestamp;

    private String startTimeText;

    public BinlogOffset() {
        this(-1, -1);
    }

    public BinlogOffset(long sequence, long timestamp) {
        this.sequence = sequence;
        this.timestamp = timestamp;
        setTimestamp(timestamp);
    }

    public long getSequence() {
        return sequence;
    }

    public BinlogOffset setSequence(long sequence) {
        this.sequence = sequence;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getStartTimeText() {
        return startTimeText;
    }

    public BinlogOffset setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        ZonedDateTime dateTime =
                ZonedDateTime.ofInstant(
                        java.time.Instant.ofEpochSecond(
                                timestamp / 1000_000L, (timestamp % 1_000_000) * 1000),
                        ZoneId.of("Asia/Shanghai"));
        this.startTimeText = dateTime.format(SQL_ZONED_DATE_TIME);
        return this;
    }

    public BinlogOffset setTimestamp(String timestampStr) {
        if (timestampStr == null) {
            return this;
        }
        ZonedDateTime zonedDateTime;
        try {
            if (timestampStr.contains("T")) {
                try {
                    // ISO_ZONED_DATE_TIME: 2007-12-03T10:15:30.1+01:00[Europe/ Paris]
                    zonedDateTime = ZonedDateTime.parse(timestampStr);
                } catch (DateTimeParseException e) {
                    // ISO_LOCAL_DATE_TIME: 2007-12-03T10:15:30.1
                    zonedDateTime =
                            LocalDateTime.parse(timestampStr)
                                    .atZone(TimeZone.getDefault().toZoneId());
                }
            } else {
                try {
                    zonedDateTime = ZonedDateTime.parse(timestampStr, SQL_ZONED_DATE_TIME);
                } catch (DateTimeParseException e) {
                    zonedDateTime =
                            LocalDateTime.parse(timestampStr, SQL_LOCAL_DATE_TIME)
                                    .atZone(TimeZone.getDefault().toZoneId());
                }
            }
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                    "Invalid timestamp format, it is recommended to pass timestamp in microseconds since epoch (Unix time)",
                    e);
        }
        timestamp =
                zonedDateTime.toInstant().getEpochSecond() * 1000_000L
                        + zonedDateTime.getNano() / 1000;
        setTimestamp(timestamp);
        return this;
    }

    /** lsn和timestamp至少有一个大于-1才表示有效的进行了offset的设置. */
    public boolean isValid() {
        return sequence > -1 || timestamp > -1;
    }

    public boolean hasSequence() {
        return sequence > -1;
    }

    public boolean hasTimestamp() {
        return timestamp > -1;
    }

    public BinlogOffset next() {
        return new BinlogOffset(this.sequence >= 0 ? this.sequence + 1 : -1, this.timestamp);
    }

    @Override
    public String toString() {
        return "(" + sequence + ", " + timestamp + ")";
    }
}
