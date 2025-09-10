package org.postgresql.jdbc;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.concurrent.TimeUnit;

/** TimestampUtil. */
public class TimestampUtil {

    public static long timestampToPgEpochMicroSecond(Object obj, String typeName) {
        return timestampToMicroSecond(obj, typeName, true);
    }

    public static long timestampToMillisecond(Object obj, String typeName) {
        return timestampToMicroSecond(obj, typeName, false) / 1000L;
    }

    /**
     * 将Timestamp字段转为需要的微秒值.
     *
     * @param obj 传入的Timestamp数据,可能是不同的类型
     * @param typeName holo表的字段类型名，timestamp和timestamptz需要分别处理
     * @param pgEpoch 返回的微秒的起始时间,pg的epoch起始时间是2000-01-01零点,java的起始时间是1970-01-01零点
     * @return 微秒单位的long类型
     */
    public static long timestampToMicroSecond(Object obj, String typeName, boolean pgEpoch) {
        long seconds;
        long nanos;
        if (obj instanceof Timestamp) {
            obj = ((Timestamp) obj).toLocalDateTime();
        }
        if (obj instanceof LocalDateTime) {
            LocalDateTime time = (LocalDateTime) obj;
            ZonedDateTime zdt =
                    time.atZone(
                            "timestamp".equals(typeName)
                                    ? ZoneId.of("UTC")
                                    : ZoneId.systemDefault());
            seconds = zdt.toEpochSecond();
            nanos = zdt.getNano();
        } else if (obj instanceof String) {
            OffsetDateTime dateTime = OffsetDateTime.parse((String) obj, DATE_TIME_FORMATTER);
            seconds = dateTime.toEpochSecond();
            nanos = dateTime.getNano();
        } else if (obj instanceof Number) {
            long ms = ((Number) obj).longValue();
            seconds = ms / 1000L;
            nanos = (ms % 1000) * 1000000L;
        } else if (obj instanceof java.util.Date) {
            long ms = ((java.util.Date) obj).getTime();
            seconds = ms / 1000L;
            nanos = (ms % 1000) * 1000000L;
        } else {
            throw new RuntimeException(
                    "unsupported type for timestamp " + obj.getClass().getName());
        }
        if (pgEpoch) {
            seconds = javaEpochToPg(seconds, TimeUnit.SECONDS);
        }
        // Convert to micros rounding nanoseconds
        long micros =
                TimeUnit.SECONDS.toMicros(seconds) + TimeUnit.NANOSECONDS.toMicros(nanos + 500);
        return micros;
    }

    private static final DateTimeFormatter DATE_TIME_FORMATTER =
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

    private static final long PG_EPOCH_SECS = 946684800L;

    private static long javaEpochToPg(long value, TimeUnit timeUnit) {
        return value - timeUnit.convert(PG_EPOCH_SECS, TimeUnit.SECONDS);
    }

    private static final DateTimeFormatter BASIC_ISO_DATE_WITHOUT_OFFSET =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendValue(ChronoField.YEAR, 4)
                    .appendValue(ChronoField.MONTH_OF_YEAR, 2)
                    .appendValue(ChronoField.DAY_OF_MONTH, 2)
                    .toFormatter();

    /**
     * 将Date字段转为java.sql.Date.
     *
     * <p>用户传入的Object可能是不同的类型及格式(YYYMMDD和YYYY-MM-DD)
     *
     * <p>主要用于以下场景: <br>
     * 1. DATE类型做主键时, ShardUtil计算shard需要java.sql.Date类型 <br>
     * 2. DATE类型做分区键时, 通过java.sql.Date的YYYY-MM-DD格式保证可以正确获取到分区
     *
     * @param obj 传入的Date数据,可能是不同的类型
     * @return java.sql.Date
     */
    public static Date formatDateObject(Object obj) {
        if (obj instanceof Date) {
            return (Date) obj;
        } else if (obj instanceof LocalDate) {
            return Date.valueOf((LocalDate) obj);
        } else if (obj instanceof String) {
            try {
                // YYYY-MM-DD
                return Date.valueOf((String) obj);
            } catch (IllegalArgumentException e) {
                // YYYYMMDD
                return Date.valueOf(LocalDate.parse((String) obj, BASIC_ISO_DATE_WITHOUT_OFFSET));
            }
        } else if (obj instanceof Number) {
            return new Date(((Number) obj).longValue());
        } else {
            throw new RuntimeException("unsupported type for date " + obj.getClass().getName());
        }
    }
}
