package org.postgresql.jdbc;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * TimestampUtil.
 */

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
			Timestamp ts = (Timestamp) obj;
			seconds = ts.getTime() / 1000;
			nanos = ts.getNanos();
			if ("timestamp".equals(typeName)) {
				nanos += TimeZone.getDefault().getRawOffset() * 1000000L;
			}
		} else if (obj instanceof LocalDateTime) {
			LocalDateTime time = (LocalDateTime) obj;
			seconds = time.toEpochSecond("timestamp".equals(typeName) ? ZoneOffset.UTC : ZoneOffset.systemDefault().getRules().getOffset(time));
			nanos = time.getNano();
		} else if (obj instanceof String) {
			OffsetDateTime dateTime =
					OffsetDateTime.parse((String) obj, DATE_TIME_FORMATTER);
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
				TimeUnit.SECONDS.toMicros(seconds)
						+ TimeUnit.NANOSECONDS.toMicros(nanos + 500);
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
}
