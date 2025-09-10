package com.alibaba.hologres.client.utils;

import com.alibaba.hologres.client.model.AutoPartitioning;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.alibaba.hologres.client.model.AutoPartitioning.AutoPartitioningTimeFormat.YYYY_MM;
import static com.alibaba.hologres.client.model.AutoPartitioning.AutoPartitioningTimeFormat.YYYY_MM_DD;
import static com.alibaba.hologres.client.model.AutoPartitioning.AutoPartitioningTimeFormat.YYYY_MM_DD_HH24;
import static com.alibaba.hologres.client.model.AutoPartitioning.AutoPartitioningTimeFormat.YYYY_Q;

/** 分区表相关工具类. */
public class PartitionUtil {

    /**
     * 根据时间获取对应分区后缀.
     *
     * @param dateTime 日期时间（带时区信息）
     * @param autoPartitioningInfo 自动分区信息
     * @return 分区后缀
     */
    public static String getPartitionSuffixByDateTime(
            ZonedDateTime dateTime, AutoPartitioning autoPartitioningInfo) {
        return dateTime.format(calculateDateTimeFormatter(autoPartitioningInfo));
    }

    /**
     * 根据分区后缀获取此分区的时间范围, 左闭右开.
     *
     * @param timePart 时间后缀, 比如20240708
     * @param autoPartitioningInfo 自动分区信息
     * @return 分区后缀所属时间范围 [2024-07-08 00:00:00, 2024-07-09 00:00:00), 此分区的结束时间即下一个分区的开始时间
     */
    public static Tuple<ZonedDateTime, ZonedDateTime> getPartitionUnitDateTimeRange(
            String timePart, AutoPartitioning autoPartitioningInfo) {
        DateTimeFormatter dateTimeFormatter = calculateDateTimeFormatter(autoPartitioningInfo);
        LocalDateTime leftDateTime;
        LocalDateTime rightDateTime;
        switch (autoPartitioningInfo.getTimeUnit()) {
            case HOUR:
                leftDateTime = LocalDateTime.parse(timePart, dateTimeFormatter);
                rightDateTime = leftDateTime.plusHours(1);
                break;
            case DAY:
                LocalDate date = LocalDate.parse(timePart, dateTimeFormatter);
                leftDateTime = date.atStartOfDay();
                rightDateTime = leftDateTime.plusDays(1);
                break;
            case MONTH:
                {
                    YearMonth yearMonth = YearMonth.parse(timePart, dateTimeFormatter);
                    leftDateTime = yearMonth.atDay(1).atStartOfDay();
                    rightDateTime = leftDateTime.plusMonths(1);
                    break;
                }
            case QUARTER:
                {
                    int year = Integer.parseInt(timePart.substring(0, 4));
                    int quarter =
                            YYYY_Q.equals(autoPartitioningInfo.getTimeFormat())
                                    ? Integer.parseInt(timePart.substring(5, 6))
                                    : Integer.parseInt(timePart.substring(4, 5));
                    YearMonth yearMonth = YearMonth.of(year, quarter * 3 - 2);
                    leftDateTime = yearMonth.atDay(1).atStartOfDay();
                    rightDateTime = leftDateTime.plusMonths(3);
                    break;
                }
            case YEAR:
                Year year = Year.parse(timePart, dateTimeFormatter);
                leftDateTime = year.atMonth(1).atDay(1).atStartOfDay();
                rightDateTime = leftDateTime.plusYears(1);
                break;
            default:
                throw new IllegalArgumentException("Unsupported TimeUnit");
        }
        return new Tuple<>(
                leftDateTime.atZone(autoPartitioningInfo.getTimeZoneId()),
                rightDateTime.atZone(autoPartitioningInfo.getTimeZoneId()));
    }

    /**
     * 根据父表名和时间获取对应分区名称.
     *
     * @param parentTableName 父表名称
     * @param dateTime 日期时间（带时区信息）
     * @param autoPartitioningInfo 自动分区信息
     * @return 分区名称
     */
    public static String getPartitionNameByDateTime(
            String parentTableName, ZonedDateTime dateTime, AutoPartitioning autoPartitioningInfo) {
        return parentTableName + "_" + getPartitionSuffixByDateTime(dateTime, autoPartitioningInfo);
    }

    /**
     * 获取下一个分区表名称.
     *
     * @param tableName 表名称
     * @param autoPartitioningInfo 自动分区信息
     * @return 下一个分区表名称
     */
    public static String getNextPartitionTableName(
            String tableName, AutoPartitioning autoPartitioningInfo) {
        // example: table_20240708 -> 20240708
        String timePart = extractTimePartFromTableName(tableName, autoPartitioningInfo);
        // 2024-07-09 00:00:00
        ZonedDateTime nextDateTime =
                getPartitionUnitDateTimeRange(timePart, autoPartitioningInfo).r;
        // table_20240708 replace to table_20240709
        return tableName.replaceFirst(
                getPatternForTimeUnit(
                                autoPartitioningInfo.getTimeUnit(),
                                autoPartitioningInfo.getTimeFormat())
                        .pattern(),
                "_" + getPartitionSuffixByDateTime(nextDateTime, autoPartitioningInfo));
    }

    /**
     * 根据表名称提取时间分区后缀.
     *
     * @param tableName 表名称
     * @param autoPartitioningInfo 自动分区信息
     * @return 时间后缀
     */
    public static String extractTimePartFromTableName(
            String tableName, AutoPartitioning autoPartitioningInfo) {
        Matcher matcher =
                getPatternForTimeUnit(
                                autoPartitioningInfo.getTimeUnit(),
                                autoPartitioningInfo.getTimeFormat())
                        .matcher(tableName);
        if (!matcher.find()) {
            throw new RuntimeException(
                    String.format(
                            "The table %s has no suffix matching timeunit %s.",
                            tableName, autoPartitioningInfo.getTimeUnit()));
        }
        return matcher.group(1);
    }

    /**
     * 根据自动分区信息计算日期时间格式化器.
     *
     * @param autoPartitioningInfo 自动分区信息对象
     * @return 返回根据时间单位设置好的日期时间格式化器
     */
    private static DateTimeFormatter calculateDateTimeFormatter(
            AutoPartitioning autoPartitioningInfo) {
        // 通过timeUnit, 计算当前分区子表的后缀
        switch (autoPartitioningInfo.getTimeUnit()) {
            case HOUR:
                return YYYY_MM_DD_HH24.equals(autoPartitioningInfo.getTimeFormat())
                        ? DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
                                .withZone(autoPartitioningInfo.getTimeZoneId())
                        : DateTimeFormatter.ofPattern("yyyyMMddHH")
                                .withZone(autoPartitioningInfo.getTimeZoneId());
            case DAY:
                return YYYY_MM_DD.equals(autoPartitioningInfo.getTimeFormat())
                        ? DateTimeFormatter.ofPattern("yyyy-MM-dd")
                                .withZone(autoPartitioningInfo.getTimeZoneId())
                        : DateTimeFormatter.ofPattern("yyyyMMdd")
                                .withZone(autoPartitioningInfo.getTimeZoneId());
            case MONTH:
                return YYYY_MM.equals(autoPartitioningInfo.getTimeFormat())
                        ? DateTimeFormatter.ofPattern("yyyy-MM")
                                .withZone(autoPartitioningInfo.getTimeZoneId())
                        : DateTimeFormatter.ofPattern("yyyyMM")
                                .withZone(autoPartitioningInfo.getTimeZoneId());
            case QUARTER:
                return YYYY_Q.equals(autoPartitioningInfo.getTimeFormat())
                        ? DateTimeFormatter.ofPattern("yyyy-Q")
                                .withZone(autoPartitioningInfo.getTimeZoneId())
                        : DateTimeFormatter.ofPattern("yyyyQ")
                                .withZone(autoPartitioningInfo.getTimeZoneId());
            case YEAR:
                // 季度, dateTimeFormatter格式为yyyy, 在计算时需要将季度转换为数字
                return DateTimeFormatter.ofPattern("yyyy")
                        .withZone(autoPartitioningInfo.getTimeZoneId());
            default:
                throw new IllegalArgumentException(
                        "Unsupported auto partitioning time unit: "
                                + autoPartitioningInfo.getTimeUnit());
        }
    }

    /**
     * 根据自动分区时间单位获取正则表达式.
     *
     * @param timeUnit 时间单位
     * @return 正则表达式
     */
    private static Pattern getPatternForTimeUnit(
            AutoPartitioning.AutoPartitioningTimeUnit timeUnit,
            AutoPartitioning.AutoPartitioningTimeFormat timeFormat) {
        switch (timeUnit) {
            case HOUR:
                return YYYY_MM_DD_HH24.equals(timeFormat)
                        ? Pattern.compile("_(\\d{4}-\\d{2}-\\d{2}-\\d{2})$")
                        : Pattern.compile("_(\\d{10})$");
            case DAY:
                return YYYY_MM_DD.equals(timeFormat)
                        ? Pattern.compile("_(\\d{4}-\\d{2}-\\d{2})$")
                        : Pattern.compile("_(\\d{8})$");
            case MONTH:
                return YYYY_MM.equals(timeFormat)
                        ? Pattern.compile("_(\\d{4}-\\d{2})$")
                        : Pattern.compile("_(\\d{6})$");
            case QUARTER:
                return YYYY_Q.equals(timeFormat)
                        ? Pattern.compile("_(\\d{4}-\\d{1})$")
                        : Pattern.compile("_(\\d{5})$");
            case YEAR:
                return Pattern.compile("_(\\d{4})$");
            default:
                throw new IllegalArgumentException(
                        "Unsupported auto partitioning time unit: " + timeUnit);
        }
    }

    /**
     * 判断分区表名是否合法.
     *
     * @param tableName 表名称
     * @param autoPartitioningInfo 自动分区信息
     * @return 是否为分区表名称
     */
    public static Boolean isPartitionTableNameLegal(
            String tableName, AutoPartitioning autoPartitioningInfo) {
        try {
            getPartitionUnitDateTimeRange(
                    extractTimePartFromTableName(tableName, autoPartitioningInfo),
                    autoPartitioningInfo);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
