package com.alibaba.hologres.client.utils;

import com.alibaba.hologres.client.model.AutoPartitioning;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static com.alibaba.hologres.client.utils.PartitionUtil.getNextPartitionTableName;
import static com.alibaba.hologres.client.utils.PartitionUtil.getPartitionSuffixByDateTime;

/** PartitionUtilTest. */
public class PartitionUtilTest {

    @Test
    public void testGetPartitionSuffixByDateTime() {
        AutoPartitioning autoPartitioningInfo = new AutoPartitioning();
        autoPartitioningInfo.setTimeZone("Asia/Shanghai");
        for (String timeFormat : new String[] {null, "YYYYMMDDHH24", "YYYY-MM-DD-HH24"}) {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.HOUR);
            autoPartitioningInfo.setTimeFormat(timeFormat);
            boolean hyphen = "YYYY-MM-DD-HH24".equals(timeFormat);
            Assert.assertEquals(
                    isNeedHyphen("2023-01-01-00", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-01-01T00:00:00+08:00[Asia/Shanghai]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2023-12-31-23", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-12-31T23:59:59+08:00[Asia/Shanghai]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2023-06-30-12", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-06-30T12:12:12+08:00[Asia/Shanghai]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2020-02-29-23", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2020-02-29T23:59:59+08:00[Asia/Shanghai]"),
                            autoPartitioningInfo));
        }
        for (String timeFormat : new String[] {null, "YYYYMMDD", "YYYY-MM-DD"}) {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.DAY);
            autoPartitioningInfo.setTimeFormat(timeFormat);
            boolean hyphen = "YYYY-MM-DD".equals(timeFormat);
            Assert.assertEquals(
                    isNeedHyphen("2023-01-01", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-01-01T00:00:00+08:00[Asia/Shanghai]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2023-12-31", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-12-31T23:59:59+08:00[Asia/Shanghai]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2023-06-30", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-06-30T12:12:12+08:00[Asia/Shanghai]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2020-02-29", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2020-02-29T23:59:59+08:00[Asia/Shanghai]"),
                            autoPartitioningInfo));
        }
        for (String timeFormat : new String[] {null, "YYYYMM", "YYYY-MM"}) {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.MONTH);
            autoPartitioningInfo.setTimeFormat(timeFormat);
            boolean hyphen = "YYYY-MM".equals(timeFormat);
            Assert.assertEquals(
                    isNeedHyphen("2023-01", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-01-01T00:00:00+08:00[Asia/Shanghai]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2023-12", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-12-31T23:59:59+08:00[Asia/Shanghai]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2023-06", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-06-30T12:12:12+08:00[Asia/Shanghai]"),
                            autoPartitioningInfo));
        }
        for (String timeFormat : new String[] {null, "YYYYQ", "YYYY-Q"}) {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.QUARTER);
            autoPartitioningInfo.setTimeFormat(timeFormat);
            boolean hyphen = "YYYY-Q".equals(timeFormat);
            Assert.assertEquals(
                    isNeedHyphen("2023-1", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-01-01T00:00:00+08:00[Asia/Shanghai]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2023-4", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-12-31T23:59:59+08:00[Asia/Shanghai]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2023-2", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-06-30T12:12:12+08:00[Asia/Shanghai]"),
                            autoPartitioningInfo));
        }
        for (String timeFormat : new String[] {null, "YYYY"}) {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.YEAR);
            Assert.assertEquals(
                    "2023",
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-01-01T00:00:00+08:00[Asia/Shanghai]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    "2023",
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-12-31T23:59:59+08:00[Asia/Shanghai]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    "2023",
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-06-30T12:12:12+08:00[Asia/Shanghai]"),
                            autoPartitioningInfo));
        }
        // 巴黎时间, 包括包括冬令时+01和夏令时+02
        autoPartitioningInfo.setTimeZone("Europe/Paris");
        for (String timeFormat : new String[] {null, "YYYYMMDDHH24", "YYYY-MM-DD-HH24"}) {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.HOUR);
            autoPartitioningInfo.setTimeFormat(timeFormat);
            boolean hyphen = "YYYY-MM-DD-HH24".equals(timeFormat);
            Assert.assertEquals(
                    isNeedHyphen("2023-01-01-00", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-01-01T00:00:00+01:00[Europe/Paris]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2023-12-31-23", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-12-31T23:59:59+01:00[Europe/Paris]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2023-06-30-12", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-06-30T12:12:12+02:00[Europe/Paris]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2023-06-30-23", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-06-30T23:59:59+02:00[Europe/Paris]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2020-02-29-23", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2020-02-29T23:59:59+01:00[Europe/Paris]"),
                            autoPartitioningInfo));
        }
        for (String timeFormat : new String[] {null, "YYYYMMDD", "YYYY-MM-DD"}) {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.DAY);
            autoPartitioningInfo.setTimeFormat(timeFormat);
            boolean hyphen = "YYYY-MM-DD".equals(timeFormat);
            Assert.assertEquals(
                    isNeedHyphen("2023-01-01", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-01-01T00:00:00+01:00[Europe/Paris]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2023-12-31", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-12-31T23:59:59+01:00[Europe/Paris]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2023-06-30", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-06-30T23:59:59+02:00[Europe/Paris]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2023-07-31", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-07-31T23:59:59+02:00[Europe/Paris]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2020-02-29", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2020-02-29T23:59:59+02:00[Europe/Paris]"),
                            autoPartitioningInfo));
        }
        for (String timeFormat : new String[] {null, "YYYYMM", "YYYY-MM"}) {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.MONTH);
            autoPartitioningInfo.setTimeFormat(timeFormat);
            boolean hyphen = "YYYY-MM".equals(timeFormat);
            Assert.assertEquals(
                    isNeedHyphen("2023-01", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-01-01T00:00:00+01:00[Europe/Paris]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2023-12", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-12-31T23:59:59+02:00[Europe/Paris]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2023-06", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-06-30T12:12:12+01:00[Europe/Paris]"),
                            autoPartitioningInfo));
        }
        for (String timeFormat : new String[] {null, "YYYYQ", "YYYY-Q"}) {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.QUARTER);
            autoPartitioningInfo.setTimeFormat(timeFormat);
            boolean hyphen = "YYYY-Q".equals(timeFormat);
            Assert.assertEquals(
                    isNeedHyphen("2023-1", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-01-01T00:00:00+01:00[Europe/Paris]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2023-4", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-12-31T23:59:59+02:00[Europe/Paris]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    isNeedHyphen("2023-2", hyphen),
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-06-30T12:12:12+01:00[Europe/Paris]"),
                            autoPartitioningInfo));
        }
        {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.YEAR);
            Assert.assertEquals(
                    "2023",
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-01-01T00:00:00+01:00[Europe/Paris]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    "2023",
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-12-31T23:59:59+02:00[Europe/Paris]"),
                            autoPartitioningInfo));
            Assert.assertEquals(
                    "2023",
                    getPartitionSuffixByDateTime(
                            ZonedDateTime.parse("2023-06-30T12:12:12+01:00[Europe/Paris]"),
                            autoPartitioningInfo));
        }
    }

    @Test
    public void testGetNextPartitionTableName() {
        AutoPartitioning autoPartitioningInfo = new AutoPartitioning();
        String[] timeZoneList = new String[] {"Asia/Shanghai", "Europe/Paris", "UTC"};
        for (String timeZone : timeZoneList) {
            autoPartitioningInfo.setTimeZone(timeZone);
            for (String timeFormat : new String[] {null, "YYYYMMDDHH24", "YYYY-MM-DD-HH24"}) {
                autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.HOUR);
                autoPartitioningInfo.setTimeFormat(timeFormat);
                boolean hyphen = "YYYY-MM-DD-HH24".equals(timeFormat);
                Assert.assertEquals(
                        isNeedHyphen("test_2024-01-01-00", hyphen),
                        getNextPartitionTableName(
                                isNeedHyphen("test_2023-12-31-23", hyphen), autoPartitioningInfo));
                Assert.assertEquals(
                        isNeedHyphen("test_2023-01-01-01", hyphen),
                        getNextPartitionTableName(
                                isNeedHyphen("test_2023-01-01-00", hyphen), autoPartitioningInfo));
                Assert.assertEquals(
                        isNeedHyphen("test_2020-03-01-00", hyphen),
                        getNextPartitionTableName(
                                isNeedHyphen("test_2020-02-29-23", hyphen), autoPartitioningInfo));
                // 只有末尾是分区后缀
                Assert.assertEquals(
                        isNeedHyphen("test_2024-07-08-23_test_2024-07-09-00", hyphen),
                        getNextPartitionTableName(
                                isNeedHyphen("test_2024-07-08-23_test_2024-07-08-23", hyphen),
                                autoPartitioningInfo));
                Assert.assertEquals(
                        isNeedHyphen("TEST_2024-07-09-00", hyphen),
                        getNextPartitionTableName(
                                isNeedHyphen("TEST_2024-07-08-23", hyphen), autoPartitioningInfo));
            }
            for (String timeFormat : new String[] {null, "YYYYMMDD", "YYYY-MM-DD"}) {
                autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.DAY);
                autoPartitioningInfo.setTimeFormat(timeFormat);
                boolean hyphen = "YYYY-MM-DD".equals(timeFormat);
                Assert.assertEquals(
                        isNeedHyphen("test_2024-01-01", hyphen),
                        getNextPartitionTableName(
                                isNeedHyphen("test_2023-12-31", hyphen), autoPartitioningInfo));
                Assert.assertEquals(
                        isNeedHyphen("test_2023-01-02", hyphen),
                        getNextPartitionTableName(
                                isNeedHyphen("test_2023-01-01", hyphen), autoPartitioningInfo));
                Assert.assertEquals(
                        isNeedHyphen("test_2020-03-01", hyphen),
                        getNextPartitionTableName(
                                isNeedHyphen("test_2020-02-29", hyphen), autoPartitioningInfo));
                Assert.assertEquals(
                        isNeedHyphen("test_2024-07-08_test_2024-07-09", hyphen),
                        getNextPartitionTableName(
                                isNeedHyphen("test_2024-07-08_test_2024-07-08", hyphen),
                                autoPartitioningInfo));
                Assert.assertEquals(
                        isNeedHyphen("TEST_2024-07-09", hyphen),
                        getNextPartitionTableName(
                                isNeedHyphen("TEST_2024-07-08", hyphen), autoPartitioningInfo));
            }
            for (String timeFormat : new String[] {null, "YYYYMM", "YYYY-MM"}) {
                autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.MONTH);
                autoPartitioningInfo.setTimeFormat(timeFormat);
                boolean hyphen = "YYYY-MM".equals(timeFormat);
                Assert.assertEquals(
                        isNeedHyphen("test_2024-01", hyphen),
                        getNextPartitionTableName(
                                isNeedHyphen("test_2023-12", hyphen), autoPartitioningInfo));
                Assert.assertEquals(
                        isNeedHyphen("test_2023-02", hyphen),
                        getNextPartitionTableName(
                                isNeedHyphen("test_2023-01", hyphen), autoPartitioningInfo));
                Assert.assertEquals(
                        isNeedHyphen("test_2020-03", hyphen),
                        getNextPartitionTableName(
                                isNeedHyphen("test_2020-02", hyphen), autoPartitioningInfo));
                Assert.assertEquals(
                        isNeedHyphen("test_2024-07_test_2024-08", hyphen),
                        getNextPartitionTableName(
                                isNeedHyphen("test_2024-07_test_2024-07", hyphen),
                                autoPartitioningInfo));
                Assert.assertEquals(
                        isNeedHyphen("TEST_2024-08", hyphen),
                        getNextPartitionTableName(
                                isNeedHyphen("TEST_2024-07", hyphen), autoPartitioningInfo));
            }
            for (String timeFormat : new String[] {null, "YYYYQ", "YYYY-Q"}) {
                autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.QUARTER);
                autoPartitioningInfo.setTimeFormat(timeFormat);
                boolean hyphen = "YYYY-Q".equals(timeFormat);
                Assert.assertEquals(
                        isNeedHyphen("test_2024-1", hyphen),
                        getNextPartitionTableName(
                                isNeedHyphen("test_2023-4", hyphen), autoPartitioningInfo));
                Assert.assertEquals(
                        isNeedHyphen("test_2023-2", hyphen),
                        getNextPartitionTableName(
                                isNeedHyphen("test_2023-1", hyphen), autoPartitioningInfo));
                Assert.assertEquals(
                        isNeedHyphen("test_2024-1_2024-2", hyphen),
                        getNextPartitionTableName(
                                isNeedHyphen("test_2024-1_2024-1", hyphen), autoPartitioningInfo));
                Assert.assertEquals(
                        isNeedHyphen("TEST_2024-4", hyphen),
                        getNextPartitionTableName(
                                isNeedHyphen("TEST_2024-3", hyphen), autoPartitioningInfo));
            }
            {
                autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.YEAR);
                Assert.assertEquals(
                        "test_2024", getNextPartitionTableName("test_2023", autoPartitioningInfo));
                Assert.assertEquals(
                        "test_2024_2025",
                        getNextPartitionTableName("test_2024_2024", autoPartitioningInfo));
                Assert.assertEquals(
                        "TEST_2025", getNextPartitionTableName("TEST_2024", autoPartitioningInfo));
            }
        }
    }

    @Test
    public void testGetPartitionBinlogEndTimeStamp() {
        AutoPartitioning autoPartitioningInfo = new AutoPartitioning();
        String[] timeZoneList = new String[] {"Asia/Shanghai", "Europe/Paris", "UTC"};
        for (String timeZone : timeZoneList) {
            autoPartitioningInfo.setTimeZone(timeZone);
            for (String timeFormat : new String[] {null, "YYYYMMDDHH24", "YYYY-MM-DD-HH24"}) {
                // 设置自动分区信息的时间单位为小时
                autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.HOUR);
                autoPartitioningInfo.setTimeFormat(timeFormat);
                boolean hyphen = "YYYY-MM-DD-HH24".equals(timeFormat);
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange(
                                isNeedHyphen("test_2023-12-31-23", hyphen), autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2023-12-31 23:00:00", timeZone),
                                getZonedDateTime("2024-01-01 00:00:00", timeZone)));
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange(
                                isNeedHyphen("test_2023-01-01-00", hyphen), autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2023-01-01 00:00:00", timeZone),
                                getZonedDateTime("2023-01-01 01:00:00", timeZone)));
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange(
                                isNeedHyphen("test_2020-02-29-23", hyphen), autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2020-02-29 23:00:00", timeZone),
                                getZonedDateTime("2020-03-01 00:00:00", timeZone)));
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange(
                                isNeedHyphen("test_2024-07-08-23_2024-07-08-23", hyphen),
                                autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2024-07-08 23:00:00", timeZone),
                                getZonedDateTime("2024-07-09 00:00:00", timeZone)));
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange(
                                isNeedHyphen("TEST_2024-07-08-23", hyphen), autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2024-07-08 23:00:00", timeZone),
                                getZonedDateTime("2024-07-09 00:00:00", timeZone)));
            }
            for (String timeFormat : new String[] {null, "YYYYMMDD", "YYYY-MM-DD"}) {
                autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.DAY);
                autoPartitioningInfo.setTimeFormat(timeFormat);
                boolean hyphen = "YYYY-MM-DD".equals(timeFormat);
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange(
                                isNeedHyphen("test_2023-12-31", hyphen), autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2023-12-31 00:00:00", timeZone),
                                getZonedDateTime("2024-01-01 00:00:00", timeZone)));
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange(
                                isNeedHyphen("test_2023-01-01", hyphen), autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2023-01-01 00:00:00", timeZone),
                                getZonedDateTime("2023-01-02 00:00:00", timeZone)));
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange(
                                isNeedHyphen("test_2020-02-29", hyphen), autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2020-02-29 00:00:00", timeZone),
                                getZonedDateTime("2020-03-01 00:00:00", timeZone)));
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange(
                                isNeedHyphen("test_2024-07-08_2024-07-08", hyphen),
                                autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2024-07-08 00:00:00", timeZone),
                                getZonedDateTime("2024-07-09 00:00:00", timeZone)));
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange(
                                isNeedHyphen("TEST_2024-07-08", hyphen), autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2024-07-08 00:00:00", timeZone),
                                getZonedDateTime("2024-07-09 00:00:00", timeZone)));
            }
            for (String timeFormat : new String[] {null, "YYYYMM", "YYYY-MM"}) {
                autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.MONTH);
                autoPartitioningInfo.setTimeFormat(timeFormat);
                boolean hyphen = "YYYY-MM".equals(timeFormat);
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange(
                                isNeedHyphen("test_2023-12", hyphen), autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2023-12-01 00:00:00", timeZone),
                                getZonedDateTime("2024-01-01 00:00:00", timeZone)));
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange(
                                isNeedHyphen("test_2023-01", hyphen), autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2023-01-01 00:00:00", timeZone),
                                getZonedDateTime("2023-02-01 00:00:00", timeZone)));
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange(
                                isNeedHyphen("test_2020-02", hyphen), autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2020-02-01 00:00:00", timeZone),
                                getZonedDateTime("2020-03-01 00:00:00", timeZone)));
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange(
                                isNeedHyphen("test_2024-07_2024-07", hyphen), autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2024-07-01 00:00:00", timeZone),
                                getZonedDateTime("2024-08-01 00:00:00", timeZone)));
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange(
                                isNeedHyphen("TEST_2024-07", hyphen), autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2024-07-01 00:00:00", timeZone),
                                getZonedDateTime("2024-08-01 00:00:00", timeZone)));
            }

            for (String timeFormat : new String[] {null, "YYYYQ", "YYYY-Q"}) {
                autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.QUARTER);
                autoPartitioningInfo.setTimeFormat(timeFormat);
                boolean hyphen = "YYYY-Q".equals(timeFormat);
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange(
                                isNeedHyphen("test_2023-4", hyphen), autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2023-10-01 00:00:00", timeZone),
                                getZonedDateTime("2024-01-01 00:00:00", timeZone)));
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange(
                                isNeedHyphen("test_2023-1", hyphen), autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2023-01-01 00:00:00", timeZone),
                                getZonedDateTime("2023-04-01 00:00:00", timeZone)));
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange(
                                isNeedHyphen("test_2024-1_2024-1", hyphen), autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2024-01-01 00:00:00", timeZone),
                                getZonedDateTime("2024-04-01 00:00:00", timeZone)));
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange(
                                isNeedHyphen("TEST_2024-3", hyphen), autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2024-07-01 00:00:00", timeZone),
                                getZonedDateTime("2024-10-01 00:00:00", timeZone)));
            }

            {
                autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.YEAR);
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange("test_2023", autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2023-01-01 00:00:00", timeZone),
                                getZonedDateTime("2024-01-01 00:00:00", timeZone)));
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange("test_2024_2024", autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2024-01-01 00:00:00", timeZone),
                                getZonedDateTime("2025-01-01 00:00:00", timeZone)));
                Assert.assertEquals(
                        getPartitionUnitDateTimeRange("TEST_2024", autoPartitioningInfo),
                        new Tuple<>(
                                getZonedDateTime("2024-01-01 00:00:00", timeZone),
                                getZonedDateTime("2025-01-01 00:00:00", timeZone)));
                try {
                    // 时间后缀必须以_开始
                    getPartitionUnitDateTimeRange("TEST_202024", autoPartitioningInfo);
                } catch (Exception e) {
                    Assert.assertTrue(
                            e.getMessage()
                                    .contains(
                                            "The table TEST_202024 has no suffix matching timeunit YEAR"));
                }
            }
        }
    }

    @Test
    void testIsPartitionTableNameLegal() {
        AutoPartitioning autoPartitioningInfo = new AutoPartitioning();
        autoPartitioningInfo.setTimeZone("Asia/Shanghai");
        for (String timeFormat : new String[] {null, "YYYYMMDDHH24", "YYYY-MM-DD-HH24"}) {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.HOUR);
            autoPartitioningInfo.setTimeFormat(timeFormat);
            boolean hyphen = "YYYY-MM-DD-HH24".equals(timeFormat);
            Assert.assertTrue(
                    PartitionUtil.isPartitionTableNameLegal(
                            isNeedHyphen("test_2023-01-08-12", hyphen), autoPartitioningInfo));
            // invalid hour 25
            Assert.assertFalse(
                    PartitionUtil.isPartitionTableNameLegal(
                            isNeedHyphen("test_2023010825", hyphen), autoPartitioningInfo));
            // invalid month 13
            Assert.assertFalse(
                    PartitionUtil.isPartitionTableNameLegal(
                            isNeedHyphen("test_2023130812", hyphen), autoPartitioningInfo));
            // invalid time suffix
            Assert.assertFalse(
                    PartitionUtil.isPartitionTableNameLegal(
                            isNeedHyphen("test_202023150812", hyphen), autoPartitioningInfo));
            // time suffix dose not match the time unit hour
            Assert.assertFalse(
                    PartitionUtil.isPartitionTableNameLegal("test_2023", autoPartitioningInfo));
        }
        for (String timeFormat : new String[] {null, "YYYYMMDD", "YYYY-MM-DD"}) {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.DAY);
            autoPartitioningInfo.setTimeFormat(timeFormat);
            boolean hyphen = "YYYY-MM-DD".equals(timeFormat);
            Assert.assertTrue(
                    PartitionUtil.isPartitionTableNameLegal(
                            isNeedHyphen("test_2023-01-08", hyphen), autoPartitioningInfo));
            Assert.assertFalse(
                    PartitionUtil.isPartitionTableNameLegal(
                            isNeedHyphen("test_2023-00-08", hyphen), autoPartitioningInfo));
            Assert.assertFalse(
                    PartitionUtil.isPartitionTableNameLegal(
                            isNeedHyphen("test_2023-01-32", hyphen), autoPartitioningInfo));
            Assert.assertFalse(
                    PartitionUtil.isPartitionTableNameLegal(
                            isNeedHyphen("test_2023", hyphen), autoPartitioningInfo));
        }
        for (String timeFormat : new String[] {null, "YYYYMM", "YYYY-MM"}) {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.MONTH);
            autoPartitioningInfo.setTimeFormat(timeFormat);
            boolean hyphen = "YYYY-MM".equals(timeFormat);
            Assert.assertTrue(
                    PartitionUtil.isPartitionTableNameLegal(
                            isNeedHyphen("test_2023-01", hyphen), autoPartitioningInfo));
            Assert.assertFalse(
                    PartitionUtil.isPartitionTableNameLegal(
                            isNeedHyphen("test_2023-15", hyphen), autoPartitioningInfo));
            Assert.assertFalse(
                    PartitionUtil.isPartitionTableNameLegal(
                            isNeedHyphen("test_2023", hyphen), autoPartitioningInfo));
        }
        for (String timeFormat : new String[] {null, "YYYYQ", "YYYY-Q"}) {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.QUARTER);
            autoPartitioningInfo.setTimeFormat(timeFormat);
            boolean hyphen = "YYYY-Q".equals(timeFormat);
            Assert.assertTrue(
                    PartitionUtil.isPartitionTableNameLegal(
                            isNeedHyphen("test_2023-4", hyphen), autoPartitioningInfo));
            Assert.assertFalse(
                    PartitionUtil.isPartitionTableNameLegal(
                            isNeedHyphen("test_2023-5", hyphen), autoPartitioningInfo));
            Assert.assertFalse(
                    PartitionUtil.isPartitionTableNameLegal(
                            isNeedHyphen("test_2023", hyphen), autoPartitioningInfo));
        }
        {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.YEAR);
            Assert.assertTrue(
                    PartitionUtil.isPartitionTableNameLegal("test_2023", autoPartitioningInfo));
            Assert.assertFalse(
                    PartitionUtil.isPartitionTableNameLegal("test_202305", autoPartitioningInfo));
        }
    }

    private ZonedDateTime getZonedDateTime(String time, String timeZone) {
        return ZonedDateTime.parse(
                time,
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of(timeZone)));
    }

    private Tuple<ZonedDateTime, ZonedDateTime> getPartitionUnitDateTimeRange(
            String tableName, AutoPartitioning autoPartitioningInfo) {
        String suffix = PartitionUtil.extractTimePartFromTableName(tableName, autoPartitioningInfo);
        return PartitionUtil.getPartitionUnitDateTimeRange(suffix, autoPartitioningInfo);
    }

    private String isNeedHyphen(String suffix, boolean hyphen) {
        return hyphen ? suffix : suffix.replace("-", "");
    }
}
