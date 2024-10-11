package com.alibaba.hologres.client.utils;

import com.alibaba.hologres.client.model.AutoPartitioning;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static com.alibaba.hologres.client.utils.PartitionUtil.getNextPartitionTableName;
import static com.alibaba.hologres.client.utils.PartitionUtil.getPartitionSuffixByDateTime;

/**
 * PartitionUtilTest.
 */
public class PartitionUtilTest {

    @Test
    public void testGetPartitionSuffixByDateTime() {
        AutoPartitioning autoPartitioningInfo = new AutoPartitioning();
        autoPartitioningInfo.setTimeZone("Asia/Shanghai");
        {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.HOUR);
            Assert.assertEquals("2023010100", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-01-01T00:00:00+08:00[Asia/Shanghai]"),  autoPartitioningInfo));
            Assert.assertEquals("2023123123", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-12-31T23:59:59+08:00[Asia/Shanghai]"),  autoPartitioningInfo));
            Assert.assertEquals("2023063012", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-06-30T12:12:12+08:00[Asia/Shanghai]"),  autoPartitioningInfo));
            Assert.assertEquals("2020022923", getPartitionSuffixByDateTime(ZonedDateTime.parse("2020-02-29T23:59:59+08:00[Asia/Shanghai]"),  autoPartitioningInfo));
        }
        {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.DAY);
            Assert.assertEquals("20230101", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-01-01T00:00:00+08:00[Asia/Shanghai]"),  autoPartitioningInfo));
            Assert.assertEquals("20231231", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-12-31T23:59:59+08:00[Asia/Shanghai]"),  autoPartitioningInfo));
            Assert.assertEquals("20230630", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-06-30T12:12:12+08:00[Asia/Shanghai]"),  autoPartitioningInfo));
            Assert.assertEquals("20200229", getPartitionSuffixByDateTime(ZonedDateTime.parse("2020-02-29T23:59:59+08:00[Asia/Shanghai]"),  autoPartitioningInfo));
        }
        {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.MONTH);
            Assert.assertEquals("202301", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-01-01T00:00:00+08:00[Asia/Shanghai]"),  autoPartitioningInfo));
            Assert.assertEquals("202312", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-12-31T23:59:59+08:00[Asia/Shanghai]"),  autoPartitioningInfo));
            Assert.assertEquals("202306", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-06-30T12:12:12+08:00[Asia/Shanghai]"),  autoPartitioningInfo));
        }
        {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.QUARTER);
            Assert.assertEquals("20231", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-01-01T00:00:00+08:00[Asia/Shanghai]"),  autoPartitioningInfo));
            Assert.assertEquals("20234", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-12-31T23:59:59+08:00[Asia/Shanghai]"),  autoPartitioningInfo));
            Assert.assertEquals("20232", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-06-30T12:12:12+08:00[Asia/Shanghai]"),  autoPartitioningInfo));
        }
        {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.YEAR);
            Assert.assertEquals("2023", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-01-01T00:00:00+08:00[Asia/Shanghai]"),  autoPartitioningInfo));
            Assert.assertEquals("2023", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-12-31T23:59:59+08:00[Asia/Shanghai]"),  autoPartitioningInfo));
            Assert.assertEquals("2023", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-06-30T12:12:12+08:00[Asia/Shanghai]"),  autoPartitioningInfo));
        }
        // 巴黎时间, 包括包括冬令时+01和夏令时+02
        autoPartitioningInfo.setTimeZone("Europe/Paris");
        {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.HOUR);
            Assert.assertEquals("2023010100", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-01-01T00:00:00+01:00[Europe/Paris]"),  autoPartitioningInfo));
            Assert.assertEquals("2023123123", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-12-31T23:59:59+01:00[Europe/Paris]"),  autoPartitioningInfo));
            Assert.assertEquals("2023063012", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-06-30T12:12:12+02:00[Europe/Paris]"),  autoPartitioningInfo));
            Assert.assertEquals("2023063023", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-06-30T23:59:59+02:00[Europe/Paris]"),  autoPartitioningInfo));
            Assert.assertEquals("2020022923", getPartitionSuffixByDateTime(ZonedDateTime.parse("2020-02-29T23:59:59+01:00[Europe/Paris]"),  autoPartitioningInfo));
        }
        {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.DAY);
            Assert.assertEquals("20230101", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-01-01T00:00:00+01:00[Europe/Paris]"),  autoPartitioningInfo));
            Assert.assertEquals("20231231", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-12-31T23:59:59+01:00[Europe/Paris]"),  autoPartitioningInfo));
            Assert.assertEquals("20230630", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-06-30T23:59:59+02:00[Europe/Paris]"),  autoPartitioningInfo));
            Assert.assertEquals("20230731", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-07-31T23:59:59+02:00[Europe/Paris]"),  autoPartitioningInfo));
            Assert.assertEquals("20200229", getPartitionSuffixByDateTime(ZonedDateTime.parse("2020-02-29T23:59:59+02:00[Europe/Paris]"),  autoPartitioningInfo));
        }
        {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.MONTH);
            Assert.assertEquals("202301", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-01-01T00:00:00+01:00[Europe/Paris]"),  autoPartitioningInfo));
            Assert.assertEquals("202312", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-12-31T23:59:59+02:00[Europe/Paris]"),  autoPartitioningInfo));
            Assert.assertEquals("202306", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-06-30T12:12:12+01:00[Europe/Paris]"),  autoPartitioningInfo));
        }
        {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.QUARTER);
            Assert.assertEquals("20231", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-01-01T00:00:00+01:00[Europe/Paris]"),  autoPartitioningInfo));
            Assert.assertEquals("20234", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-12-31T23:59:59+02:00[Europe/Paris]"),  autoPartitioningInfo));
            Assert.assertEquals("20232", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-06-30T12:12:12+01:00[Europe/Paris]"),  autoPartitioningInfo));
        }
        {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.YEAR);
            Assert.assertEquals("2023", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-01-01T00:00:00+01:00[Europe/Paris]"),  autoPartitioningInfo));
            Assert.assertEquals("2023", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-12-31T23:59:59+02:00[Europe/Paris]"),  autoPartitioningInfo));
            Assert.assertEquals("2023", getPartitionSuffixByDateTime(ZonedDateTime.parse("2023-06-30T12:12:12+01:00[Europe/Paris]"),  autoPartitioningInfo));
        }
    }

    @Test
    public void testGetNextPartitionTableName() {
        AutoPartitioning autoPartitioningInfo = new AutoPartitioning();
        String[] timeZoneList = new String[]{"Asia/Shanghai", "Europe/Paris", "UTC"};
        for (String timeZone : timeZoneList) {
            autoPartitioningInfo.setTimeZone(timeZone);
            {
                autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.HOUR);
                Assert.assertEquals("test_2024010100", getNextPartitionTableName("test_2023123123", autoPartitioningInfo));
                Assert.assertEquals("test_2023010101", getNextPartitionTableName("test_2023010100", autoPartitioningInfo));
                Assert.assertEquals("test_2020030100", getNextPartitionTableName("test_2020022923", autoPartitioningInfo));
                // 只有末尾是分区后缀
                Assert.assertEquals("test_2024070823_2024070900", getNextPartitionTableName("test_2024070823_2024070823", autoPartitioningInfo));
                Assert.assertEquals("TEST_2024070900", getNextPartitionTableName("TEST_2024070823", autoPartitioningInfo));
            }
            {
                autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.DAY);
                Assert.assertEquals("test_20240101", getNextPartitionTableName("test_20231231", autoPartitioningInfo));
                Assert.assertEquals("test_20230102", getNextPartitionTableName("test_20230101", autoPartitioningInfo));
                Assert.assertEquals("test_20200301", getNextPartitionTableName("test_20200229", autoPartitioningInfo));
                Assert.assertEquals("test_20240708_20240709", getNextPartitionTableName("test_20240708_20240708", autoPartitioningInfo));
                Assert.assertEquals("TEST_20240709", getNextPartitionTableName("TEST_20240708", autoPartitioningInfo));
            }
            {
                autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.MONTH);
                Assert.assertEquals("test_202401", getNextPartitionTableName("test_202312", autoPartitioningInfo));
                Assert.assertEquals("test_202302", getNextPartitionTableName("test_202301", autoPartitioningInfo));
                Assert.assertEquals("test_202003", getNextPartitionTableName("test_202002", autoPartitioningInfo));
                Assert.assertEquals("test_202407_202408", getNextPartitionTableName("test_202407_202407", autoPartitioningInfo));
                Assert.assertEquals("TEST_202408", getNextPartitionTableName("TEST_202407", autoPartitioningInfo));
            }
            {
                autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.QUARTER);
                Assert.assertEquals("test_20241", getNextPartitionTableName("test_20234", autoPartitioningInfo));
                Assert.assertEquals("test_20232", getNextPartitionTableName("test_20231", autoPartitioningInfo));
                Assert.assertEquals("test_20241_20242", getNextPartitionTableName("test_20241_20241", autoPartitioningInfo));
                Assert.assertEquals("TEST_20244", getNextPartitionTableName("TEST_20243", autoPartitioningInfo));
            }
            {
                autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.YEAR);
                Assert.assertEquals("test_2024", getNextPartitionTableName("test_2023", autoPartitioningInfo));
                Assert.assertEquals("test_2024_2025", getNextPartitionTableName("test_2024_2024", autoPartitioningInfo));
                Assert.assertEquals("TEST_2025", getNextPartitionTableName("TEST_2024", autoPartitioningInfo));
            }
        }
    }

    @Test
    public void testGetPartitionBinlogEndTimeStamp() {
        AutoPartitioning autoPartitioningInfo = new AutoPartitioning();
        String[] timeZoneList = new String[]{"Asia/Shanghai", "Europe/Paris", "UTC"};
        for (String timeZone : timeZoneList) {
            autoPartitioningInfo.setTimeZone(timeZone);
            {
                // 设置自动分区信息的时间单位为小时
                autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.HOUR);
                Assert.assertEquals(getPartitionUnitDateTimeRange("test_2023123123", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2023-12-31 23:00:00", timeZone), getZonedDateTime("2024-01-01 00:00:00", timeZone)));
                Assert.assertEquals(getPartitionUnitDateTimeRange("test_2023010100", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2023-01-01 00:00:00", timeZone), getZonedDateTime("2023-01-01 01:00:00", timeZone)));
                Assert.assertEquals(getPartitionUnitDateTimeRange("test_2020022923", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2020-02-29 23:00:00", timeZone), getZonedDateTime("2020-03-01 00:00:00", timeZone)));
                Assert.assertEquals(getPartitionUnitDateTimeRange("test_2024070823_2024070823", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2024-07-08 23:00:00", timeZone), getZonedDateTime("2024-07-09 00:00:00", timeZone)));
                Assert.assertEquals(getPartitionUnitDateTimeRange("TEST_2024070823", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2024-07-08 23:00:00", timeZone), getZonedDateTime("2024-07-09 00:00:00", timeZone)));
            }
            {
                autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.DAY);
                Assert.assertEquals(getPartitionUnitDateTimeRange("test_20231231", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2023-12-31 00:00:00", timeZone), getZonedDateTime("2024-01-01 00:00:00", timeZone)));
                Assert.assertEquals(getPartitionUnitDateTimeRange("test_20230101", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2023-01-01 00:00:00", timeZone), getZonedDateTime("2023-01-02 00:00:00", timeZone)));
                Assert.assertEquals(getPartitionUnitDateTimeRange("test_20200229", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2020-02-29 00:00:00", timeZone), getZonedDateTime("2020-03-01 00:00:00", timeZone)));
                Assert.assertEquals(getPartitionUnitDateTimeRange("test_20240708_20240708", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2024-07-08 00:00:00", timeZone), getZonedDateTime("2024-07-09 00:00:00", timeZone)));
                Assert.assertEquals(getPartitionUnitDateTimeRange("TEST_20240708", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2024-07-08 00:00:00", timeZone), getZonedDateTime("2024-07-09 00:00:00", timeZone)));
            }

            {
                autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.MONTH);
                Assert.assertEquals(getPartitionUnitDateTimeRange("test_202312", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2023-12-01 00:00:00", timeZone), getZonedDateTime("2024-01-01 00:00:00", timeZone)));
                Assert.assertEquals(getPartitionUnitDateTimeRange("test_202301", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2023-01-01 00:00:00", timeZone), getZonedDateTime("2023-02-01 00:00:00", timeZone)));
                Assert.assertEquals(getPartitionUnitDateTimeRange("test_202002", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2020-02-01 00:00:00", timeZone), getZonedDateTime("2020-03-01 00:00:00", timeZone)));
                Assert.assertEquals(getPartitionUnitDateTimeRange("test_202407_202407", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2024-07-01 00:00:00", timeZone), getZonedDateTime("2024-08-01 00:00:00", timeZone)));
                Assert.assertEquals(getPartitionUnitDateTimeRange("TEST_202407", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2024-07-01 00:00:00", timeZone), getZonedDateTime("2024-08-01 00:00:00", timeZone)));
            }

            {
                autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.QUARTER);
                Assert.assertEquals(getPartitionUnitDateTimeRange("test_20234", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2023-10-01 00:00:00", timeZone), getZonedDateTime("2024-01-01 00:00:00", timeZone)));
                Assert.assertEquals(getPartitionUnitDateTimeRange("test_20231", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2023-01-01 00:00:00", timeZone), getZonedDateTime("2023-04-01 00:00:00", timeZone)));
                Assert.assertEquals(getPartitionUnitDateTimeRange("test_20241_20241", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2024-01-01 00:00:00", timeZone), getZonedDateTime("2024-04-01 00:00:00", timeZone)));
                Assert.assertEquals(getPartitionUnitDateTimeRange("TEST_20243", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2024-07-01 00:00:00", timeZone), getZonedDateTime("2024-10-01 00:00:00", timeZone)));
            }

            {
                autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.YEAR);
                Assert.assertEquals(getPartitionUnitDateTimeRange("test_2023", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2023-01-01 00:00:00", timeZone), getZonedDateTime("2024-01-01 00:00:00", timeZone)));
                Assert.assertEquals(getPartitionUnitDateTimeRange("test_2024_2024", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2024-01-01 00:00:00", timeZone), getZonedDateTime("2025-01-01 00:00:00", timeZone)));
                Assert.assertEquals(getPartitionUnitDateTimeRange("TEST_2024", autoPartitioningInfo),
                        new Tuple<>(getZonedDateTime("2024-01-01 00:00:00", timeZone), getZonedDateTime("2025-01-01 00:00:00", timeZone)));
                try {
                    // 时间后缀必须以_开始
                    getPartitionUnitDateTimeRange("TEST_202024", autoPartitioningInfo);
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage().contains("The table TEST_202024 has no suffix matching timeunit YEAR"));
                }
            }
        }
    }

    @Test
    void testIsPartitionTableNameLegal() {
        AutoPartitioning autoPartitioningInfo = new AutoPartitioning();
        autoPartitioningInfo.setTimeZone("Asia/Shanghai");
        {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.HOUR);
            Assert.assertTrue(PartitionUtil.isPartitionTableNameLegal("test_2023010812", autoPartitioningInfo));
            // invalid hour 25
            Assert.assertFalse(PartitionUtil.isPartitionTableNameLegal("test_2023010825", autoPartitioningInfo));
            // invalid month 13
            Assert.assertFalse(PartitionUtil.isPartitionTableNameLegal("test_2023130812", autoPartitioningInfo));
            // invalid time suffix
            Assert.assertFalse(PartitionUtil.isPartitionTableNameLegal("test_202023150812", autoPartitioningInfo));
            // time suffix dose not match the time unit hour
            Assert.assertFalse(PartitionUtil.isPartitionTableNameLegal("test_2023", autoPartitioningInfo));
        }
        {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.DAY);
            Assert.assertTrue(PartitionUtil.isPartitionTableNameLegal("test_20230108", autoPartitioningInfo));
            Assert.assertFalse(PartitionUtil.isPartitionTableNameLegal("test_20230008", autoPartitioningInfo));
            Assert.assertFalse(PartitionUtil.isPartitionTableNameLegal("test_20230132", autoPartitioningInfo));
            Assert.assertFalse(PartitionUtil.isPartitionTableNameLegal("test_2023", autoPartitioningInfo));
        }
        {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.MONTH);
            Assert.assertTrue(PartitionUtil.isPartitionTableNameLegal("test_202301", autoPartitioningInfo));
            Assert.assertFalse(PartitionUtil.isPartitionTableNameLegal("test_202315", autoPartitioningInfo));
            Assert.assertFalse(PartitionUtil.isPartitionTableNameLegal("test_2023", autoPartitioningInfo));
        }
        {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.QUARTER);
            Assert.assertTrue(PartitionUtil.isPartitionTableNameLegal("test_20234", autoPartitioningInfo));
            Assert.assertFalse(PartitionUtil.isPartitionTableNameLegal("test_20235", autoPartitioningInfo));
            Assert.assertFalse(PartitionUtil.isPartitionTableNameLegal("test_2023", autoPartitioningInfo));
        }
        {
            autoPartitioningInfo.setTimeUnit(AutoPartitioning.AutoPartitioningTimeUnit.YEAR);
            Assert.assertTrue(PartitionUtil.isPartitionTableNameLegal("test_2023", autoPartitioningInfo));
            Assert.assertFalse(PartitionUtil.isPartitionTableNameLegal("test_202305", autoPartitioningInfo));
        }
    }

    private ZonedDateTime getZonedDateTime(String time, String timeZone) {
        return ZonedDateTime.parse(time, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of(timeZone)));
    }

    private Tuple<ZonedDateTime, ZonedDateTime> getPartitionUnitDateTimeRange(String tableName, AutoPartitioning autoPartitioningInfo) {
        String suffix = PartitionUtil.extractTimePartFromTableName(tableName, autoPartitioningInfo);
        return PartitionUtil.getPartitionUnitDateTimeRange(suffix, autoPartitioningInfo);
    }
}
