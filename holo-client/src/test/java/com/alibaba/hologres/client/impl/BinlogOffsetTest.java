package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.impl.binlog.BinlogOffset;
import org.testng.annotations.Test;

import java.time.ZoneOffset;
import java.util.TimeZone;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class BinlogOffsetTest {

    @Test
    void testSetTimestampWithMicroSeconds() {
        BinlogOffset binlogOffset = new BinlogOffset();
        binlogOffset.setTimestamp(1639285200135789L);
        assertEquals(binlogOffset.getTimestamp(), 1639285200135789L);
        assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 13:00:00.135789+08:00");
    }

    @Test(groups = "nonConcurrentGroup")
    void testSetTimestampWithMicroSecondsDifferentTimeZone() {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        try {
            for (int i = -8; i <= 10; i++) {
                TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.ofHours(i)));
                BinlogOffset binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp(1639285200135789L);
                assertEquals(binlogOffset.getTimestamp(), 1639285200135789L);
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 13:00:00.135789+08:00");
            }
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    @Test
    void testSetTimestampWithString() {
        // ISO_ZONED_DATE_TIME, ZonedDateTime toString的字符串可以通过此format解析
        BinlogOffset binlogOffset = new BinlogOffset();
        binlogOffset.setTimestamp("2021-12-12T13:00:00.135789+08:00[Asia/Shanghai]");
        assertEquals(binlogOffset.getTimestamp(), 1639285200135789L);
        assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 13:00:00.135789+08:00");

        binlogOffset = new BinlogOffset();
        binlogOffset.setTimestamp("2021-12-12T13:00:00.135+08:00[Asia/Shanghai]");
        assertEquals(binlogOffset.getTimestamp(), 1639285200135000L);
        assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 13:00:00.135+08:00");

        // ISO_LOCAL_DATE_TIME, LocalDateTime toString的字符串可以通过此format解析
        binlogOffset = new BinlogOffset();
        binlogOffset.setTimestamp("2021-12-12T13:00:00.135789");
        assertEquals(binlogOffset.getTimestamp(), 1639285200135789L);
        assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 13:00:00.135789+08:00");

        binlogOffset = new BinlogOffset();
        binlogOffset.setTimestamp("2021-12-12T13:00:00.135");
        assertEquals(binlogOffset.getTimestamp(), 1639285200135000L);
        assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 13:00:00.135+08:00");

        // SQL_ZONED_DATE_TIME, postgres timestamptz 的字符串可以通过此format解析
        binlogOffset = new BinlogOffset();
        binlogOffset.setTimestamp("2021-12-12 13:00:00.135789+08");
        assertEquals(binlogOffset.getTimestamp(), 1639285200135789L);
        assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 13:00:00.135789+08:00");

        binlogOffset = new BinlogOffset();
        binlogOffset.setTimestamp("2021-12-12 13:00:00.135+08");
        assertEquals(binlogOffset.getTimestamp(), 1639285200135000L);
        assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 13:00:00.135+08:00");

        // SQL_LOCAL_DATE_TIME, postgres timestamp 的字符串可以通过此format解析
        binlogOffset = new BinlogOffset();
        binlogOffset.setTimestamp("2021-12-12 13:00:00.135789");
        assertEquals(binlogOffset.getTimestamp(), 1639285200135789L);
        assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 13:00:00.135789+08:00");

        binlogOffset = new BinlogOffset();
        binlogOffset.setTimestamp("2021-12-12 13:00:00.135");
        assertEquals(binlogOffset.getTimestamp(), 1639285200135000L);
        assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 13:00:00.135+08:00");
    }

    @Test(groups = "nonConcurrentGroup")
    void testSetTimestampZonedStrDifferentTimeZone() {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        try {
            for (int i = -8; i <= 10; i++) {
                TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.ofHours(i)));
                // ISO_ZONED_DATE_TIME, ZonedDateTime toString的字符串可以通过此format解析
                BinlogOffset binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12T13:00:00.135789+08:00[Asia/Shanghai]");
                assertEquals(binlogOffset.getTimestamp(), 1639285200135789L);
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 13:00:00.135789+08:00");

                binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12T13:00:00.135+08:00[Asia/Shanghai]");
                assertEquals(binlogOffset.getTimestamp(), 1639285200135000L);
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 13:00:00.135+08:00");

                // SQL_ZONED_DATE_TIME, postgres timestamptz 的字符串可以通过此format解析
                binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12 13:00:00.135789+08");
                assertEquals(binlogOffset.getTimestamp(), 1639285200135789L);
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 13:00:00.135789+08:00");

                binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12 13:00:00.135+08");
                assertEquals(binlogOffset.getTimestamp(), 1639285200135000L);
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 13:00:00.135+08:00");
            }
            {
                // 夏令时, 冬季 UTC+1, 夏季 UTC+2
                TimeZone.setDefault(TimeZone.getTimeZone("Europe/Berlin"));
                // ISO_ZONED_DATE_TIME, ZonedDateTime toString的字符串可以通过此format解析
                BinlogOffset binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12T13:00:00.135789+08:00[Asia/Shanghai]");
                assertEquals(binlogOffset.getTimestamp(), 1639285200135789L);
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 13:00:00.135789+08:00");

                // SQL_ZONED_DATE_TIME, postgres timestamptz 的字符串可以通过此format解析
                binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12 13:00:00.135789+08");
                assertEquals(binlogOffset.getTimestamp(), 1639285200135789L);
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 13:00:00.135789+08:00");

                // ISO_ZONED_DATE_TIME, ZonedDateTime toString的字符串可以通过此format解析
                binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-06-12T13:00:00.135789+08:00[Asia/Shanghai]");
                assertEquals(binlogOffset.getTimestamp(), 1623474000135789L);
                assertEquals(binlogOffset.getStartTimeText(), "2021-06-12 13:00:00.135789+08:00");

                // SQL_ZONED_DATE_TIME, postgres timestamptz 的字符串可以通过此format解析
                binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-06-12 13:00:00.135789+08");
                assertEquals(binlogOffset.getTimestamp(), 1623474000135789L);
                assertEquals(binlogOffset.getStartTimeText(), "2021-06-12 13:00:00.135789+08:00");
            }
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    @Test(groups = "nonConcurrentGroup")
    void testSetTimestampLocalStrDifferentTimeZone() {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        try {
            {
                TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.ofHours(-8)));

                // ISO_LOCAL_DATE_TIME, LocalDateTime toString的字符串可以通过此format解析
                BinlogOffset binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12T13:00:00.135789");
                assertEquals(
                        binlogOffset.getTimestamp(),
                        1639285200135789L + (16 * 3600 * 1000 * 1000L));
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-13 05:00:00.135789+08:00");

                // SQL_LOCAL_DATE_TIME, postgres timestamp 的字符串可以通过此format解析
                binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12 13:00:00.135789");
                assertEquals(
                        binlogOffset.getTimestamp(),
                        1639285200135789L + (16 * 3600 * 1000 * 1000L));
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-13 05:00:00.135789+08:00");
            }
            {
                TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.ofHours(-5)));

                // ISO_LOCAL_DATE_TIME, LocalDateTime toString的字符串可以通过此format解析
                BinlogOffset binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12T13:00:00.135789");
                assertEquals(
                        binlogOffset.getTimestamp(),
                        1639285200135789L + (13 * 3600 * 1000 * 1000L));
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-13 02:00:00.135789+08:00");

                // SQL_LOCAL_DATE_TIME, postgres timestamp 的字符串可以通过此format解析
                binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12 13:00:00.135789");
                assertEquals(
                        binlogOffset.getTimestamp(),
                        1639285200135789L + (13 * 3600 * 1000 * 1000L));
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-13 02:00:00.135789+08:00");
            }
            {
                TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.ofHours(-2)));

                // ISO_LOCAL_DATE_TIME, LocalDateTime toString的字符串可以通过此format解析
                BinlogOffset binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12T13:00:00.135789");
                assertEquals(
                        binlogOffset.getTimestamp(),
                        1639285200135789L + (10 * 3600 * 1000 * 1000L));
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 23:00:00.135789+08:00");

                // SQL_LOCAL_DATE_TIME, postgres timestamp 的字符串可以通过此format解析
                binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12 13:00:00.135789");
                assertEquals(
                        binlogOffset.getTimestamp(),
                        1639285200135789L + (10 * 3600 * 1000 * 1000L));
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 23:00:00.135789+08:00");
            }
            {
                TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.ofHours(0)));

                // ISO_LOCAL_DATE_TIME, LocalDateTime toString的字符串可以通过此format解析
                BinlogOffset binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12T13:00:00.135789");
                assertEquals(
                        binlogOffset.getTimestamp(), 1639285200135789L + (8 * 3600 * 1000 * 1000L));
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 21:00:00.135789+08:00");

                // SQL_LOCAL_DATE_TIME, postgres timestamp 的字符串可以通过此format解析
                binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12 13:00:00.135789");
                assertEquals(
                        binlogOffset.getTimestamp(), 1639285200135789L + (8 * 3600 * 1000 * 1000L));
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 21:00:00.135789+08:00");
            }
            {
                TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.ofHours(4)));

                // ISO_LOCAL_DATE_TIME, LocalDateTime toString的字符串可以通过此format解析
                BinlogOffset binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12T13:00:00.135789");
                assertEquals(
                        binlogOffset.getTimestamp(), 1639285200135789L + (4 * 3600 * 1000 * 1000L));
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 17:00:00.135789+08:00");

                // SQL_LOCAL_DATE_TIME, postgres timestamp 的字符串可以通过此format解析
                binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12 13:00:00.135789");
                assertEquals(
                        binlogOffset.getTimestamp(), 1639285200135789L + (4 * 3600 * 1000 * 1000L));
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 17:00:00.135789+08:00");
            }
            {
                TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.ofHours(8)));

                // ISO_LOCAL_DATE_TIME, LocalDateTime toString的字符串可以通过此format解析
                BinlogOffset binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12T13:00:00.135789");
                assertEquals(binlogOffset.getTimestamp(), 1639285200135789L);
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 13:00:00.135789+08:00");

                // SQL_LOCAL_DATE_TIME, postgres timestamp 的字符串可以通过此format解析
                binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12 13:00:00.135789");
                assertEquals(binlogOffset.getTimestamp(), 1639285200135789L);
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 13:00:00.135789+08:00");
            }
            {
                TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.ofHours(10)));

                // ISO_LOCAL_DATE_TIME, LocalDateTime toString的字符串可以通过此format解析
                BinlogOffset binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12T13:00:00.135789");
                assertEquals(
                        binlogOffset.getTimestamp(),
                        1639285200135789L + (-2 * 3600 * 1000 * 1000L));
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 11:00:00.135789+08:00");

                // SQL_LOCAL_DATE_TIME, postgres timestamp 的字符串可以通过此format解析
                binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12 13:00:00.135789");
                assertEquals(
                        binlogOffset.getTimestamp(),
                        1639285200135789L + (-2 * 3600 * 1000 * 1000L));
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 11:00:00.135789+08:00");
            }
            {
                // 夏令时, 冬季 UTC+1, 夏季 UTC+2
                TimeZone.setDefault(TimeZone.getTimeZone("Europe/Berlin"));
                // ISO_LOCAL_DATE_TIME, LocalDateTime toString的字符串可以通过此format解析
                BinlogOffset binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12T13:00:00.135789");
                assertEquals(
                        binlogOffset.getTimestamp(), 1639285200135789L + (7 * 3600 * 1000 * 1000L));
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 20:00:00.135789+08:00");

                // SQL_LOCAL_DATE_TIME, postgres timestamp 的字符串可以通过此format解析
                binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-12-12 13:00:00.135789");
                assertEquals(
                        binlogOffset.getTimestamp(), 1639285200135789L + (7 * 3600 * 1000 * 1000L));
                assertEquals(binlogOffset.getStartTimeText(), "2021-12-12 20:00:00.135789+08:00");

                // ISO_LOCAL_DATE_TIME, LocalDateTime toString的字符串可以通过此format解析
                binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-06-12T13:00:00.135789");
                assertEquals(binlogOffset.getTimestamp(), 1623495600135789L);
                assertEquals(binlogOffset.getStartTimeText(), "2021-06-12 19:00:00.135789+08:00");

                // SQL_LOCAL_DATE_TIME, postgres timestamp 的字符串可以通过此format解析
                binlogOffset = new BinlogOffset();
                binlogOffset.setTimestamp("2021-06-12 13:00:00.135789");
                assertEquals(binlogOffset.getTimestamp(), 1623495600135789L);
                assertEquals(binlogOffset.getStartTimeText(), "2021-06-12 19:00:00.135789+08:00");
            }
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    @Test
    void testSetTimestampInvalidFormat() {
        BinlogOffset binlogOffset = new BinlogOffset();
        assertThrows(
                IllegalArgumentException.class,
                () -> binlogOffset.setTimestamp("invalid-timestamp-format"));
    }
}
