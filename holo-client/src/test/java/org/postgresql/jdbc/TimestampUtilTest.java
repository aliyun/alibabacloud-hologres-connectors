package org.postgresql.jdbc;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Timestamp;

public class TimestampUtilTest {

    @Test
    public void testTimestampToMicroSecondPreEpoch() {
        // -877ms => Math.floorDiv(-877, 1000) = -1 second, Math.floorMod(-877, 1000) = 123 millis
        // micros = -1*1000000 + 123*1000 = -877000
        long ms = -877L;
        long micros = TimestampUtil.timestampToMicroSecond(ms, "timestamp", false);
        Assert.assertTrue(micros < 0, "pre-epoch micros should be negative");
        Assert.assertEquals(micros, -877000L);
    }

    @Test
    public void testTimestampToMicroSecondPreEpochDate() {
        java.util.Date date = new java.util.Date(-877L);
        long micros = TimestampUtil.timestampToMicroSecond(date, "timestamp", false);
        Assert.assertTrue(micros < 0);
        Assert.assertEquals(micros, -877000L);
    }

    @Test
    public void testTimestampToMicroSecondPreEpochTimestamp() {
        Timestamp ts = Timestamp.valueOf("1969-12-31 23:59:59.123456");
        long micros = TimestampUtil.timestampToMicroSecond(ts, "timestamp", false);
        Assert.assertTrue(micros < 0, "pre-epoch micros should be negative");
        Assert.assertEquals(micros, -876544L);
    }

    @Test
    public void testTimestampToMicroSecondPostEpoch() {
        long ms = 1234L;
        long micros = TimestampUtil.timestampToMicroSecond(ms, "timestamp", false);
        // 1234ms = 1 second + 234 millis => 1*1000000 + 234*1000 = 1234000
        Assert.assertEquals(micros, 1234000L);
    }

    @Test
    public void testTimestampToMicroSecondExactSecondNegative() {
        long ms = -1000L;
        long micros = TimestampUtil.timestampToMicroSecond(ms, "timestamp", false);
        Assert.assertEquals(micros, -1000000L);
    }

    @Test
    public void testTimestampToMicroSecondZero() {
        long ms = 0L;
        long micros = TimestampUtil.timestampToMicroSecond(ms, "timestamp", false);
        Assert.assertEquals(micros, 0L);
    }
}
