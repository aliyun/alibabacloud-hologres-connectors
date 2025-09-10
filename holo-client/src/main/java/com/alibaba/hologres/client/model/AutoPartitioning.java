/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.ZoneId;
import java.time.ZoneOffset;

/** 动态分区相关属性. */
public class AutoPartitioning implements Serializable {
    public static final Logger LOGGER = LoggerFactory.getLogger(AutoPartitioning.class);
    boolean enable;
    AutoPartitioningTimeUnit timeUnit;
    ZoneId timeZoneId;
    int preCreateNum;
    AutoPartitioningTimeFormat timeFormat;

    public AutoPartitioning() {
        this.enable = false;
    }

    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    public AutoPartitioningTimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(String timeUnit) {
        this.timeUnit = AutoPartitioningTimeUnit.of(timeUnit);
    }

    public void setTimeUnit(AutoPartitioningTimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    public ZoneId getTimeZoneId() {
        return timeZoneId;
    }

    public boolean setTimeZone(String pgZoneName) {
        // getAvailableZoneIds include all available region-based IDs.
        String matchingZoneId =
                ZoneId.getAvailableZoneIds().stream()
                        .filter(id -> id.equalsIgnoreCase(pgZoneName))
                        .findFirst()
                        .orElse(null);
        if (matchingZoneId != null) {
            this.timeZoneId = ZoneId.of(matchingZoneId);
            return true;
        } else {
            return false;
        }
    }

    public void setTimeZoneByOffset(int timeZoneOffsetSeconds) {
        // 非region-based的ID,都通过offset来计算
        this.timeZoneId = ZoneId.ofOffset("UTC", ZoneOffset.ofTotalSeconds(timeZoneOffsetSeconds));
    }

    public int getPreCreateNum() {
        return preCreateNum;
    }

    public void setPreCreateNum(int preCreateNum) {
        this.preCreateNum = preCreateNum;
    }

    public AutoPartitioningTimeFormat getTimeFormat() {
        return timeFormat;
    }

    public void setTimeFormat(String timeFormat) {
        this.timeFormat = AutoPartitioningTimeFormat.of(timeFormat);
    }

    /** 动态分区支持的时间单位. */
    public enum AutoPartitioningTimeUnit {
        HOUR("hour"),
        DAY("day"),
        MONTH("month"),
        QUARTER("quarter"),
        YEAR("year");

        private final String timeUnit;

        AutoPartitioningTimeUnit(String timeUnit) {
            this.timeUnit = timeUnit;
        }

        public String getTimeUnit() {
            return timeUnit;
        }

        public static AutoPartitioningTimeUnit of(String name) {
            switch (name.toLowerCase()) {
                case "hour":
                    return HOUR;
                case "day":
                    return DAY;
                case "month":
                    return MONTH;
                case "quarter":
                    return QUARTER;
                case "year":
                    return YEAR;
                default:
                    throw new IllegalArgumentException("Invalid time unit: " + name);
            }
        }
    }

    public enum AutoPartitioningTimeFormat {

        // 默认格式：YYYYMMDDHH24（HOUR）、YYYYMMDD（DAY）、YYYYMM（MONTH）、YYYYQ（QUARTER）、YYYY（YEAR）。
        YYYYMMDDHH24("YYYYMMDDHH24"),
        YYYYMMDD("YYYYMMDD"),
        YYYYMM("YYYYMM"),
        YYYYQ("YYYYQ"),
        YYYY("YYYY"),

        // 3.0.12之后可选格式：YYYY-MM-DD-HH24（HOUR）、YYYY-MM-DD（DAY）、YYYY-MM（MONTH）、YYYY-Q（QUARTER）。
        YYYY_MM_DD_HH24("YYYY-MM-DD-HH24"),
        YYYY_MM_DD("YYYY-MM-DD"),
        YYYY_MM("YYYY-MM"),
        YYYY_Q("YYYY-Q");
        private final String timeFormat;

        AutoPartitioningTimeFormat(String timeFormat) {
            this.timeFormat = timeFormat;
        }

        public String getTimeFormat() {
            return timeFormat;
        }

        public static AutoPartitioningTimeFormat of(String name) {
            if (null == name) {
                return null;
            }
            switch (name.toUpperCase()) {
                case "YYYYMMDDHH24":
                    return YYYYMMDDHH24;
                case "YYYYMMDD":
                    return YYYYMMDD;
                case "YYYYMM":
                    return YYYYMM;
                case "YYYYQ":
                    return YYYYQ;
                case "YYYY":
                    return YYYY;
                case "YYYY-MM-DD-HH24":
                    return YYYY_MM_DD_HH24;
                case "YYYY-MM-DD":
                    return YYYY_MM_DD;
                case "YYYY-MM":
                    return YYYY_MM;
                case "YYYY-Q":
                    return YYYY_Q;
                default:
                    LOGGER.warn("Invalid auto partitioning time format: " + name);
                    return null;
            }
        }
    }

    @Override
    public String toString() {
        return enable
                ? ("AutoPartitioning{"
                        + "enable="
                        + enable
                        + ", timeUnit="
                        + timeUnit
                        + ", timeZoneId="
                        + timeZoneId
                        + ", preCreateNum="
                        + preCreateNum
                        + (timeFormat != null ? (", timeFormat=" + timeFormat) : "")
                        + '}')
                : "";
    }
}
