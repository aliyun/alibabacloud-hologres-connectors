/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.Set;

/**
 * 动态分区相关属性.
 */
public class AutoPartitioning implements Serializable {
	boolean enable;
	AutoPartitioningTimeUnit timeUnit;
	ZoneId timeZoneId;
	int preCreateNum;

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

	public void setTimeZone(String timeZone) {
		// 获取所有可用的时区ID集合
		Set<String> zoneIds = ZoneId.getAvailableZoneIds();

		// 尝试匹配全大写时区字符串到有效的时区ID
		String matchingZoneId = zoneIds.stream()
				.filter(id -> id.equalsIgnoreCase(timeZone))
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException("Unknown time-zone ID: " + timeZone));
		this.timeZoneId = ZoneId.of(matchingZoneId);
	}

	public int getPreCreateNum() {
		return preCreateNum;
	}

	public void setPreCreateNum(int preCreateNum) {
		this.preCreateNum = preCreateNum;
	}

	/**
	 * 动态分区支持的时间单位.
	 */
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
}

