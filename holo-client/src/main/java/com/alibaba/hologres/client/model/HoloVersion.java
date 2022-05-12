/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model;

import java.util.Objects;

/**
 * holo版本号.
 */
public class HoloVersion implements Comparable<HoloVersion> {
	private int majorVersion;
	private int minorVersion;
	private int fixVersion;

	public HoloVersion(int majorVersion, int minorVersion, int fixVersion) {
		this.majorVersion = majorVersion;
		this.minorVersion = minorVersion;
		this.fixVersion = fixVersion;
	}

	public int getMajorVersion() {
		return majorVersion;
	}

	public int getMinorVersion() {
		return minorVersion;
	}

	public int getFixVersion() {
		return fixVersion;
	}

	public boolean isUndefined() {
		return majorVersion == 0 && minorVersion == 0 && fixVersion == 0;
	}

	//e.g. 0.8.x
	public HoloVersion(String versionStr) {
		String[] arr = versionStr.split("\\.");
		if (arr.length >= 1) {
			majorVersion = str2int(arr[0]);
		}
		if (arr.length >= 2) {
			minorVersion = str2int(arr[1]);
		}
		if (arr.length >= 3) {
			fixVersion = str2int(arr[2]);
		}
	}

	private static int str2int(String s) {
		try {
			return Integer.valueOf(s);
		} catch (NumberFormatException e) {
			return 0;
		}
	}

	@Override
	public int compareTo(HoloVersion o) {
		if (majorVersion != o.majorVersion) {
			return majorVersion - o.majorVersion;
		}
		if (minorVersion != o.minorVersion) {
			return minorVersion - o.minorVersion;
		}
		return fixVersion - o.fixVersion;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		HoloVersion that = (HoloVersion) o;
		return majorVersion == that.majorVersion && minorVersion == that.minorVersion && fixVersion == that.fixVersion;
	}

	@Override
	public int hashCode() {
		return Objects.hash(majorVersion, minorVersion, fixVersion);
	}

	@Override
	public String toString() {
		return "HoloVersion{" +
				"majorVersion=" + majorVersion +
				", minorVersion=" + minorVersion +
				", fixVersion=" + fixVersion +
				'}';
	}
}

