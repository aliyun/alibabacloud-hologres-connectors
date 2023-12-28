/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.impl.binlog.BinlogOffset;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;

/**
 * 消费binlog的请求.
 */
public class Subscribe {
	private final String tableName;
	private final String slotName;
	//两个builder会确保offsetMap和startTime
	private final Map<Integer, BinlogOffset> offsetMap;
	private final String binlogReadStartTime;

	protected Subscribe(String tableName, String slotName, Map<Integer, BinlogOffset> offsetMap, String binlogReadStartTime) {
		this.tableName = tableName;
		this.slotName = slotName;
		this.offsetMap = offsetMap;
		this.binlogReadStartTime = binlogReadStartTime;
	}

	public String getTableName() {
		return tableName;
	}

	public String getSlotName() {
		return slotName;
	}

	public Map<Integer, BinlogOffset> getOffsetMap() {
		return offsetMap;
	}

	public String getBinlogReadStartTime() {
		return binlogReadStartTime;
	}

	public static OffsetBuilder newOffsetBuilder(String tableName, String slotName) {
		return new OffsetBuilder(tableName, slotName);
	}

	public static OffsetBuilder newOffsetBuilder(String tableName) {
		return new OffsetBuilder(tableName);
	}

	public static StartTimeBuilder newStartTimeBuilder(String tableName, String slotName) {
		return new StartTimeBuilder(tableName, slotName);
	}

	public static StartTimeBuilder newStartTimeBuilder(String tableName) {
		return new StartTimeBuilder(tableName);
	}

	/**
	 * Builder.
	 */
	public abstract static class Builder {
		protected final String tableName;
		protected final String slotName;

		public Builder(String tableName) {
			if (tableName == null) {
				throw new InvalidParameterException("tableName must be not null");
			}
			this.tableName = tableName;
			this.slotName = null;
		}

		public Builder(String tableName, String slotName) {
			if (tableName == null) {
				throw new InvalidParameterException("tableName must be not null");
			}
			this.tableName = tableName;
			this.slotName = slotName;
		}
	}

	/**
	 * 指定shard时用的.
	 */
	public static class OffsetBuilder extends Builder {
		private Map<Integer, BinlogOffset> offsetMap;

		public OffsetBuilder(String tableName, String slotName) {
			super(tableName, slotName);
		}

		public OffsetBuilder(String tableName) {
			super(tableName);
		}

		/**
		 * 设定每个shardId的offset.
		 *
		 * @param shardId
		 * @param offset
		 * @return
		 */
		public OffsetBuilder addShardStartOffset(int shardId, BinlogOffset offset) {
			if (offsetMap == null) {
				offsetMap = new HashMap<>();
			}
			offsetMap.putIfAbsent(shardId, offset);
			return this;
		}

		public Subscribe build() {
			if (offsetMap == null) {
				throw new InvalidParameterException("must call addShardStartOffset before build");
			}
			return new Subscribe(tableName, slotName, offsetMap, null);
		}
	}

	/**
	 * 指定时间时用的.
	 */
	public static class StartTimeBuilder extends Builder {
		private String binlogReadStartTime;

		public StartTimeBuilder(String tableName, String slotName) {
			super(tableName, slotName);
		}

		public StartTimeBuilder(String tableName) {
			super(tableName);
		}

		public StartTimeBuilder setBinlogReadStartTime(String binlogReadStartTime) {
			this.binlogReadStartTime = binlogReadStartTime;
			return this;
		}

		public Subscribe build() {
			return new Subscribe(tableName, slotName, null, binlogReadStartTime);
		}
	}
}
