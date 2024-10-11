/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.impl.binlog.BinlogOffset;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * 消费binlog的请求.
 */
public class Subscribe {
	private final String tableName;
	private final String slotName;
	//两个builder会确保offsetMap和startTime
	private final Map<Integer, BinlogOffset> offsetMap;
	private final String binlogReadStartTime;

	// tableName是分区父表时,分区子表的消费请求
	private final NavigableMap<String, Subscribe> partitionToSubscribeMap;

	protected Subscribe(String tableName, String slotName, Map<Integer, BinlogOffset> offsetMap, String binlogReadStartTime) {
		this(tableName, slotName, offsetMap, binlogReadStartTime, new TreeMap<>());
	}

	protected Subscribe(String tableName, String slotName, Map<Integer, BinlogOffset> offsetMap, String binlogReadStartTime, NavigableMap<String, Subscribe> partitionToSubscribeMap) {
		this.tableName = tableName;
		this.slotName = slotName;
		this.offsetMap = offsetMap;
		this.binlogReadStartTime = binlogReadStartTime;
		this.partitionToSubscribeMap = partitionToSubscribeMap;
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

	public NavigableMap<String, Subscribe> getPartitionToSubscribeMap() {
		return partitionToSubscribeMap;
	}

	@Override
	public String toString() {
        return "Subscribe{" +
                "tableName='" + tableName + '\'' +
                ", slotName='" + slotName + '\'' +
                ", offsetMap=" + offsetMap +
                ", binlogReadStartTime='" + binlogReadStartTime + '\'' +
                ", partitionToSubscribeMap=" + partitionToSubscribeMap +
                '}';
    }

	public static OffsetBuilder newOffsetBuilder(String tableName) {
		return new OffsetBuilder(tableName);
	}

	public static OffsetBuilder newOffsetBuilder(String tableName, String slotName) {
		return new OffsetBuilder(tableName, slotName);
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
		private Map<String, Subscribe.OffsetBuilder> partitionToBuilderMap;

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

		/**
		 * 为一组shard设置offset.
		 * 	一般用于批量设置启动时间.
		 *
		 * @param shardIds
		 * @param offset
		 * @return
		 */
		public OffsetBuilder addShardsStartOffset(Set<Integer> shardIds, BinlogOffset offset) {
			shardIds.forEach(shardId -> addShardStartOffset(shardId, offset));
			return this;
		}

		public OffsetBuilder addShardStartOffsetForPartition(String partitionName, int shardId, BinlogOffset offset) {
			if (partitionToBuilderMap == null) {
				partitionToBuilderMap = new HashMap<>();
            }
			OffsetBuilder partitionBuilder = partitionToBuilderMap.computeIfAbsent(partitionName, k -> new OffsetBuilder(partitionName));
			partitionBuilder.addShardStartOffset(shardId, offset);
            return this;
        }

		public OffsetBuilder addShardsStartOffsetForPartition(String partitionName, Set<Integer> shardIds, BinlogOffset offset) {
			if (partitionToBuilderMap == null) {
				partitionToBuilderMap = new HashMap<>();
            }
			OffsetBuilder partitionBuilder = partitionToBuilderMap.computeIfAbsent(partitionName, k -> new OffsetBuilder(partitionName));
			partitionBuilder.addShardsStartOffset(shardIds, offset);
			return this;
        }

		public Subscribe build() {
			if (offsetMap == null) {
				throw new InvalidParameterException("must call addShardStartOffset before build");
			}
			if (partitionToBuilderMap != null) {
                NavigableMap<String, Subscribe> partitionToSubscribeMap = new TreeMap<>();
				partitionToBuilderMap.forEach((partition, subscribe) -> {
                    partitionToSubscribeMap.put(partition, subscribe.build());
                });
                return new Subscribe(tableName, slotName, offsetMap, null, partitionToSubscribeMap);
            } else {
                return new Subscribe(tableName, slotName, offsetMap, null);
            }
		}
	}

	/**
	 * 指定时间时用的, 不能指定shard, 运行时会默认给表的所有shard指定相同的启动时间.
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
