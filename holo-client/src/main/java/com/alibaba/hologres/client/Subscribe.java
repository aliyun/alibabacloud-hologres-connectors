/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.binlog.BinlogOffset;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.binlog.BinlogRecord;
import com.alibaba.hologres.client.utils.IdentifierUtil;
import com.alibaba.niagara.client.table.ServiceContractMsg;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import static com.alibaba.hologres.client.utils.CommonUtil.encodeColumnNamesToString;

/** 消费binlog的请求. */
public class Subscribe {
    private final String tableName;
    private String slotName;
    private String consumerGroup;
    // 两个builder会确保offsetMap和startTime
    private final Map<Integer, BinlogOffset> offsetMap;
    private final String binlogReadStartTime;

    private final boolean enableCompression;

    // tableName是分区父表时,分区子表的消费请求
    private final NavigableMap<String, Subscribe> partitionToSubscribeMap;

    // STATIC模式, 指定希望消费的分区的分区值,字符串数组
    private String[] partitionValues;

    private final String[] projectionColumnNames;
    private String[] logicalPartitionColumnNames;
    private String[][] logicalPartitionValues;
    private final List<BinlogFilter> binlogFilters;

    protected Subscribe(
            String tableName,
            String slotName,
            String consumerGroup,
            Map<Integer, BinlogOffset> offsetMap,
            String binlogReadStartTime,
            boolean enableCompression,
            String[] projectionColumnNames,
            List<BinlogFilter> binlogFilters) {
        this(
                tableName,
                slotName,
                consumerGroup,
                offsetMap,
                binlogReadStartTime,
                enableCompression,
                projectionColumnNames,
                binlogFilters,
                new TreeMap<String, Subscribe>(),
                new String[0]);
    }

    protected Subscribe(
            String tableName,
            String slotName,
            String consumerGroup,
            Map<Integer, BinlogOffset> offsetMap,
            String binlogReadStartTime,
            boolean enableCompression,
            String[] projectionColumnNames,
            List<BinlogFilter> binlogFilters,
            NavigableMap<String, Subscribe> partitionToSubscribeMap,
            String[] partitionValues) {
        this.tableName = tableName;
        this.slotName = slotName;
        this.consumerGroup = consumerGroup;
        this.offsetMap = offsetMap;
        this.binlogReadStartTime = binlogReadStartTime;
        this.enableCompression = enableCompression;
        this.projectionColumnNames =
                generateProjectionColumnNames(projectionColumnNames, binlogFilters);
        this.binlogFilters = binlogFilters;
        this.partitionToSubscribeMap = partitionToSubscribeMap;
        this.partitionValues = partitionValues;
    }

    protected Subscribe(
            String tableName,
            String slotName,
            String consumerGroup,
            Map<Integer, BinlogOffset> offsetMap,
            String binlogReadStartTime,
            Boolean enableCompression,
            String[] projectionColumnNames,
            List<BinlogFilter> binlogFilters,
            NavigableMap<String, Subscribe> partitionToSubscribeMap,
            String[] logicalPartitionNames,
            String[][] logicalPartitionValues) {
        this.tableName = tableName;
        this.slotName = slotName;
        this.consumerGroup = consumerGroup;
        this.offsetMap = offsetMap;
        this.binlogReadStartTime = binlogReadStartTime;
        this.enableCompression = enableCompression;
        this.partitionToSubscribeMap = partitionToSubscribeMap;
        this.projectionColumnNames =
                generateProjectionColumnNames(projectionColumnNames, binlogFilters);
        this.binlogFilters = binlogFilters;
        this.logicalPartitionColumnNames = logicalPartitionNames;
        this.logicalPartitionValues = logicalPartitionValues;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSlotName() {
        return slotName;
    }

    public void setSlotName(String slotName) {
        this.slotName = slotName;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public Map<Integer, BinlogOffset> getOffsetMap() {
        return offsetMap;
    }

    public String getBinlogReadStartTime() {
        return binlogReadStartTime;
    }

    public boolean isEnableCompression() {
        return enableCompression;
    }

    public NavigableMap<String, Subscribe> getPartitionToSubscribeMap() {
        return partitionToSubscribeMap;
    }

    public String[] getPartitionValuesToSubscribe() {
        return partitionValues;
    }

    public void setLogicalPartitionValuesToSubscribe(String[][] logicalPartitionValues) {
        this.logicalPartitionValues = logicalPartitionValues;
    }

    // Encode the logical_partition_values 2d array into a string.
    // The first level of the array represents  each row and is delimited by colons.
    // The second level represents each column in the same row and is delimited by commas.
    public String encodeLogicalPartitionValuesToSubscribe() throws HoloClientException {
        if (logicalPartitionValues == null) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < logicalPartitionValues.length; i++) {
            if (logicalPartitionValues[0].length != logicalPartitionValues[i].length) {
                throw new HoloClientException(
                        ExceptionCode.INVALID_REQUEST,
                        "Each row in partition-values-to-read should have the same column number");
            }
            for (int j = 0; j < logicalPartitionValues[i].length; j++) {
                if (j > 0) {
                    builder.append(",");
                }
                builder.append(
                        IdentifierUtil.quoteIdentifier(logicalPartitionValues[i][j], true, true));
            }
            if (i < logicalPartitionValues.length - 1) {
                builder.append(";");
            }
        }
        return builder.toString();
    }

    public void setLogicalPartitionColumnNamesToSubscribe(String[] logicalPartitionColumnNames) {
        this.logicalPartitionColumnNames = logicalPartitionColumnNames;
    }

    // Encode the logical_partition_column_names array into a string delimited by commas.
    public String encodeLogicalPartitionColumnNamesToSubscribe() {
        if (logicalPartitionColumnNames == null) {
            return null;
        }
        return encodeColumnNamesToString(logicalPartitionColumnNames);
    }

    // 反序列化读读取的BinaryRow时,需要用到列名,这里返回原始数组而不是array to string.
    public String[] getProjectionColumnNamesToSubscribe() {
        return projectionColumnNames;
    }

    public List<BinlogFilter> getBinlogFilters() {
        return binlogFilters;
    }

    @Override
    public String toString() {
        return "Subscribe{"
                + "tableName='"
                + tableName
                + ", slotName='"
                + slotName
                + ", consumerGroup='"
                + consumerGroup
                + ", offsetMap="
                + offsetMap
                + ", binlogReadStartTime='"
                + binlogReadStartTime
                + ", enableCompression="
                + enableCompression
                + ", projectionColumnNames="
                + Arrays.toString(projectionColumnNames)
                + ", binlogFilters="
                + binlogFilters.toString()
                + ", partitionToSubscribeMap="
                + partitionToSubscribeMap
                + ", logicalPartitionColumnNames="
                + Arrays.toString(logicalPartitionColumnNames)
                + ", logicalPartitionValues="
                + Arrays.deepToString(logicalPartitionValues)
                + '}';
    }

    // 对于filter 的column,如果projectionColumnNames中没有,则加入到projectionColumnNames中
    private String[] generateProjectionColumnNames(
            String[] projectionColumnNames, List<BinlogFilter> binlogFilters) {
        if (projectionColumnNames == null || binlogFilters == null || binlogFilters.isEmpty()) {
            return projectionColumnNames;
        }
        List<String> projectionColumnNameList = Arrays.asList(projectionColumnNames);
        for (BinlogFilter filter : binlogFilters) {
            String colName = filter.column.getName();
            // hg_binlog_event_type这些内部列在引擎侧会自动加到projection中
            if (!BinlogRecord.IsInternalColumn(colName)
                    && !projectionColumnNameList.contains(colName)) {
                projectionColumnNameList.add(colName);
            }
        }
        return projectionColumnNameList.toArray(new String[0]);
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

    /** Builder. */
    public abstract static class Builder {
        protected final String tableName;
        protected final String slotName;
        protected String consumerGroup;

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

        public abstract Builder setConsumerGroup(String consumerGroup);
    }

    /** 指定shard时用的. */
    public static class OffsetBuilder extends Builder {
        private Map<Integer, BinlogOffset> offsetMap;
        private boolean enableCompression = false;
        private Map<String, Subscribe.OffsetBuilder> partitionToBuilderMap;
        private List<String> partitionValuesToSubscribe = new ArrayList<>();
        private String[] projectionColumnNamesToSubscribe;
        private String[] logicalPartitionNamesToSubscribe;
        private String[][] logicalPartitionValuesToSubscribe;
        private List<BinlogFilter> binlogFilters;

        public OffsetBuilder(String tableName, String slotName) {
            super(tableName, slotName);
        }

        public OffsetBuilder(String tableName) {
            super(tableName);
        }

        @Override
        public OffsetBuilder setConsumerGroup(String consumerGroup) {
            this.consumerGroup = consumerGroup;
            return this;
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
         * 为一组shard设置offset. 一般用于批量设置启动时间.
         *
         * @param shardIds
         * @param offset
         * @return
         */
        public OffsetBuilder addShardsStartOffset(Set<Integer> shardIds, BinlogOffset offset) {
            shardIds.forEach(shardId -> addShardStartOffset(shardId, offset));
            return this;
        }

        public OffsetBuilder setEnableCompression(boolean enableCompression) {
            this.enableCompression = enableCompression;
            return this;
        }

        public OffsetBuilder addShardStartOffsetForPartition(
                String partitionName, int shardId, BinlogOffset offset) {
            if (partitionToBuilderMap == null) {
                partitionToBuilderMap = new HashMap<>();
            }
            OffsetBuilder partitionBuilder =
                    partitionToBuilderMap.computeIfAbsent(
                            partitionName, k -> new OffsetBuilder(partitionName));
            partitionBuilder.addShardStartOffset(shardId, offset);
            return this;
        }

        public OffsetBuilder addShardsStartOffsetForPartition(
                String partitionName, Set<Integer> shardIds, BinlogOffset offset) {
            if (partitionToBuilderMap == null) {
                partitionToBuilderMap = new HashMap<>();
            }
            OffsetBuilder partitionBuilder =
                    partitionToBuilderMap.computeIfAbsent(
                            partitionName, k -> new OffsetBuilder(partitionName));
            partitionBuilder.addShardsStartOffset(shardIds, offset);
            return this;
        }

        public OffsetBuilder addPartitionValuesToSubscribe(String[] partitionValues) {
            partitionValuesToSubscribe.addAll(Arrays.asList(partitionValues));
            return this;
        }

        public OffsetBuilder addProjectionColumnNamesToSubscribe(String[] projectionColumnNames) {
            projectionColumnNamesToSubscribe = projectionColumnNames;
            return this;
        }

        // Since logical partition table supports multiple partition columns, we need to pass in
        // a 2D array.
        public OffsetBuilder addLogicalPartitionValuesToSubscribe(
                String[][] logicalPartitionValues) {
            logicalPartitionValuesToSubscribe = logicalPartitionValues;
            return this;
        }

        public OffsetBuilder addLogicalPartitionNamesToSubscribe(String[] logicalPartitionNames) {
            logicalPartitionNamesToSubscribe = logicalPartitionNames;
            return this;
        }

        public OffsetBuilder addBinlogFilter(Column column, ServiceContractMsg.OperatorType op) {
            return addBinlogFilter(column, op, null);
        }

        public OffsetBuilder addBinlogFilter(
                Column column, ServiceContractMsg.OperatorType op, Object value) {
            BinlogFilter binlogFilter = new BinlogFilter(column, op, value);
            if (binlogFilters == null) {
                binlogFilters = new ArrayList<>();
            }
            binlogFilters.add(binlogFilter);
            return this;
        }

        public OffsetBuilder setBinlogFilters(List<BinlogFilter> binlogFilters) {
            this.binlogFilters = binlogFilters;
            return this;
        }

        public Subscribe build() {
            if (offsetMap == null) {
                throw new InvalidParameterException("must call addShardStartOffset before build");
            }
            NavigableMap<String, Subscribe> partitionToSubscribeMap = new TreeMap<>();
            if (partitionToBuilderMap != null) {
                // partitions inherit projectionColumnNames,enableCompression from parent table
                partitionToBuilderMap.forEach(
                        (partition, subscribe) ->
                                partitionToSubscribeMap.put(
                                        partition,
                                        subscribe
                                                .setEnableCompression(enableCompression)
                                                .addProjectionColumnNamesToSubscribe(
                                                        projectionColumnNamesToSubscribe)
                                                .setBinlogFilters(binlogFilters)
                                                .build()));
            }
            if (!partitionValuesToSubscribe.isEmpty()) {
                // Initialize Subscribe for physical partition table.
                return new Subscribe(
                        tableName,
                        slotName,
                        consumerGroup,
                        offsetMap,
                        null,
                        enableCompression,
                        projectionColumnNamesToSubscribe,
                        binlogFilters,
                        partitionToSubscribeMap,
                        partitionValuesToSubscribe.toArray(new String[0]));
            } else {
                // Initialize Subscribe for logical partition table.
                return new Subscribe(
                        tableName,
                        slotName,
                        consumerGroup,
                        offsetMap,
                        null,
                        enableCompression,
                        projectionColumnNamesToSubscribe,
                        binlogFilters,
                        partitionToSubscribeMap,
                        logicalPartitionNamesToSubscribe,
                        logicalPartitionValuesToSubscribe);
            }
        }
    }

    /** 指定时间时用的, 不能指定shard, 运行时会默认给表的所有shard指定相同的启动时间. */
    public static class StartTimeBuilder extends Builder {
        private String binlogReadStartTime;
        private boolean enableCompression = false;
        private List<String> partitionValuesToSubscribe = new ArrayList<>();
        private String[] projectionColumnNamesToSubscribe;
        private String[] logicalPartitionNamesToSubscribe;
        private String[][] logicalPartitionValuesToSubscribe;
        private List<BinlogFilter> binlogFilters;

        public StartTimeBuilder(String tableName, String slotName) {
            super(tableName, slotName);
        }

        public StartTimeBuilder(String tableName) {
            super(tableName);
        }

        @Override
        public StartTimeBuilder setConsumerGroup(String consumerGroup) {
            this.consumerGroup = consumerGroup;
            return this;
        }

        public StartTimeBuilder setBinlogReadStartTime(String binlogReadStartTime) {
            this.binlogReadStartTime = binlogReadStartTime;
            return this;
        }

        public StartTimeBuilder setEnableCompression(boolean enableCompression) {
            this.enableCompression = enableCompression;
            return this;
        }

        public StartTimeBuilder addProjectionColumnNamesToSubscribe(
                String[] projectionColumnNames) {
            projectionColumnNamesToSubscribe = projectionColumnNames;
            return this;
        }

        public StartTimeBuilder addPartitionValuesToSubscribe(String[] partitionValues) {
            partitionValuesToSubscribe.addAll(Arrays.asList(partitionValues));
            return this;
        }

        // Since logical partition table supports multiple partition columns, we need to pass in
        // a 2D array.
        public StartTimeBuilder addLogicalPartitionValuesToSubscribe(
                String[][] logicalPartitionValues) {
            logicalPartitionValuesToSubscribe = logicalPartitionValues;
            return this;
        }

        public StartTimeBuilder addLogicalPartitionNamesToSubscribe(
                String[] logicalPartitionNames) {
            logicalPartitionNamesToSubscribe = logicalPartitionNames;
            return this;
        }

        public StartTimeBuilder addBinlogFilter(Column column, ServiceContractMsg.OperatorType op) {
            return addBinlogFilter(column, op, null);
        }

        public StartTimeBuilder addBinlogFilter(
                Column column, ServiceContractMsg.OperatorType op, Object value) {
            BinlogFilter binlogFilter = new BinlogFilter(column, op, value);
            if (binlogFilters == null) {
                binlogFilters = new ArrayList<>();
            }
            binlogFilters.add(binlogFilter);
            return this;
        }

        public Subscribe build() {
            if (!partitionValuesToSubscribe.isEmpty()) {
                // Initialize Subscribe for physical partition table.
                return new Subscribe(
                        tableName,
                        slotName,
                        consumerGroup,
                        null,
                        binlogReadStartTime,
                        enableCompression,
                        projectionColumnNamesToSubscribe,
                        binlogFilters,
                        new TreeMap<>(),
                        partitionValuesToSubscribe.toArray(new String[0]));
            } else {
                // Initialize Subscribe for logical partition table.
                return new Subscribe(
                        tableName,
                        slotName,
                        consumerGroup,
                        null,
                        binlogReadStartTime,
                        enableCompression,
                        projectionColumnNamesToSubscribe,
                        binlogFilters,
                        new TreeMap<>(),
                        logicalPartitionNamesToSubscribe,
                        logicalPartitionValuesToSubscribe);
            }
        }
    }

    public static class BinlogFilter implements Serializable {
        public final Column column;
        public final ServiceContractMsg.OperatorType op;
        public final Object value;

        public BinlogFilter(Column column, ServiceContractMsg.OperatorType op, Object value) {
            this.column = column;
            this.op = op;
            this.value = value;
        }
    }
}
