/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.binlog.action;

import com.alibaba.hologres.client.Subscribe;
import com.alibaba.hologres.client.impl.action.AbstractAction;
import com.alibaba.hologres.client.impl.binlog.BinlogRecordCollector;
import com.alibaba.hologres.client.impl.binlog.TableSchemaSupplier;
import com.alibaba.hologres.client.utils.Tuple;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/** Binlog消费请求. */
public class BinlogAction extends AbstractAction<Void> {
    final String tableName;
    final int shardId;
    final long lsn;
    final String timestamp;
    final BinlogRecordCollector collector;
    final String slotName;
    final String consumerGroup;
    final TableSchemaSupplier supplier;
    // Reader通过这个queue把flush请求发过去，worker搞定了以后通过future通知reader
    final Queue<Tuple<CompletableFuture<Void>, Long>> commitJob;
    final boolean enableCompression;
    final String[] projectionColumnNames;
    final List<Subscribe.BinlogFilter> binlogFilters;
    final String logicalPartitionColumnNames;
    final String logicalPartitionValues;

    // hologres 2.1版本起，可以不创建slot，仅使用表名消费。同时也不需要commit
    public BinlogAction(
            String tableName,
            String consumerGroup,
            int shardId,
            long lsn,
            String timestamp,
            boolean compressed,
            BinlogRecordCollector collector,
            TableSchemaSupplier supplier,
            String[] projectionColumnNames,
            List<Subscribe.BinlogFilter> binlogFilters) {
        this(
                tableName,
                null,
                consumerGroup,
                shardId,
                lsn,
                timestamp,
                compressed,
                collector,
                supplier,
                null,
                projectionColumnNames,
                binlogFilters,
                null,
                null);
    }

    public BinlogAction(
            String tableName,
            String slotName,
            String consumerGroup,
            int shardId,
            long lsn,
            String timestamp,
            boolean enableCompression,
            BinlogRecordCollector collector,
            TableSchemaSupplier supplier,
            Queue<Tuple<CompletableFuture<Void>, Long>> commitJob,
            String[] projectionColumnNames,
            List<Subscribe.BinlogFilter> binlogFilters,
            String logicalPartitionColumnNames,
            String logicalPartitionValues) {
        this.tableName = tableName;
        this.slotName = slotName;
        this.consumerGroup = consumerGroup;
        this.shardId = shardId;
        this.lsn = lsn;
        this.timestamp = timestamp;
        this.enableCompression = enableCompression;
        this.collector = collector;
        this.supplier = supplier;
        this.commitJob = commitJob;
        this.projectionColumnNames = projectionColumnNames;
        this.logicalPartitionColumnNames = logicalPartitionColumnNames;
        this.logicalPartitionValues = logicalPartitionValues;
        this.binlogFilters = binlogFilters;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSlotName() {
        return slotName;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public int getShardId() {
        return shardId;
    }

    public long getLsn() {
        return lsn;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public boolean isEnableCompression() {
        return enableCompression;
    }

    public BinlogRecordCollector getCollector() {
        return collector;
    }

    public TableSchemaSupplier getSupplier() {
        return supplier;
    }

    public Queue<Tuple<CompletableFuture<Void>, Long>> getCommitJob() {
        return commitJob;
    }

    public String[] getProjectionColumnNames() {
        return projectionColumnNames;
    }

    public List<Subscribe.BinlogFilter> getBinlogFilters() {
        return binlogFilters;
    }

    public String getLogicalPartitionColumnNames() {
        return logicalPartitionColumnNames;
    }

    public String getLogicalPartitionValues() {
        return logicalPartitionValues;
    }
}
