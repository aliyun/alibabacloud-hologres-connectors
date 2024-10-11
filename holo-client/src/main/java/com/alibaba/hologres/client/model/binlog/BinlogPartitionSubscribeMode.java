package com.alibaba.hologres.client.model.binlog;

/**
 * enum for binlog partition mode.
 */
public enum BinlogPartitionSubscribeMode {
    DISABLE,
    // Specify a set of partition values when subscribe starts, which cannot be modified afterward.
    STATIC,
    // Specify a starting partition table when subscribe starts, and then subscribe in chronological order（the table is partitioned by time).
    DYNAMIC
}
