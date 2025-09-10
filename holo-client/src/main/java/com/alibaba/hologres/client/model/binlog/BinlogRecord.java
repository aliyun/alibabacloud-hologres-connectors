/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model.binlog;

import com.alibaba.hologres.client.impl.binlog.BinlogEventType;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Binlog Record, 包含binlog相关参数. */
public class BinlogRecord extends Record implements Serializable {
    private final long lsn;
    private final BinlogEventType eventType;
    private final long timestamp;
    private static final Set<String> internalColNames =
            new HashSet<>(
                    Arrays.asList(
                            "hg_binlog_event_type", "hg_binlog_lsn", "hg_binlog_timestamp_us"));

    public BinlogRecord(TableSchema schema, long lsn, BinlogEventType eventType, long timestamp) {
        super(schema);
        this.lsn = lsn;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    public long getBinlogLsn() {
        return lsn;
    }

    public BinlogEventType getBinlogEventType() {
        return eventType;
    }

    public long getBinlogTimestamp() {
        return timestamp;
    }

    public boolean isHeartBeat() {
        return false;
    }

    @Override
    public String toString() {
        return "BinlogRecord{"
                + "schema="
                + getSchema()
                + ", binlog lsn="
                + lsn
                + ", binlog eventType="
                + eventType
                + ", binlog timestamp="
                + timestamp
                + ", values="
                + Arrays.toString(getValues())
                + ", bitSet="
                + getBitSet()
                + '}';
    }

    public static boolean IsInternalColumn(String columnName) {
        return internalColNames.contains(columnName.toLowerCase());
    }
}
