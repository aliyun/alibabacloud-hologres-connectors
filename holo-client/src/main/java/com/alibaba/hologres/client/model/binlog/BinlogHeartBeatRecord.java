/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model.binlog;

import com.alibaba.hologres.client.impl.binlog.BinlogEventType;
import com.alibaba.hologres.client.model.TableSchema;

/** 仅仅用来表示消费的timestamp位置. 当没有新数据产生时，lsn不会变，但timestamp可以推进，表示消费延时. */
public class BinlogHeartBeatRecord extends BinlogRecord {

    public BinlogHeartBeatRecord(
            TableSchema schema, long lsn, BinlogEventType eventType, long timestamp) {
        super(schema, lsn, eventType, timestamp);
    }

    @Override
    public boolean isHeartBeat() {
        return true;
    }
}
