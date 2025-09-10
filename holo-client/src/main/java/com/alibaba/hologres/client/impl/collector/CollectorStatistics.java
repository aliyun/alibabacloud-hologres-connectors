/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.collector;

import java.util.concurrent.atomic.AtomicInteger;

/** 攒批的统计信息. */
public class CollectorStatistics {

    /** 因为条数或byte大小提交的Batch数. */
    AtomicInteger fullBatchCount = new AtomicInteger(0);

    /** 因为时间（或者总内存不够了，不一定是这个表的问题）提交的Batch数. */
    AtomicInteger notFullBatchCount = new AtomicInteger(0);

    /** 每轮统计的开始时间. */
    long nanoTime = System.nanoTime();

    public void add(BatchState state) {
        switch (state) {
            case SizeEnough:
            case ByteSizeEnough:
            case ByteSizeCondition:
                fullBatchCount.incrementAndGet();
                break;
            case NotEnough:
                break;
            default:
                notFullBatchCount.incrementAndGet();
        }
    }

    public int getFullBatchCount() {
        return fullBatchCount.get();
    }

    public int getNotFullBatchCount() {
        return notFullBatchCount.get();
    }

    public long getNanoTime() {
        return nanoTime;
    }

    public void clear() {
        fullBatchCount.set(0);
        notFullBatchCount.set(0);
        nanoTime = System.nanoTime();
    }
}
