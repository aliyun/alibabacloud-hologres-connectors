/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.binlog;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.binlog.BinlogRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/** 数据管道. */
public class BinlogRecordCollector {
    public static final Logger LOGGER = LoggerFactory.getLogger(BinlogRecordCollector.class);

    private final BlockingQueue<BinlogRecord> queue;
    private volatile HoloClientException exception = null;

    public BinlogRecordCollector(BlockingQueue<BinlogRecord> queue) {
        this.queue = queue;
    }

    /**
     * size == 0 表示追上了. 此处list并不会根据ignoreDelete和ignoreUpdateBefore，需要接受者自行处理.
     *
     * @param shardId
     * @param recordList array会被不断peek，pop
     * @return 最后一条写成功的
     * @throws InterruptedException
     */
    public BinlogRecord emit(int shardId, ArrayBuffer<BinlogRecord> recordList)
            throws InterruptedException {
        BinlogRecord lastSuccessRecord = null;
        do {
            BinlogRecord record = recordList.peek();
            boolean succ = queue.offer(record, 1000L, TimeUnit.MILLISECONDS);
            if (succ) {
                lastSuccessRecord = recordList.pop();
            } else {
                break;
            }
        } while (recordList.remain() > 0);
        return lastSuccessRecord;
    }

    /**
     * 捕捉到异常喂进来以后，worker那边就不会再工作了. 子线程发生异常时，使主线程感知
     *
     * @param e
     */
    public void exceptionally(int shardId, Throwable e) {
        LOGGER.error("shard id " + shardId + "fetch binlog fail", e);
        if (e instanceof HoloClientException) {
            exception = (HoloClientException) e;
        } else {
            exception =
                    new HoloClientException(
                            ExceptionCode.INTERNAL_ERROR,
                            "shard id " + shardId + " fetch binlog fail",
                            e);
        }
    }

    public BlockingQueue<BinlogRecord> getQueue() {
        return queue;
    }

    public HoloClientException getException() {
        return exception;
    }
}
