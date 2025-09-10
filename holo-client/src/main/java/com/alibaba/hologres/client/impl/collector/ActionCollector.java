/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.collector;

import com.alibaba.hologres.client.Get;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.ExecutionPool;
import com.alibaba.hologres.client.impl.util.ExceptionUtil;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Action收集器. 每一个HoloClient对应一个ActionClient ActionCollector - TableCollector PutAction收集器（表级别） -
 * TableShardCollector PutAction收集器（shard级别，此shard和holo的shard是2个概念，仅代表客户端侧的数据攒批的分区） - queue
 * GetAction的队列
 */
public class ActionCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(ActionCollector.class);
    Map<TableName, TableCollector> map;

    private ReentrantReadWriteLock flushLock = new ReentrantReadWriteLock();
    final HoloConfig config;
    final ExecutionPool pool;
    final ArrayBlockingQueue<Get> queue;

    private final ResizePolicy resizePolicy;
    private final long writerShardCountResizeIntervalNano;

    public ActionCollector(HoloConfig config, ExecutionPool pool, ArrayBlockingQueue<Get> queue) {
        map = new ConcurrentHashMap<>();
        this.config = config;
        this.pool = pool;
        this.queue = queue;
        this.resizePolicy = new DefaultResizePolicy();
        resizePolicy.init(config);
        this.writerShardCountResizeIntervalNano =
                config.getWriterShardCountResizeIntervalMs() * 1000000L;
    }

    public long getByteSize() {
        return map.values().stream().collect(Collectors.summingLong(TableCollector::getByteSize));
    }

    public void append(Record record) throws HoloClientException {
        flushLock.readLock().lock();
        try {
            TableCollector pairArray =
                    map.computeIfAbsent(
                            record.getTableName(), (tableName) -> new TableCollector(config, pool));
            pairArray.append(record);
            HoloClientException exception = lastException.getAndSet(null);
            if (null != exception) {
                throw exception;
            }
        } finally {
            flushLock.readLock().unlock();
        }
    }

    public void appendGet(Get get) {
        try {
            if (!queue.offer(
                    get, config.getReadBatchQueueOfferTimeoutMs(), TimeUnit.MILLISECONDS)) {
                get.getFuture()
                        .completeExceptionally(
                                new TimeoutException(
                                        "The get request queue is full. Consider increasing the readThreadSize."));
            }
        } catch (InterruptedException e) {
            get.getFuture().completeExceptionally(e);
        }
    }

    public void appendGet(List<Get> list) {
        try {
            boolean timeout = false;
            for (Iterator<Get> iter = list.iterator(); iter.hasNext(); ) {
                Get get = iter.next();
                if (timeout) {
                    get.getFuture().completeExceptionally(new TimeoutException());
                    continue;
                }
                if (!queue.offer(
                        get, config.getReadBatchQueueOfferTimeoutMs(), TimeUnit.MILLISECONDS)) {
                    get.getFuture()
                            .completeExceptionally(
                                    new TimeoutException(
                                            "The get request queue is full. Consider increasing the readThreadSize."));
                    timeout = true;
                }
            }
        } catch (InterruptedException e) {
            for (Get get : list) {
                get.getFuture().completeExceptionally(e);
            }
        }
    }

    AtomicReference<HoloClientException> lastException = new AtomicReference<>(null);

    /**
     * 该函数仅由ExecutionPool后台线程调用，因此不能抛出任何异常，要在后续的flush(internal=false)、append时再抛出.
     *
     * @throws HoloClientException
     */
    public void tryFlush() {
        flushLock.readLock().lock();
        try {
            for (Iterator<Map.Entry<TableName, TableCollector>> iter = map.entrySet().iterator();
                    iter.hasNext(); ) {
                TableCollector array = iter.next().getValue();
                try {
                    array.flush(false);
                } catch (HoloClientException e) {
                    LOGGER.error("try flush fail", e);
                    lastException.accumulateAndGet(
                            e, (lastOne, newOne) -> ExceptionUtil.merge(lastOne, newOne));
                }
            }
        } finally {
            flushLock.readLock().unlock();
        }
    }

    public void flush(boolean internal) throws HoloClientException {
        flushLock.writeLock().lock();
        try {
            HoloClientException exception = null;
            int doneCount = 0;
            AtomicInteger uncommittedActionCount = new AtomicInteger(0);
            boolean async = true;
            while (true) {
                doneCount = 0;
                uncommittedActionCount.set(0);
                for (Iterator<Map.Entry<TableName, TableCollector>> iter =
                                map.entrySet().iterator();
                        iter.hasNext(); ) {
                    TableCollector array = iter.next().getValue();
                    try {
                        if (array.flush(true, async, uncommittedActionCount)) {
                            ++doneCount;
                        }
                    } catch (HoloClientException e) {
                        exception = ExceptionUtil.merge(exception, e);
                    }
                }
                if (doneCount == map.size()) {
                    break;
                }
                if (uncommittedActionCount.get() == 0) {
                    async = false;
                }
            }
            // 此时所有的TableCollector的buffer都已经是空的了，根据统计信息尝试resize shard数
            resize();
            if (exception != null) {
                lastException.accumulateAndGet(
                        exception, (lastOne, newOne) -> ExceptionUtil.merge(lastOne, newOne));
            }

            if (!internal) {
                HoloClientException last = lastException.getAndSet(null);
                if (last != null) {
                    throw last;
                }
            }
        } finally {
            flushLock.writeLock().unlock();
        }
    }

    private void resize() {
        long currentNano = System.nanoTime();
        for (Map.Entry<TableName, TableCollector> entry : map.entrySet()) {
            TableName tableName = entry.getKey();
            TableCollector tableCollector = entry.getValue();
            if (tableCollector.getStat().getNanoTime() + writerShardCountResizeIntervalNano
                    < currentNano) {
                int currentSize = tableCollector.getShardCount();
                int size =
                        resizePolicy.calculate(
                                tableName,
                                tableCollector.getStat(),
                                tableCollector.getShardCount(),
                                pool.getWorkerCount(),
                                currentNano);
                if (currentSize != size) {
                    LOGGER.info(
                            "resize table {} shard size , {} -> {}", tableName, currentSize, size);
                    tableCollector.resize(size);
                }
                tableCollector.getStat().clear();
            }
        }
    }
}
