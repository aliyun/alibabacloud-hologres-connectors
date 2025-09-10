/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.collector;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.exception.HoloClientWithDetailsException;
import com.alibaba.hologres.client.impl.ExecutionPool;
import com.alibaba.hologres.client.impl.action.PutAction;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.RecordKey;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.checkandput.CheckAndPutCondition;
import com.alibaba.hologres.client.model.checkandput.CheckAndPutRecord;
import com.alibaba.hologres.client.model.expression.ExpressionUtil;
import com.alibaba.hologres.client.model.expression.RecordWithExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * PutAction收集器（shard级别）. 每一个HoloClient对应一个ActionClient ActionCollector - TableCollector
 * PutAction收集器（表级别） - TableShardCollector
 * PutAction收集器（shard级别，此shard和holo的shard是2个概念，仅代表客户端侧的数据攒批的分区） - queue GetAction的队列
 */
public class TableShardCollector {
    public static final Logger LOGGER = LoggerFactory.getLogger(TableShardCollector.class);

    private RecordCollector buffer;

    /** 当前buffer中的TableSchema. */
    private TableSchema tableSchemaInBuffer;

    private CheckAndPutCondition checkAndPutConditionInBuffer;
    private Integer exprHashCodeBuffer;
    private PutAction activeAction;
    private long activeActionByteSize = 0L;
    private final ExecutionPool pool;
    private final CollectorStatistics stat;
    private final boolean enableDeduplication;
    private final boolean enableAggressive;
    private final Set<Integer> verifiedExprCode;

    public TableShardCollector(
            HoloConfig config, ExecutionPool pool, CollectorStatistics stat, int size) {
        buffer = new RecordCollector(config, pool, size);
        activeAction = null;
        this.pool = pool;
        this.stat = stat;
        this.enableDeduplication = config.isEnableDeduplication();
        this.enableAggressive = config.isEnableAggressive();
        this.verifiedExprCode = new HashSet<>();
    }

    public synchronized void append(Record record) throws HoloClientException {
        HoloClientException exception = null;
        // 与之前的TableSchema不一致时，先commit，再append
        if (buffer.size() > 0 && !Objects.equals(record.getSchema(), tableSchemaInBuffer)) {
            try {
                flush(true, false, null);
            } catch (HoloClientException e) {
                exception = e;
            }
        }
        Integer exprHashCode = null;
        if (record instanceof RecordWithExpression) {
            exprHashCode = ((RecordWithExpression) record).getExpression().hashCode();
            if (!verifiedExprCode.contains(exprHashCode)) {
                ExpressionUtil.CheckExpr((RecordWithExpression) record);
                verifiedExprCode.add(exprHashCode);
            }
        }
        if (buffer.size() > 0 && !Objects.equals(exprHashCode, exprHashCodeBuffer)) {
            try {
                flush(true, false, null);
            } catch (HoloClientException e) {
                exception = e;
            }
        }

        // 与之前的CheckAndPutRecord的condition不一致时(包括之前不是CheckAndPut)，先commit，再append
        CheckAndPutCondition checkAndPutCondition = null;
        if (record instanceof CheckAndPutRecord) {
            checkAndPutCondition = ((CheckAndPutRecord) record).getCheckAndPutCondition();
        }
        if (buffer.size() > 0
                && !Objects.equals(checkAndPutCondition, checkAndPutConditionInBuffer)) {
            try {
                flush(true, false, null);
            } catch (HoloClientException e) {
                exception = e;
            }
        }
        // 配置不允许去重(checkAndPut Record强制不允许去重, RecordWithExpression
        // 强制不允许去重)，与之前的record主键重复时，先commit，再append
        if ((!enableDeduplication || checkAndPutCondition != null || exprHashCode != null)
                && buffer.isKeyExists(new RecordKey(record))) {
            try {
                flush(true, false, null);
            } catch (HoloClientException e) {
                exception = e;
            }
        }
        // 异常在函数末尾抛出，即使有异常，当前record也会被append到buffer中
        boolean full = buffer.append(record);
        setRecordInfoInBuffer(record);
        if (full) {
            try {
                waitActionDone();
            } catch (HoloClientWithDetailsException e) {
                if (exception == null) {
                    exception = e;
                } else if (exception instanceof HoloClientWithDetailsException) {
                    ((HoloClientWithDetailsException) exception).merge(e);
                }
            } catch (HoloClientException e) {
                exception = e;
            }
            commit(buffer.getBatchState());

        } else {
            boolean isActionDone = false;
            try {
                isActionDone = isActionDone();
            } catch (HoloClientWithDetailsException e) {
                if (exception == null) {
                    exception = e;
                } else if (exception instanceof HoloClientWithDetailsException) {
                    ((HoloClientWithDetailsException) exception).merge(e);
                }
            } catch (HoloClientException e) {
                exception = e;
            }
            // 激进模式, 发现空闲直接提交
            if (enableAggressive && isActionDone) {
                commit(buffer.getBatchState());
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    private void commit(BatchState state) throws HoloClientException {
        stat.add(state);
        activeAction =
                new PutAction(
                        buffer.getRecords(),
                        buffer.getByteSize(),
                        buffer.getOnConflictAction(),
                        state);
        try {
            while (!pool.submit(activeAction)) {}
            activeActionByteSize = activeAction.getByteSize();
        } catch (Exception e) {
            activeAction.getFuture().completeExceptionally(e);
            if (activeAction.getRecordList() != null) {
                for (Record record : activeAction.getRecordList()) {
                    if (record.getPutFutures() != null) {
                        for (CompletableFuture<Void> future : record.getPutFutures()) {
                            if (!future.isDone()) {
                                future.completeExceptionally(e);
                            }
                        }
                    }
                }
            }
            if (!(e instanceof HoloClientException)) {
                throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "", e);
            } else {
                throw e;
            }
        } finally {
            buffer.clear();
            // currentTableSchema = tableSchema in buffer.
            tableSchemaInBuffer = null;
            checkAndPutConditionInBuffer = null;
            exprHashCodeBuffer = null;
        }
    }

    /** append到buffer之后更新. */
    private void setRecordInfoInBuffer(Record record) {
        tableSchemaInBuffer = record.getSchema();
        if (record instanceof CheckAndPutRecord) {
            checkAndPutConditionInBuffer = ((CheckAndPutRecord) record).getCheckAndPutCondition();
        } else {
            checkAndPutConditionInBuffer = null;
        }
        if (record instanceof RecordWithExpression) {
            exprHashCodeBuffer = ((RecordWithExpression) record).getExpression().hashCode();
        } else {
            exprHashCodeBuffer = null;
        }
    }

    private void clearActiveAction() {
        activeAction = null;
        activeActionByteSize = 0L;
    }

    private void waitActionDone() throws HoloClientException {
        if (activeAction != null) {
            try {
                activeAction.getFuture().get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof HoloClientException) {
                    throw (HoloClientException) cause;
                } else {
                    throw new HoloClientException(
                            ExceptionCode.INTERNAL_ERROR, "unknow exception", cause);
                }
            } catch (InterruptedException e) {
            } finally {
                clearActiveAction();
            }
        }
    }

    private boolean isActionDone() throws HoloClientException {
        if (activeAction != null) {
            try {
                if (activeAction.getFuture().isDone()) {
                    // LOGGER.info("Pair Done:{}",readable.getPutAction().getFuture());
                    try {
                        activeAction.getFuture().get();
                    } finally {
                        clearActiveAction();
                    }
                    return true;
                } else {
                    return false;
                }
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                clearActiveAction();
                if (cause instanceof HoloClientException) {
                    throw (HoloClientException) cause;
                } else {
                    throw new HoloClientException(
                            ExceptionCode.INTERNAL_ERROR, "unknow exception", cause);
                }
            } catch (InterruptedException e) {
                return false;
            }
        } else {
            return true;
        }
    }

    /**
     * 是否flush完成.
     *
     * @param force 是否强制flush，强制flush只要buffer.size > 0就一定提交，否则还是看RecordCollector自己判断是不是应该提交
     * @param async 是否异步，同步的话，对于activeAction会wait到完成为止
     * @param uncommittedActionCount 如果activeAction未完成，并且buffer.size > 0 ，加一，表示还有任务没有提交給worker
     * @return true, 没有任何pending的记录
     * @throws HoloClientException 异常
     */
    public synchronized boolean flush(
            boolean force, boolean async, AtomicInteger uncommittedActionCount)
            throws HoloClientException {
        HoloClientWithDetailsException failedRecords = null;

        boolean readableDone = false;
        try {
            if (async) {
                readableDone = isActionDone();
            } else {
                readableDone = true;
                waitActionDone();
            }
        } catch (HoloClientWithDetailsException e) {
            readableDone = true;
            failedRecords = e;
        }
        boolean done = false;
        if (readableDone) {
            if (buffer.size > 0) {
                BatchState state = force ? BatchState.Force : buffer.getBatchState();
                if (state != BatchState.NotEnough) {
                    commit(state);
                }
            } else {
                done = true;
            }
        } else if (uncommittedActionCount != null) {
            if (buffer.size > 0) {
                uncommittedActionCount.incrementAndGet();
            }
        }

        if (failedRecords != null) {
            throw failedRecords;
        }

        return done;
    }

    public long getByteSize() {
        return activeActionByteSize + buffer.getByteSize();
    }
}
