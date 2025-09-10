/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.collector;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.impl.ExecutionPool;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.RecordKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** 将多条record记录在内存中进行聚合. */
public class RecordCollector {

    public static final Logger LOGGER = LoggerFactory.getLogger(RecordCollector.class);

    private final OnConflictAction action;
    private final int maxRecords;
    private final long maxByteSize;
    private final long maxWaitTime;

    private final int shardCount;
    private ExecutionPool pool;

    public RecordCollector(HoloConfig config, ExecutionPool pool, int shardCount) {
        this.action = config.getOnConflictAction();
        this.maxRecords = config.getWriteBatchSize();
        this.maxByteSize = config.getWriteBatchByteSize();
        this.maxWaitTime = config.getWriteMaxIntervalMs();
        this.pool = pool;
        this.shardCount = shardCount;
    }

    Map<RecordKey, Record> deleteRecords = new HashMap<>();
    Map<RecordKey, Record> records = new HashMap<>();

    int size = 0;
    long byteSize = 0L;
    long startTimeMs = -1L;

    /**
     * 有insert和delete两个map 处理逻辑如下： - insert map中已有主键相同数据 1. 新的record是delete，删掉insert
     * map中的已有数据，放入delete map delete map中本来就有相同主键的数据，替换旧的把新的放入delete map delete
     * map中没有主键相同的数据，放入delete map 2. 新的record是insert，根据WriteMode进行处理 INSERT_OR_UPDATE
     * 使用新的数据set过的字段覆盖旧的数据 INSERT_OR_IGNORE 啥也不做 INSERT_OR_REPLACE 使用新数据直接替换旧数据 - insert
     * map中没有主键相同数据 1. 新的record是delete delete map中本来就有相同主键的数据，替换旧的把新的放入delete map delete
     * map中没有主键相同的数据，放入delete map 2. 新的record是insert，根据WriteMode进行处理 INSERT_OR_UPDATE 将新数据放入insert
     * map INSERT_OR_IGNORE 将新数据放入insert map INSERT_OR_REPLACE 看下delete map里有没有主键相同的，有的话从delete
     * map删掉
     */
    public boolean append(Record record) {
        if (startTimeMs == -1) {
            startTimeMs = System.currentTimeMillis();
        }
        Map<RecordKey, Record> recordMap = records;
        Map<RecordKey, Record> deleteMap = deleteRecords;

        RecordKey key = new RecordKey(record);
        Record origin = recordMap.get(key);
        if (origin != null) {
            switch (record.getType()) {
                case DELETE:
                    Record deleteRecord = deleteMap.get(key);
                    /*
                     * 如果delete列表没有这个key
                     * record.attachmentList=origin.attachmentList+record.attachmentList
                     * 否则
                     * record.attachmentList=deleteRecord.attachmentList+origin.attachmentList+record.attachmentList
                     * */
                    if (null != deleteRecord) {
                        size += -1;
                        byteSize -= deleteRecord.getByteSize();
                        origin.cover(deleteRecord);
                    }
                    record.cover(origin);
                    recordMap.remove(key);
                    byteSize -= origin.getByteSize();
                    byteSize += record.getByteSize();
                    deleteMap.put(key, record);
                    break;
                case INSERT:
                    switch (action) {
                        case INSERT_OR_UPDATE:
                            byteSize -= origin.getByteSize();
                            origin.merge(record);
                            byteSize += origin.getByteSize();
                            origin.setType(Put.MutationType.INSERT);
                            break;
                        case INSERT_OR_IGNORE:
                            origin.addAttachmentList(record.getAttachmentList());
                            break;
                        case INSERT_OR_REPLACE:
                            record.cover(origin);
                            byteSize -= origin.getByteSize();
                            byteSize += record.getByteSize();
                            recordMap.put(key, record);
                            break;
                        default:
                            break;
                    }
                    break;
                default:
                    break;
            }
        } else {
            Record baseRecord;
            switch (record.getType()) {
                case DELETE:
                    baseRecord = deleteMap.get(key);
                    if (baseRecord == null) {
                        size += 1;
                    } else {
                        byteSize -= baseRecord.getByteSize();
                        record.cover(baseRecord);
                    }
                    byteSize += record.getByteSize();
                    deleteMap.put(key, record);
                    break;
                case INSERT:
                    byteSize += record.getByteSize();
                    recordMap.put(key, record);
                    if (action == OnConflictAction.INSERT_OR_REPLACE) {
                        baseRecord = deleteMap.get(key);
                        if (baseRecord == null) {
                            size += 1;
                        } else {
                            byteSize -= baseRecord.getByteSize();
                            record.cover(baseRecord);
                            deleteMap.remove(key);
                        }
                    } else {
                        ++size;
                    }
                    break;
                default:
                    break;
            }
        }
        BatchState bs = getBatchState();
        if (bs != BatchState.NotEnough) {
            return true;
        }
        return false;
    }

    public boolean isKeyExists(RecordKey key) {
        // 不论是delete还是insert，只要有相同的key，都将之前攒批的数据先flush掉
        return records.get(key) != null || deleteRecords.get(key) != null;
    }

    /**
     * reason. 1 isSizeEnough 2 isByteSizeEnough 3 isTimeWaitEnough 4 timeCondition 5
     * byteSizeCondition 6 totalByteSizeCondition 7 force 8 retry one by one
     */
    public BatchState getBatchState() {
        long afterLastCommit = System.currentTimeMillis() - startTimeMs;
        // 行数够多少条
        boolean isSizeEnough = size >= maxRecords;
        if (isSizeEnough) {
            return BatchState.SizeEnough;
        }
        // 大小够多少条
        boolean isByteSizeEnough = byteSize >= maxByteSize;
        if (isByteSizeEnough) {
            return BatchState.ByteSizeEnough;
        }
        boolean isTimeWaitEnough = startTimeMs > -1 && afterLastCommit >= maxWaitTime;
        if (isTimeWaitEnough) {
            return BatchState.TimeWaitEnough;
        }
        boolean isEarlyCommit = false;
        // 当已经凑够2的指数时
        if (size > 0 && (size & (size - 1)) == 0) {
            // 已经过去了maxWaitTime 40%的时间，统计上来说，不能再翻倍，那就提早commit
            boolean timeCondition = startTimeMs > -1 && afterLastCommit * 5 > maxWaitTime * 2;
            if (timeCondition) {
                return BatchState.TimeCondition;
            }
            // 当前行数的数据已经超过40%maxByteSize，可能不能再翻倍，那就提早commit
            boolean byteSizeCondition = byteSize * 5 > maxByteSize * 2;
            if (byteSizeCondition) {
                return BatchState.ByteSizeCondition;
            }
            // 当当前的行数的数据超过1/4的剩余avaliable
            long availableByteSize = pool.getAvailableByteSize();
            boolean totalByteSizeCondition = byteSize * shardCount > availableByteSize;
            if (totalByteSizeCondition) {
                return BatchState.TotalByteSizeCondition;
            }
            isEarlyCommit = timeCondition || byteSizeCondition || totalByteSizeCondition;
            if (isEarlyCommit) {
                if (timeCondition) {
                    LOGGER.debug(
                            "table {} earlyCommit[timeCondition].afterLastCommit({}) > 40% maxWaitTime({})",
                            afterLastCommit, maxWaitTime);
                } else if (byteSizeCondition) {
                    LOGGER.debug(
                            "table {} earlyCommit[byteSizeCondition].byteSize({}) > 40% maxByteSize({})",
                            byteSize, maxByteSize);
                } else {
                    LOGGER.debug(
                            "table {} earlyCommit[totalByteSizeCondition].afterLastCommit({}) > 40% availableByteSize({})",
                            afterLastCommit, maxWaitTime);
                }
            }
        }
        return BatchState.NotEnough;
        // return isSizeEnough || isByteSizeEnough || isTimeWaitEnough || isEarlyCommit;
    }

    public int size() {
        return size;
    }

    public long getByteSize() {
        return byteSize;
    }

    /** @return 永远是先给delete，再给upsert */
    public List<Record> getRecords() {
        List<Record> list = new ArrayList<>();
        list.addAll(deleteRecords.values());
        list.addAll(records.values());
        return list;
    }

    public OnConflictAction getOnConflictAction() {
        return action;
    }

    public void clear() {
        startTimeMs = -1;
        size = 0;
        byteSize = 0L;
        records.clear();
        deleteRecords.clear();
    }
}
