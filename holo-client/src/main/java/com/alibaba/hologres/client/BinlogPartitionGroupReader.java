package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.binlog.BinlogEventType;
import com.alibaba.hologres.client.impl.binlog.BinlogOffset;
import com.alibaba.hologres.client.impl.binlog.BinlogRecordCollector;
import com.alibaba.hologres.client.impl.binlog.TableSchemaSupplier;
import com.alibaba.hologres.client.impl.binlog.action.BinlogAction;
import com.alibaba.hologres.client.model.AutoPartitioning;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.binlog.BinlogPartitionSubscribeMode;
import com.alibaba.hologres.client.model.binlog.BinlogRecord;
import com.alibaba.hologres.client.utils.PartitionUtil;
import com.alibaba.hologres.client.utils.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/** BinlogPartitionGroupReader，启动后台线程将每个分区reader返回的BinlogRecord放入queue. */
public class BinlogPartitionGroupReader implements Closeable {
    public static final Logger LOGGER = LoggerFactory.getLogger(BinlogPartitionGroupReader.class);

    private final HoloConfig config;
    private final HoloClient client;
    private final AtomicBoolean started;
    private final Set<Integer> shards;
    private final TableSchema parentSchema;
    private final AutoPartitioning autoPartitioningInfo;
    private final Subscribe parentSubscribe;

    /** 消费状态. */
    // 目前已经消费的最新的分区对应的起始时间
    private long latestPartitionStartTimeStamp = -1;
    // 子表的消费状态,包含已经消费完成的表
    private final Map<TableName, PartitionSubscribeContext> partitionToContext;
    // 当前正在消费的子表的BinlogShardGroupReader,
    private final Map<TableName, BinlogShardGroupReader> partitionToReader;

    BlockingQueue<BinlogRecord> queue;
    BinlogRecordCollector collector;

    public BinlogPartitionGroupReader(
            HoloConfig config,
            Subscribe parentSubscribe,
            AtomicBoolean started,
            TableSchema parentSchema,
            HoloClient client,
            BinlogRecordCollector collector) {
        this.config = config;
        this.started = started;
        this.client = client;
        this.parentSubscribe = parentSubscribe;
        this.shards = parentSubscribe.getOffsetMap().keySet();
        this.parentSchema = parentSchema;
        this.partitionToContext = new HashMap<>();
        this.partitionToReader = new HashMap<>();
        this.autoPartitioningInfo = parentSchema.getAutoPartitioning();
        this.collector = collector;
        this.queue = collector.getQueue();
    }

    public BinlogRecord getBinlogRecord()
            throws HoloClientException, InterruptedException, TimeoutException {
        return getBinlogRecord(config.getBinlogReadTimeoutMs());
    }

    public BinlogRecord getBinlogRecord(long timeoutMs)
            throws HoloClientException, InterruptedException, TimeoutException {
        if (null != collector.getException()) {
            throw collector.getException();
        }
        BinlogRecord r = null;

        long target = timeoutMs > 0 ? (System.nanoTime() + timeoutMs * 1000000L) : Long.MAX_VALUE;
        while (r == null) {
            tryFetch(target);
            if (buffer.size() > bufferPosition) {
                r = buffer.get(bufferPosition++);
            }
            if (r != null) {
                if (config.getBinlogPartitionSubscribeMode()
                        == BinlogPartitionSubscribeMode.DYNAMIC) {
                    slidingNextPartitionIfNeed(r);
                }
                if ((r.getBinlogEventType() == BinlogEventType.DELETE
                                && config.getBinlogIgnoreDelete())
                        || (r.getBinlogEventType() == BinlogEventType.BEFORE_UPDATE
                                && config.getBinlogIgnoreBeforeUpdate())) {
                    r = null;
                }
            }
        }
        return r;
    }

    int bufferPosition = 0;
    List<BinlogRecord> buffer = new ArrayList<>();

    // 当buffer消费完了就拿一批回来
    private void tryFetch(long target)
            throws InterruptedException, TimeoutException, HoloClientException {
        if (buffer.size() <= bufferPosition) {
            if (buffer.size() > 0) {
                buffer.clear();
            }
            BinlogRecord r = null;
            while (r == null) {
                if (null != collector.getException()) {
                    throw collector.getException();
                }
                if (System.nanoTime() > target) {
                    throw new TimeoutException(
                            "Binlog record fetch timed out. This may occur if the job has experienced backpressure, causing the consumer thread to be unable to read the binlog for a long time."
                                    + " Consider resolving the job's backpressure issue or increasing the timeout period.");
                }
                r = queue.poll(1000, TimeUnit.MILLISECONDS);
                if (r != null) {
                    buffer.add(r);
                    queue.drainTo(buffer);
                    bufferPosition = 0;
                }
            }
        }
    }

    /**
     * 判断record所属分区是否需要结束读取,以及是否需要滑动到下一个分区继续读取. 通过当前时间计算出目前最新的分区 1 BinlogRecord来自最新分区, 不需要滑动, 直接返回 2
     * BinlogRecord来自之前的分区, 判断对应的shard是否消费完成, 所有shard消费完成则开始下一个分区的消费 2.1 下一个需要消费的分区与最新分区相同,
     * 表示新的时间单元到来, 直接启动消费, 不需要等上一个分区消费完成.
     *
     * @param binlogRecord 将要返回给用户的BinlogRecord
     */
    private void slidingNextPartitionIfNeed(BinlogRecord binlogRecord) throws HoloClientException {
        TableName tableNameOfRecord = binlogRecord.getTableName();
        // record所属的分区已经关闭, 迟到的record直接忽略
        if (!partitionToContext.containsKey(tableNameOfRecord)) {
            return;
        }
        // 当前正在消费的分区的结束时间, 也是下个分区的开始时间
        long nextPartitionStartTimeStamp =
                partitionToContext.get(tableNameOfRecord).binlogReadEndTimeStamp;

        // 当前时间
        ZonedDateTime now = ZonedDateTime.now(autoPartitioningInfo.getTimeZoneId());
        // 当前时间所处分区的起始时间
        long nowPartitionStartTimestamp =
                PartitionUtil.getPartitionUnitDateTimeRange(
                                PartitionUtil.getPartitionSuffixByDateTime(
                                        now, autoPartitioningInfo),
                                autoPartitioningInfo)
                        .l
                        .toInstant()
                        .toEpochMilli();

        if (nowPartitionStartTimestamp < nextPartitionStartTimeStamp) {
            // 目前正在消费最新的分区
            return;
        } else {
            // record属于历史分区表

            // 判断BinlogRecord对应的子表和shard是否消费完成, 所有shard都消费完成, 停止对应分区的消费并开始消费下一个分区
            if (checkIfSubscribeFinish(binlogRecord)) {
                String nextPartitionTableName =
                        PartitionUtil.getNextPartitionTableName(
                                tableNameOfRecord.getTableName(), autoPartitioningInfo);
                TableName nextPartitionName =
                        TableName.quoteValueOf(
                                parentSchema.getSchemaName(), nextPartitionTableName);
                stopPartitionSubscribe(tableNameOfRecord);
                Subscribe nextPartitionSubscribe = createPartitionSubscribe(nextPartitionName);
                startPartitionSubscribe(nextPartitionName, nextPartitionSubscribe);
            }

            // 当前消费的是次新的分区,在新的时间单元到来时,启动最新分区的消费
            if (nowPartitionStartTimestamp == nextPartitionStartTimeStamp
                    && nowPartitionStartTimestamp != latestPartitionStartTimeStamp) {
                // 当前时间对应的分区, 即需要消费的最新分区
                String nowPartitionTableName =
                        PartitionUtil.getPartitionNameByDateTime(
                                parentSchema.getTableName(), now, autoPartitioningInfo);
                TableName nowPartitionName =
                        TableName.quoteValueOf(parentSchema.getSchemaName(), nowPartitionTableName);
                Subscribe nextPartitionSubscribe = createPartitionSubscribe(nowPartitionName);
                startPartitionSubscribe(nowPartitionName, nextPartitionSubscribe);
            }
        }
    }

    /**
     * 启动对某个分区的消费. 分区订阅请求保存在partitionToSubscribe中 为相应分区创建BinlogShardGroupReader,
     * 保存在partitionToReader中 启动收集BinlogShardGroupReader结果的线程
     *
     * @param partitionName 分区表名
     * @param partitionSubscribe 分区表的订阅请求
     */
    public void startPartitionSubscribe(TableName partitionName, Subscribe partitionSubscribe)
            throws HoloClientException {
        // 只有DYNAMIC模式模式下, 才需要设置每张子表的时间范围
        if (config.getBinlogPartitionSubscribeMode() == BinlogPartitionSubscribeMode.DYNAMIC) {
            String partitionSuffix =
                    PartitionUtil.extractTimePartFromTableName(
                            partitionName.getTableName(), autoPartitioningInfo);
            Tuple<ZonedDateTime, ZonedDateTime> partitionTimeRange =
                    PartitionUtil.getPartitionUnitDateTimeRange(
                            partitionSuffix, autoPartitioningInfo);
            PartitionSubscribeContext context =
                    new PartitionSubscribeContext(partitionTimeRange.l, partitionTimeRange.r);
            if (context.binlogReadStartTimeStamp <= latestPartitionStartTimeStamp) {
                // 已经启动消费了
                return;
            } else {
                latestPartitionStartTimeStamp = context.binlogReadStartTimeStamp;
            }
            partitionToContext.put(partitionName, context);
            LOGGER.info(
                    "start subscribe for partition {}, time range [{} - {}], offsets {}.",
                    partitionName.getFullName(),
                    context.binlogReadStartTime,
                    context.binlogReadEndTime,
                    partitionSubscribe.getOffsetMap());

        } else {
            partitionToContext.put(partitionName, new PartitionSubscribeContext());
            LOGGER.info(
                    "start subscribe for partition {}, offsets {}.",
                    partitionName.getFullName(),
                    partitionSubscribe.getOffsetMap());
        }
        BinlogShardGroupReader reader = binlogSubscribe(partitionSubscribe);
        partitionToReader.put(partitionName, reader);
    }

    /**
     * 停止对某个分区的消费. 停止收集BinlogShardGroupReader结果的线程 关闭BinlogShardGroupReader本身
     *
     * @param partitionName 分区表名
     */
    private void stopPartitionSubscribe(TableName partitionName) {
        LOGGER.info(
                "subscribe for partition {} all shards finished, close corresponding reader",
                partitionName.getFullName());
        partitionToContext.remove(partitionName);
        BinlogShardGroupReader reader = partitionToReader.remove(partitionName);
        if (reader != null) {
            if (!reader.isCanceled()) {
                reader.cancel();
            }
        }
    }

    /** 获取当前正在消费的分区名. 主要用于测试 对STATIC模式,分区数量应该与创建时保持一致 对DYNAMIC模式,分区数量应该小于等于2(只有新的时间单元到来时, 才会等于2) */
    public Set<TableName> getCurrentPartitionsInSubscribe() {
        return partitionToReader.keySet();
    }

    /**
     * 检查binlogRecord对应的分区和shard是否消费完成, 如果所有shard都消费完成进行标记. 分区的订阅结束时间 = 分区表分区值的结束时间 + 允许迟到时间 1.
     * 判断当前时间是否已经晚于相应分区的订阅结束时间 2. 如果满足条件1,
     * 通过hg_get_binlog_cursor('LATEST')获取每个shard的最后一条lsn,记录在LatestBinlogLsnMap中 3.
     * 判断当前Record的lsn是否已经超过相应shard的结束lsn, 满足表示此shard消费结束, 相应shard也从LatestBinlogLsnMap移除 4.
     * 所有shard都消费完成,此时LatestBinlogLsnMap为空,标记相应分区已经彻底消费结束
     *
     * @param binlogRecord Binlog记录对象
     */
    private boolean checkIfSubscribeFinish(BinlogRecord binlogRecord) throws HoloClientException {
        TableName tableNameOfRecord = binlogRecord.getTableName();
        // 子表还没开始消费
        if (!partitionToContext.containsKey(tableNameOfRecord)) {
            throw new HoloClientException(
                    ExceptionCode.INTERNAL_ERROR,
                    String.format(
                            "subscribe of partition %s has not started.",
                            tableNameOfRecord.getFullName()));
        }
        PartitionSubscribeContext context = partitionToContext.get(tableNameOfRecord);
        int shardId = binlogRecord.getShardId();
        long lsn = binlogRecord.getBinlogLsn();

        long currentTimeStamp = System.currentTimeMillis();
        // 当前时间已经超过订阅的结束时间(分区表分区值的结束时间 + 允许迟到时间)
        if (currentTimeStamp
                >= context.binlogReadEndTimeStamp
                        + config.getBinlogPartitionLatenessTimeoutSecond() * 1000L) {
            // latestBinlogLsnMap首次为空,表示还未获取每个shard的最后一条lsn
            if (context.shardToLatestLsn.isEmpty()) {
                context.shardToLatestLsn.putAll(
                        Command.getLatestBinlogLsn(client, tableNameOfRecord, shards));
                LOGGER.info(
                        "set latestBinlogLsnMap for partition {} map {}",
                        tableNameOfRecord.getFullName(),
                        context.shardToLatestLsn);
            }
            if (context.finishedShards.contains(shardId)) {
                // 当前shard已经确定关闭,晚到的数据直接丢弃(heartbeat record)
                return false;
            }
            // 此分区表的当前shard已经消费到最新
            // 读到的record的lsn为-1(一定是HeartBeatRecord),且LatestBinlog是0,表示此shard上完全没有binlog,也关闭消费.
            if (lsn >= context.shardToLatestLsn.get(shardId)
                    || (context.shardToLatestLsn.get(shardId) == 0 && lsn == -1)) {
                LOGGER.info(
                        "subscribe for partition {} shard {} finished, latest lsn {}",
                        tableNameOfRecord.getFullName(),
                        shardId,
                        lsn);
                // 每个shard消费完成时从latestBinlogLsnMap中移除
                context.finishedShards.add(shardId);
            }
            // 所有shard都已经消费结束
            if (context.isFinished()) {
                // 所有shard都已经消费结束,关闭消费
                LOGGER.info(
                        "subscribe for partition {} all shards finished",
                        tableNameOfRecord.getFullName());
                return true;
            }
        }
        return false;
    }

    /** 创建一个默认的子表订阅对象. 订阅对象中,每个shard使用默认的构造函数指定BinlogOffset,表示从子表的最早binlog开始消费 */
    private Subscribe createPartitionSubscribe(TableName partitionName) {
        Subscribe.OffsetBuilder offsetBuilder =
                Subscribe.newOffsetBuilder(partitionName.getFullName())
                        .setEnableCompression(parentSubscribe.isEnableCompression())
                        .addProjectionColumnNamesToSubscribe(
                                parentSubscribe.getProjectionColumnNamesToSubscribe())
                        .setBinlogFilters(parentSubscribe.getBinlogFilters());
        shards.forEach(i -> offsetBuilder.addShardStartOffset(i, new BinlogOffset()));
        return offsetBuilder.build();
    }

    /**
     * 订阅子表. 与HoloClient中消费普通表的binlogSubscribe方法基本相同，但会复用同一个Collector.
     *
     * @param subscribe 子表订阅对象
     */
    private BinlogShardGroupReader binlogSubscribe(Subscribe subscribe) throws HoloClientException {
        TableSchemaSupplier supplier = () -> client.getTableSchema(subscribe.getTableName(), true);
        Map<Integer, BinlogOffset> offsetMap = subscribe.getOffsetMap();
        BinlogShardGroupReader reader = null;
        try {
            AtomicBoolean started = new AtomicBoolean(true);
            reader = new BinlogShardGroupReader(config, subscribe, started, collector);
            for (Map.Entry<Integer, BinlogOffset> entry : offsetMap.entrySet()) {
                BinlogAction action =
                        new BinlogAction(
                                subscribe.getTableName(),
                                subscribe.getConsumerGroup(),
                                entry.getKey(),
                                entry.getValue().getSequence(),
                                entry.getValue().getStartTimeText(),
                                subscribe.isEnableCompression(),
                                reader.getCollector(),
                                supplier,
                                subscribe.getProjectionColumnNamesToSubscribe(),
                                subscribe.getBinlogFilters());
                reader.addThread(
                        client.getExecPool()
                                .submitOneShotAction(
                                        started,
                                        String.format(
                                                "binlog-%s-%s",
                                                subscribe.getTableName(), entry.getKey()),
                                        action));
            }
        } catch (HoloClientException e) {
            reader.close();
            throw e;
        }
        return reader;
    }

    @Override
    public void close() {
        cancel();
    }

    public void cancel() {
        started.set(false);
        for (BinlogShardGroupReader reader : partitionToReader.values()) {
            if (reader != null && !reader.isCanceled()) {
                reader.cancel();
            }
        }
    }

    public boolean isCanceled() {
        return !started.get();
    }

    /**
     * 记录每个分区表的订阅状态. 分区值对应的时间范围, 比如 20240902 对应的范围是[2024-09-02 00:00:00, 2024-09-03 00:00:00)
     * 每个shard需要消费的最后一条lsn 每个shard是否消费结束
     */
    static class PartitionSubscribeContext {
        // 分区子表的订阅开始时间
        final ZonedDateTime binlogReadStartTime;
        final long binlogReadStartTimeStamp;
        // 分区子表的订阅结束时间
        final ZonedDateTime binlogReadEndTime;
        final long binlogReadEndTimeStamp;

        PartitionSubscribeContext(
                ZonedDateTime binlogReadStartTime, ZonedDateTime binlogReadEndTime) {
            this.binlogReadStartTime = binlogReadStartTime;
            this.binlogReadEndTime = binlogReadEndTime;
            this.binlogReadStartTimeStamp = binlogReadStartTime.toInstant().toEpochMilli();
            this.binlogReadEndTimeStamp = binlogReadEndTime.toInstant().toEpochMilli();
        }

        // STATIC模式, 不需要设置订阅的时间范围
        PartitionSubscribeContext() {
            this(ZonedDateTime.now(), ZonedDateTime.now());
        }

        // 分区子表每个shard需要消费的最后一条lsn
        Map<Integer, Long> shardToLatestLsn = new HashMap<>();
        // 结束的shard
        Set<Integer> finishedShards = new HashSet<>();

        boolean isFinished() {
            return !shardToLatestLsn.isEmpty() && finishedShards.size() == shardToLatestLsn.size();
        }
    }
}
