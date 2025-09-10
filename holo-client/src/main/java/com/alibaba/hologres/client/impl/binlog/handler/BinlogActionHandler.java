/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.binlog.handler;

import com.alibaba.blink.dataformat.BinaryRow;
import com.alibaba.blink.dataformat.BinaryRowWriter;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Subscribe;
import com.alibaba.hologres.client.Version;
import com.alibaba.hologres.client.auth.AKv4AuthenticationPlugin;
import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.ExceptionToReadableWarning;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.binlog.ArrayBuffer;
import com.alibaba.hologres.client.impl.binlog.BinlogEventType;
import com.alibaba.hologres.client.impl.binlog.BinlogRecordCollector;
import com.alibaba.hologres.client.impl.binlog.HoloBinlogDecoder;
import com.alibaba.hologres.client.impl.binlog.action.BinlogAction;
import com.alibaba.hologres.client.impl.handler.ActionHandler;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.binlog.BinlogHeartBeatRecord;
import com.alibaba.hologres.client.model.binlog.BinlogRecord;
import com.alibaba.hologres.client.utils.CommonUtil;
import com.alibaba.hologres.client.utils.Tuple;
import com.alibaba.niagara.client.table.ServiceContractMsg;
import com.google.protobuf.ByteString;
import org.postgresql.PGProperty;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Base64;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.hologres.client.copy.in.binaryrow.RecordBinaryRowOutputStream.convertToBinaryRow;

/** binlog消费的处理类. 因为binlog消费的特殊性，就抛弃ConnectionHolder，单独跑了. */
public class BinlogActionHandler extends ActionHandler<BinlogAction> {

    public static final Logger LOG = LoggerFactory.getLogger(BinlogActionHandler.class);

    final Properties originalInfo;
    final String originalUrl;
    final boolean isFixed;
    final int binlogReadBatchSize;
    final int maxRetryCount;
    final boolean binlogIgnoreBeforeUpdate;
    final boolean binlogIgnoreDelete;
    final boolean isEnableDirectConnection;
    final long binlogHeartBeatIntervalMs;
    final AtomicBoolean started;
    final ArrayBuffer<BinlogRecord> binlogRecordArray;
    int retryCount;
    long connectionMaxAliveMs;

    public BinlogActionHandler(
            AtomicBoolean started, HoloConfig config, boolean isShadingEnv, boolean isFixed) {
        super(config);
        this.started = started;
        this.originalInfo = new Properties();
        String username = config.getUsername();
        if (config.isUseAKv4() && !username.startsWith("BASIC$")) {
            username = AKv4AuthenticationPlugin.AKV4_PREFIX + username;
        }
        PGProperty.USER.set(originalInfo, username);
        PGProperty.PASSWORD.set(originalInfo, config.getPassword());
        if (config.isUseAKv4()) {
            PGProperty.AUTHENTICATION_PLUGIN_CLASS_NAME.set(
                    originalInfo, AKv4AuthenticationPlugin.class.getName());
            if (config.getRegion() != null) {
                originalInfo.setProperty(AKv4AuthenticationPlugin.REGION, config.getRegion());
            }
        }
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(originalInfo, "9.4");
        PGProperty.APPLICATION_NAME.set(
                originalInfo, Version.version + "_replication_" + config.getAppName());
        PGProperty.REPLICATION.set(originalInfo, "database");
        PGProperty.SOCKET_TIMEOUT.set(originalInfo, 360);
        PGProperty.LOGIN_TIMEOUT.set(originalInfo, 60);
        String jdbcUrl = config.getJdbcUrl();
        if (isShadingEnv) {
            if (jdbcUrl.startsWith("jdbc:postgresql:")) {
                jdbcUrl = "jdbc:hologres:" + jdbcUrl.substring("jdbc:postgresql:".length());
            }
        }
        // 这里的判断不使用config.isUseFixedFe()，而是使用来自调用者的isFixed，因为可能存在一种情况：虽然config文件中标记使用fixed
        // fe，但是当前Worker没有执行在fixedPool上
        this.isFixed = isFixed;
        this.originalUrl = jdbcUrl;
        this.binlogReadBatchSize = config.getBinlogReadBatchSize();
        this.maxRetryCount = config.getRetryCount();
        this.binlogIgnoreBeforeUpdate = config.getBinlogIgnoreBeforeUpdate();
        this.binlogIgnoreDelete = config.getBinlogIgnoreDelete();
        this.binlogHeartBeatIntervalMs = config.getBinlogHeartBeatIntervalMs();
        this.binlogRecordArray = new ArrayBuffer<>(binlogReadBatchSize, BinlogRecord[].class);
        this.isEnableDirectConnection = config.isEnableDirectConnection();
        this.connectionMaxAliveMs =
                CommonUtil.randomConnectionMaxAliveMs(config.getConnectionMaxAliveMs());
    }

    @Override
    public void handle(BinlogAction action) {
        doHandle(action);
    }

    class ConnectionContext {
        PgConnection conn = null;
        PGReplicationStream pgReplicationStream = null;
        private long connCreateTs;
        private final BinlogAction action;
        private long startLsn;
        private String startTime;
        private long timestamp;

        public ConnectionContext(BinlogAction action, long emittedLsn, String startTime) {
            this.action = action;
            this.startLsn = emittedLsn;
            this.startTime = startTime;
            this.timestamp = -1;
        }

        public void setEmittedLsn(long emittedLsn, long timestamp) {
            this.startLsn = emittedLsn;
            this.timestamp = timestamp;
            startTime = null;
        }

        public void updateTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public long getTimestamp() {
            return this.timestamp;
        }

        public void init() throws SQLException {
            try {
                String url = originalUrl;
                Properties info = new Properties();
                info.putAll(originalInfo);
                if (isEnableDirectConnection) {
                    url = ConnectionUtil.getDirectConnectionUrl(originalUrl, originalInfo, isFixed);
                }
                if (isFixed) {
                    url = ConnectionUtil.generateFixedUrl(url);
                }
                this.conn = DriverManager.getConnection(url, info).unwrap(PgConnection.class);
                ChainedLogicalStreamBuilder logicalStreamBuilder =
                        this.conn
                                .getReplicationAPI()
                                .replicationStream()
                                .logical()
                                .withSlotOption("parallel_index", action.getShardId())
                                .withSlotOption("batch_size", binlogReadBatchSize)
                                .withStatusInterval(10, TimeUnit.SECONDS);
                if (action.getSlotName() != null) {
                    logicalStreamBuilder.withSlotName(action.getSlotName());
                } else {
                    logicalStreamBuilder.withSlotOption("table_name", action.getTableName());
                }
                if (action.getConsumerGroup() != null) {
                    logicalStreamBuilder.withSlotOption(
                            "consumer_group", action.getConsumerGroup());
                }
                if (startLsn > -1) {
                    logicalStreamBuilder.withSlotOption("start_lsn", String.valueOf(startLsn));
                }
                if (startTime != null) {
                    logicalStreamBuilder.withSlotOption("start_time", startTime);
                }
                if (action.isEnableCompression()) {
                    logicalStreamBuilder.withSlotOption("enable_compression", true);
                }
                if (action.getProjectionColumnNames() != null
                        && action.getProjectionColumnNames().length > 0) {
                    String projectionColumnNames =
                            CommonUtil.encodeColumnNamesToString(action.getProjectionColumnNames());
                    logicalStreamBuilder.withSlotOption(
                            "projection_column_names", projectionColumnNames);
                    LOG.info("The projection_column_names are {}", projectionColumnNames);
                }
                if (action.getBinlogFilters() != null && !action.getBinlogFilters().isEmpty()) {
                    ServiceContractMsg.BinlogFilters.Builder filtersBuilder =
                            ServiceContractMsg.BinlogFilters.newBuilder();
                    for (int i = 0; i < action.getBinlogFilters().size(); i++) {
                        Subscribe.BinlogFilter binlogFilter = action.getBinlogFilters().get(i);
                        BinaryRow row = new BinaryRow(1);
                        BinaryRowWriter w = new BinaryRowWriter(row);
                        try {
                            convertToBinaryRow(
                                    w,
                                    0,
                                    binlogFilter.value,
                                    binlogFilter.column,
                                    conn.getTimestampUtils());
                        } catch (IOException e) {
                            throw new SQLException(
                                    "Failed to convert to binary row",
                                    PSQLState.INVALID_PARAMETER_VALUE.getState(),
                                    e);
                        }
                        w.complete();
                        byte[] data = row.serializeToBytes();
                        ServiceContractMsg.BinlogFilter filter =
                                ServiceContractMsg.BinlogFilter.newBuilder()
                                        .setColumnName(
                                                ByteString.copyFromUtf8(
                                                        binlogFilter.column.getName()))
                                        .setOpType(binlogFilter.op)
                                        .setValue(ByteString.copyFrom(data))
                                        .build();
                        filtersBuilder.addFilters(filter);
                    }
                    ServiceContractMsg.BinlogFilters binlogFilters = filtersBuilder.build();
                    byte[] serializedBytes = binlogFilters.toByteArray();
                    String filterString = Base64.getEncoder().encodeToString(serializedBytes);
                    logicalStreamBuilder.withSlotOption("filter_pb_str", filterString);
                }

                if (action.getLogicalPartitionValues() != null) {
                    logicalStreamBuilder.withSlotOption(
                            "logical_partition_column_names",
                            action.getLogicalPartitionColumnNames());
                    LOG.info(
                            "The logical_partition_column_names are {}",
                            action.getLogicalPartitionColumnNames());
                    logicalStreamBuilder.withSlotOption(
                            "logical_partition_values", action.getLogicalPartitionValues());
                    LOG.info(
                            "The logical_partition_values are {}",
                            action.getLogicalPartitionValues());
                }
                LOG.info(
                        "Connected to url {}, table {} shard {} start, start_lsn={}, start_time={}",
                        url,
                        action.getTableName(),
                        action.getShardId(),
                        startLsn,
                        startTime);
                this.pgReplicationStream = logicalStreamBuilder.start();
                this.connCreateTs = System.currentTimeMillis();
            } catch (SQLException e) {
                close();
                throw e;
            }
        }

        public boolean isInit() {
            return conn != null;
        }

        public void close() {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ignore) {

                }
                this.conn = null;
            }
            this.pgReplicationStream = null;
        }
    }

    private void resetRetryCount() {
        this.retryCount = this.maxRetryCount;
    }

    private void doHandle(BinlogAction action) {
        ConnectionContext connContext =
                new ConnectionContext(action, action.getLsn(), action.getTimestamp());
        HoloBinlogDecoder decoder = null;
        try {
            decoder =
                    new HoloBinlogDecoder(
                            action.getSupplier(),
                            action.getProjectionColumnNames(),
                            action.isEnableCompression(),
                            this.binlogIgnoreDelete,
                            this.binlogIgnoreBeforeUpdate);
        } catch (HoloClientException e) {
            action.getCollector().exceptionally(action.getShardId(), e);
            return;
        }

        resetRetryCount();
        while (started.get()) {
            try {
                connContext.init();
                fetch(
                        action.getShardId(),
                        action.getCollector(),
                        connContext,
                        decoder,
                        action.getCommitJob());
            } catch (SQLException e) {
                if (--retryCount < 1) {
                    // 失败太多了，结束
                    action.getCollector().exceptionally(action.getShardId(), e);
                    break;
                } else {
                    ExceptionToReadableWarning.readableWarn(
                            LOG,
                            String.format(
                                    "shardId %s binlog read fail, retry %s times",
                                    action.getShardId(), maxRetryCount - retryCount),
                            e);
                    continue;
                }
            } catch (HoloClientException | InterruptedException e) {
                // 这种错误基本没救了，撤
                action.getCollector().exceptionally(action.getShardId(), e);
                break;
            } catch (Throwable e) {
                action.getCollector().exceptionally(action.getShardId(), e);
                throw e;
            } finally {
                connContext.close();
                decoder.trace();
            }
        }
    }

    private void fetch(
            int shardId,
            BinlogRecordCollector collector,
            ConnectionContext connContext,
            HoloBinlogDecoder decoder,
            Queue<Tuple<CompletableFuture<Void>, Long>> commitJob)
            throws SQLException, HoloClientException, InterruptedException {
        // Replication Connection 不能执行其他sql，因此单独创建 Replication Connection.
        while (started.get()) {
            if (!connContext.isInit()) {
                connContext.init();
            }
            tryFlush(connContext, commitJob);
            if (binlogRecordArray.isReadable()) {
                while (started.get() && binlogRecordArray.remain() > 0) {
                    tryFlush(connContext, commitJob);
                    collector.emit(shardId, binlogRecordArray);
                }
            }
            ByteBuffer byteBuffer = connContext.pgReplicationStream.read();
            binlogRecordArray.beginWrite();
            decoder.decode(shardId, byteBuffer, binlogRecordArray);
            binlogRecordArray.beginRead();
            // 如果成功消费了重置重试次数
            resetRetryCount();
            if (binlogRecordArray.remain() == 0) {
                if (binlogHeartBeatIntervalMs > -1) {
                    long current = System.currentTimeMillis();
                    if (current - connContext.getTimestamp() > binlogHeartBeatIntervalMs) {
                        connContext.updateTimestamp(current);
                        BinlogHeartBeatRecord record =
                                new BinlogHeartBeatRecord(
                                        decoder.getSchema(),
                                        connContext.startLsn,
                                        BinlogEventType.HeartBeat,
                                        current * 1000L);
                        record.setShardId(shardId);
                        binlogRecordArray.beginWrite();
                        binlogRecordArray.add(record);
                        binlogRecordArray.beginRead();
                    }
                }
            } else {
                BinlogRecord lastRecord = binlogRecordArray.last();
                connContext.setEmittedLsn(
                        lastRecord.getBinlogLsn(), lastRecord.getBinlogTimestamp() / 1000L);
            }
            while (started.get() && binlogRecordArray.remain() > 0) {
                tryFlush(connContext, commitJob);
                collector.emit(shardId, binlogRecordArray);
            }
            if (System.currentTimeMillis() - connContext.connCreateTs > connectionMaxAliveMs) {
                LOG.info("close binlog connection due to max alive time exceeded.");
                connContext.close();
            }
        }
    }

    private void tryFlush(
            ConnectionContext connContext, Queue<Tuple<CompletableFuture<Void>, Long>> commitJob)
            throws SQLException {
        // 用户不传入slotName和commit job，没必要flush
        if (null == commitJob) {
            return;
        }

        Tuple<CompletableFuture<Void>, Long> job = commitJob.poll();
        if (job == null) {
            return;
        }
        int flushRetryCount = maxRetryCount;
        boolean done = false;
        try {
            while (!done && --flushRetryCount > 0) {
                try {
                    if (!connContext.isInit()) {
                        connContext.init();
                    }
                    connContext.pgReplicationStream.setFlushedLSN(LogSequenceNumber.valueOf(job.r));
                    connContext.pgReplicationStream.forceUpdateStatus();
                    job.l.complete(null);
                    done = true;
                } catch (SQLException e) {
                    if (flushRetryCount > 0) {
                        connContext.close();
                    } else {
                        throw e;
                    }
                }
            }
        } catch (SQLException e) {
            job.l.completeExceptionally(e);
            throw e;
        } finally {
            if (!job.l.isDone()) {
                job.l.completeExceptionally(
                        new HoloClientException(
                                ExceptionCode.INTERNAL_ERROR,
                                "unknown exception when flush binlog lsn"));
            }
        }
    }

    @Override
    public String getCostMsMetricName() {
        // Binlog action没必要记录action cost，本来就是一次性的.
        return null;
    }
}
