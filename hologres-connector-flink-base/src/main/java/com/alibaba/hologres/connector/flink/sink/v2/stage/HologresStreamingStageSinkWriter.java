package com.alibaba.hologres.connector.flink.sink.v2.stage;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * STREAMING 模式的 SinkWriter 实现，提供 at-least-once 语义.
 *
 * <p>每个 writer 维护一个 stage，包含该下游 partition 负责的互斥 shard 集合。seal 后按生成顺序由单线程异步 INSERT。pending INSERT
 * 达到上限时阻塞 write 并向上游反压。异步 INSERT 失败后仍由同一个 executor 重建连接并重试，重试期间立即阻塞 write；只有全部 INSERT 成功后
 * checkpoint 才能继续，避免 checkpoint 成功但 INSERT 尚未完成导致数据丢失。
 */
public class HologresStreamingStageSinkWriter extends AbstractStageSinkWriter {

    private static final Logger LOG =
            LoggerFactory.getLogger(HologresStreamingStageSinkWriter.class);
    static final int MAX_INSERT_RETRIES = 3;

    @FunctionalInterface
    interface InsertAttempt {
        void execute() throws Exception;
    }

    @FunctionalInterface
    interface RetryListener {
        void beforeRetry(int retryNumber, Exception failure);
    }

    enum StageStatus {
        SEALED,
        INSERTING,
        RETRYING,
        INSERTED,
        FAILED
    }

    static class StageMeta {
        final String stageName;
        final long recordCount;
        final List<Integer> targetShards;
        final long sequenceId;
        StageStatus status;

        StageMeta(
                String stageName,
                long recordCount,
                List<Integer> targetShards,
                long sequenceId,
                StageStatus status) {
            this.stageName = stageName;
            this.recordCount = recordCount;
            this.targetShards = targetShards;
            this.sequenceId = sequenceId;
            this.status = status;
        }
    }

    private final long commitIntervalMs;
    private final int maxFilesPerStage;
    private final long flushTimeoutMs;
    private final int maxPendingInserts;
    private final boolean checkpointAsyncCommitEnabled;
    private final boolean internalStageSerialReadEnabled;

    private final Object stageStateLock = new Object();
    private final Map<String, StageMeta> stageStatusMap = new HashMap<>();
    private final Deque<StageMeta> pendingInsertQueue = new ArrayDeque<>();
    private final Set<String> previousCheckpointStageNames = new HashSet<>();
    private final ExecutorService insertExecutor;

    private long nextSequenceId;
    private boolean drainActive;
    private boolean retrying;
    private volatile boolean shutdownRequested;
    private volatile long activeFlushDeadline = Long.MAX_VALUE;
    private volatile IOException terminalFailure;

    public HologresStreamingStageSinkWriter(
            HologresConnectionParam param,
            String qualifiedTableName,
            int subtaskId,
            int attemptNumber,
            int numTasks,
            int shardCount,
            boolean isBounded,
            RowType rowType,
            boolean internalStageSerialReadEnabled,
            Integer[] distributionKeyIndexes)
            throws IOException {
        this(
                param,
                qualifiedTableName,
                subtaskId,
                attemptNumber,
                numTasks,
                shardCount,
                isBounded,
                rowType,
                internalStageSerialReadEnabled,
                distributionKeyIndexes,
                Collections.emptyList());
    }

    public HologresStreamingStageSinkWriter(
            HologresConnectionParam param,
            String qualifiedTableName,
            int subtaskId,
            int attemptNumber,
            int numTasks,
            int shardCount,
            boolean isBounded,
            RowType rowType,
            boolean internalStageSerialReadEnabled,
            Integer[] distributionKeyIndexes,
            Collection<HologresStreamingStageWriterState> recoveredStates)
            throws IOException {
        super(
                param,
                qualifiedTableName,
                subtaskId,
                attemptNumber,
                numTasks,
                shardCount,
                isBounded,
                rowType,
                distributionKeyIndexes);

        this.commitIntervalMs = param.getStageCommitIntervalMs();
        this.maxFilesPerStage = param.getStageMaxFilesPerStage();
        this.flushTimeoutMs = param.getStageFlushTimeoutMs();
        this.maxPendingInserts = param.getStageMaxPendingInserts();
        this.checkpointAsyncCommitEnabled = param.isStageCheckpointAsyncCommitEnabled();
        if (flushTimeoutMs <= 0) {
            throw new IllegalArgumentException(
                    "sink.stage.flush-timeout-ms must be greater than 0");
        }
        if (maxPendingInserts <= 0) {
            throw new IllegalArgumentException(
                    "sink.stage.max-pending-inserts must be greater than 0");
        }
        this.internalStageSerialReadEnabled = internalStageSerialReadEnabled;
        this.insertExecutor =
                Executors.newSingleThreadExecutor(
                        r -> {
                            Thread t = new Thread(r, "holo-stage-inserter-" + subtaskId);
                            t.setDaemon(true);
                            return t;
                        });
        restorePendingStages(recoveredStates);

        LOG.info(
                "StreamingStageSinkWriter[subtask={}] auto-seal thresholds: "
                        + "commitInterval={}ms, maxFiles={}, flushTimeout={}ms, maxPendingInserts={}, "
                        + "checkpointAsyncCommitEnabled={}, internalStageSerialReadEnabled={}",
                subtaskId,
                commitIntervalMs,
                maxFilesPerStage,
                flushTimeoutMs,
                maxPendingInserts,
                checkpointAsyncCommitEnabled,
                internalStageSerialReadEnabled);
        if (checkpointAsyncCommitEnabled && param.getStageTtl() < commitIntervalMs / 1000L * 10L) {
            LOG.warn(
                    "StreamingStageSinkWriter[subtask={}] sink.stage.ttl={}s may be too small "
                            + "for checkpoint async commit mode; recovery can fail if pending Stage "
                            + "expires before replay. Recommended ttl should be much larger than "
                            + "the checkpoint interval.",
                    subtaskId,
                    param.getStageTtl());
        }
    }

    private void restorePendingStages(
            Collection<HologresStreamingStageWriterState> recoveredStates) {
        if (recoveredStates == null || recoveredStates.isEmpty()) {
            return;
        }
        List<HologresStreamingStageWriterState.PendingStage> recoveredStages = new ArrayList<>();
        long restoredNextSequenceId = 0;
        for (HologresStreamingStageWriterState state : recoveredStates) {
            recoveredStages.addAll(state.getPendingStages());
            restoredNextSequenceId = Math.max(restoredNextSequenceId, state.getNextSequenceId());
        }
        recoveredStages.sort(
                Comparator.comparingLong(
                        HologresStreamingStageWriterState.PendingStage::getSequenceId));
        synchronized (stageStateLock) {
            for (HologresStreamingStageWriterState.PendingStage recovered : recoveredStages) {
                if (stageStatusMap.containsKey(recovered.getStageName())) {
                    continue;
                }
                StageMeta meta =
                        new StageMeta(
                                recovered.getStageName(),
                                recovered.getRecordCount(),
                                recovered.getTargetShards(),
                                recovered.getSequenceId(),
                                StageStatus.SEALED);
                stageStatusMap.put(meta.stageName, meta);
                pendingInsertQueue.addLast(meta);
                previousCheckpointStageNames.add(meta.stageName);
                restoredNextSequenceId = Math.max(restoredNextSequenceId, meta.sequenceId + 1);
            }
            nextSequenceId = restoredNextSequenceId;
            startDrainIfNeeded();
        }
        LOG.info(
                "StreamingStageSinkWriter[subtask={}] restored {} pending Stages, nextSequenceId={}",
                subtaskId,
                recoveredStages.size(),
                nextSequenceId);
    }

    @Override
    protected String writerName() {
        return "StreamingStageSinkWriter";
    }

    @Override
    public void write(RowData element, Context context) throws IOException, InterruptedException {
        checkTerminalFailure();
        ensureRuntimeInsertCapacity();
        super.write(element, context);
        maybeAutoSealAndEnqueue();
    }

    private void maybeAutoSealAndEnqueue() throws IOException, InterruptedException {
        if (currentStage == null) {
            return;
        }
        long now = System.currentTimeMillis();
        int fileCount = currentStage.wrapper != null ? currentStage.wrapper.getFileIndex() : 0;
        boolean exceedFiles = fileCount >= maxFilesPerStage;
        boolean exceedTime = now - currentStage.startTime >= commitIntervalMs;
        if (exceedFiles || exceedTime) {
            ensureRuntimeInsertCapacity();
            sealAndEnqueueStage();
            try {
                openNewStage();
            } catch (Exception e) {
                throw new IOException("Failed to create a new Stage after auto seal", e);
            }
        }
    }

    private void sealAndEnqueueStage() throws IOException {
        SealedStage sealed = sealCurrentStage();
        if (sealed == null) {
            return;
        }

        StageMeta meta;
        synchronized (stageStateLock) {
            meta =
                    new StageMeta(
                            sealed.stageName,
                            sealed.recordCount,
                            sealed.targetShards,
                            nextSequenceId++,
                            StageStatus.SEALED);
            stageStatusMap.put(meta.stageName, meta);
            pendingInsertQueue.addLast(meta);
            startDrainIfNeeded();
        }
        LOG.info(
                "StreamingStageSinkWriter[subtask={}] Stage {} sealed, targetShards={}, seq={}, "
                        + "records={}",
                subtaskId,
                meta.stageName,
                meta.targetShards,
                meta.sequenceId,
                meta.recordCount);
    }

    /** 必须在 stageStateLock 内调用. */
    private void startDrainIfNeeded() {
        if (!drainActive && !pendingInsertQueue.isEmpty()) {
            drainActive = true;
            insertExecutor.submit(this::drainLoop);
        }
    }

    private void drainLoop() {
        while (true) {
            StageMeta meta;
            synchronized (stageStateLock) {
                if (shutdownRequested || pendingInsertQueue.isEmpty()) {
                    drainActive = false;
                    stageStateLock.notifyAll();
                    return;
                }
                meta = pendingInsertQueue.removeFirst();
                meta.status = StageStatus.INSERTING;
            }

            IOException insertFailure = executeInsertWithRetries(meta);
            synchronized (stageStateLock) {
                retrying = false;
                if (insertFailure == null) {
                    meta.status = StageStatus.INSERTED;
                } else {
                    meta.status = StageStatus.FAILED;
                    terminalFailure = insertFailure;
                }
                stageStateLock.notifyAll();
            }
            if (insertFailure != null) {
                LOG.error(
                        "StreamingStageSinkWriter[subtask={}] Stage {} exhausted asynchronous "
                                + "INSERT retries; the next write or checkpoint flush will trigger "
                                + "Flink failover",
                        subtaskId,
                        meta.stageName,
                        insertFailure);
                synchronized (stageStateLock) {
                    drainActive = false;
                    stageStateLock.notifyAll();
                }
                return;
            }
        }
    }

    private IOException executeInsertWithRetries(StageMeta meta) {
        long retryDeadline = System.currentTimeMillis() + flushTimeoutMs;
        int[] retryCount = {0};
        Exception firstFailure =
                executeWithRetries(
                        MAX_INSERT_RETRIES,
                        () -> {
                            if (shutdownRequested) {
                                throw new IOException(
                                        "Stage INSERT aborted while the writer is closing: "
                                                + meta.stageName);
                            }
                            executeInsertAttempt(meta, currentInsertTimeoutMs(retryDeadline));
                        },
                        (retryNumber, failure) -> {
                            retryCount[0] = retryNumber;
                            synchronized (stageStateLock) {
                                meta.status = StageStatus.RETRYING;
                                retrying = true;
                                stageStateLock.notifyAll();
                            }
                            LOG.warn(
                                    "StreamingStageSinkWriter[subtask={}] Stage {} INSERT failed; "
                                            + "rebuilding the connection for retry {}/{}, "
                                            + "remainingTimeoutMs={}",
                                    subtaskId,
                                    meta.stageName,
                                    retryNumber,
                                    MAX_INSERT_RETRIES,
                                    currentRemainingInsertTimeoutMsForLog(retryDeadline),
                                    failure);
                        });
        if (firstFailure == null) {
            if (retryCount[0] > 0) {
                LOG.info(
                        "StreamingStageSinkWriter[subtask={}] Stage {} succeeded on retry {}",
                        subtaskId,
                        meta.stageName,
                        retryCount[0]);
            }
            return null;
        }
        return new IOException(
                "Stage "
                        + meta.stageName
                        + " INSERT failed on the initial attempt and all "
                        + MAX_INSERT_RETRIES
                        + " retries",
                firstFailure);
    }

    static Exception executeWithRetries(
            int maxRetries, InsertAttempt attempt, RetryListener retryListener) {
        Exception firstFailure = null;
        for (int retryNumber = 0; retryNumber <= maxRetries; retryNumber++) {
            try {
                attempt.execute();
                return null;
            } catch (Exception e) {
                if (firstFailure == null) {
                    firstFailure = e;
                }
                if (retryNumber >= maxRetries) {
                    return firstFailure;
                }
                retryListener.beforeRetry(retryNumber + 1, e);
            }
        }
        return firstFailure;
    }

    void executeInsertAttempt(StageMeta meta, long queryTimeoutMs) throws Exception {
        StageHelper.executeInsert(
                param,
                qualifiedTableName,
                columnNames,
                Collections.singletonList(meta.stageName),
                meta.recordCount,
                meta.targetShards,
                internalStageSerialReadEnabled,
                queryTimeoutMs);
    }

    private long currentInsertTimeoutMs(long retryDeadline) throws IOException {
        return currentRemainingInsertTimeoutMs(retryDeadline);
    }

    private long currentRemainingInsertTimeoutMs(long retryDeadline) throws IOException {
        long flushDeadline = activeFlushDeadline;
        long effectiveDeadline =
                flushDeadline == Long.MAX_VALUE
                        ? retryDeadline
                        : Math.min(retryDeadline, flushDeadline);
        return Math.min(flushTimeoutMs, remainingFlushTime(effectiveDeadline));
    }

    private long currentRemainingInsertTimeoutMsForLog(long retryDeadline) {
        long flushDeadline = activeFlushDeadline;
        long effectiveDeadline =
                flushDeadline == Long.MAX_VALUE
                        ? retryDeadline
                        : Math.min(retryDeadline, flushDeadline);
        return Math.max(0, effectiveDeadline - System.currentTimeMillis());
    }

    private void ensureRuntimeInsertCapacity() throws IOException, InterruptedException {
        long deadline = System.currentTimeMillis() + flushTimeoutMs;
        boolean backpressureLogged = false;
        while (true) {
            synchronized (stageStateLock) {
                if (terminalFailure != null) {
                    break;
                }
                if (!retrying && countPendingInserts() < maxPendingInserts) {
                    return;
                }
                long remaining = remainingFlushTime(deadline);
                if (!backpressureLogged) {
                    LOG.info(
                            "StreamingStageSinkWriter[subtask={}] {}; blocking writes and applying "
                                    + "backpressure upstream",
                            subtaskId,
                            retrying
                                    ? "Stage INSERT is being retried asynchronously"
                                    : "pending INSERT reached limit "
                                            + countPendingInserts()
                                            + "/"
                                            + maxPendingInserts);
                    backpressureLogged = true;
                }
                stageStateLock.wait(Math.min(remaining, 100));
            }
        }
        checkTerminalFailure();
    }

    /** 必须在 stageStateLock 内调用. */
    private int countPendingInserts() {
        int count = 0;
        for (StageMeta meta : stageStatusMap.values()) {
            if (meta.status != StageStatus.INSERTED) {
                count++;
            }
        }
        return count;
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        checkTerminalFailure();
        long deadline = System.currentTimeMillis() + flushTimeoutMs;
        if (!checkpointAsyncCommitEnabled || endOfInput) {
            activeFlushDeadline = deadline;
        }
        try {
            super.flush(endOfInput);
            ensureFlushBeforeDeadline(deadline);
            if (closed) {
                return;
            }

            if (checkpointAsyncCommitEnabled && !endOfInput) {
                waitForPreviousCheckpointCohortDone(deadline);
                discardInsertedStageMetadata();
                sealCurrentStageForCheckpoint();
                ensureFlushBeforeDeadline(deadline);
                return;
            }

            boolean hadActiveStage = currentStage != null;
            if (hadActiveStage) {
                sealAndEnqueueStage();
            }
            waitForAsyncInsertDone(deadline);
            checkTerminalFailure();
            if (checkpointAsyncCommitEnabled) {
                discardInsertedStageMetadata();
            } else {
                cleanupInsertedStages();
            }
            ensureFlushBeforeDeadline(deadline);

            if (!this.endOfInput && hadActiveStage) {
                try {
                    openNewStage();
                } catch (Exception e) {
                    throw new IOException("Failed to create a new Stage", e);
                }
            }
        } catch (IOException e) {
            terminalFailure = e;
            throw e;
        } finally {
            activeFlushDeadline = Long.MAX_VALUE;
        }
    }

    private void sealCurrentStageForCheckpoint() throws IOException {
        boolean hadActiveStage = currentStage != null;
        if (hadActiveStage) {
            sealAndEnqueueStage();
        }
        if (!this.endOfInput && hadActiveStage) {
            try {
                openNewStage();
            } catch (Exception e) {
                throw new IOException("Failed to create a new Stage after checkpoint seal", e);
            }
        }
    }

    private void waitForPreviousCheckpointCohortDone(long deadline)
            throws IOException, InterruptedException {
        synchronized (stageStateLock) {
            while (terminalFailure == null && hasUnfinishedPreviousCheckpointStage()) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    throw new IOException(
                            "StreamingStageSinkWriter[subtask="
                                    + subtaskId
                                    + "] timed out waiting for the previous checkpoint Stage "
                                    + "cohort ("
                                    + flushTimeoutMs
                                    + "ms, sink.stage.flush-timeout-ms)");
                }
                stageStateLock.wait(Math.min(remaining, 100));
            }
            previousCheckpointStageNames.clear();
        }
        checkTerminalFailure();
    }

    /** 必须在 stageStateLock 内调用. */
    private boolean hasUnfinishedPreviousCheckpointStage() {
        for (String stageName : previousCheckpointStageNames) {
            StageMeta meta = stageStatusMap.get(stageName);
            if (meta != null && meta.status != StageStatus.INSERTED) {
                return true;
            }
        }
        return false;
    }

    private void checkTerminalFailure() throws IOException {
        IOException failure = terminalFailure;
        if (failure != null) {
            throw new IOException(
                    "StreamingStageSinkWriter[subtask=" + subtaskId + "] Stage INSERT failed",
                    failure);
        }
    }

    @Override
    public Collection<HologresStageCommittable> prepareCommit() {
        return Collections.emptyList();
    }

    @Override
    public List<HologresStreamingStageWriterState> snapshotState(long checkpointId)
            throws IOException {
        if (!checkpointAsyncCommitEnabled) {
            return Collections.emptyList();
        }
        checkTerminalFailure();
        List<HologresStreamingStageWriterState.PendingStage> pendingStages = new ArrayList<>();
        synchronized (stageStateLock) {
            previousCheckpointStageNames.clear();
            for (StageMeta meta : stageStatusMap.values()) {
                if (meta.status == StageStatus.INSERTED) {
                    continue;
                }
                pendingStages.add(
                        new HologresStreamingStageWriterState.PendingStage(
                                meta.stageName,
                                meta.recordCount,
                                meta.targetShards,
                                meta.sequenceId));
                previousCheckpointStageNames.add(meta.stageName);
            }
        }
        pendingStages.sort(
                Comparator.comparingLong(
                        HologresStreamingStageWriterState.PendingStage::getSequenceId));
        LOG.info(
                "StreamingStageSinkWriter[subtask={}] checkpoint {} saved {} pending Stages; "
                        + "the next checkpoint will wait for this cohort",
                subtaskId,
                checkpointId,
                pendingStages.size());
        return Collections.singletonList(
                new HologresStreamingStageWriterState(pendingStages, nextSequenceId));
    }

    private void waitForAsyncInsertDone(long deadline) throws IOException, InterruptedException {
        synchronized (stageStateLock) {
            while (terminalFailure == null && (drainActive || hasInsertingStage())) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    throw new IOException(
                            "StreamingStageSinkWriter[subtask="
                                    + subtaskId
                                    + "] checkpoint flush timed out ("
                                    + flushTimeoutMs
                                    + "ms, config sink.stage.flush-timeout-ms)");
                }
                stageStateLock.wait(Math.min(remaining, 100));
            }
        }
    }

    /** 必须在 stageStateLock 内调用. */
    private boolean hasInsertingStage() {
        for (StageMeta meta : stageStatusMap.values()) {
            if (meta.status == StageStatus.INSERTING || meta.status == StageStatus.RETRYING) {
                return true;
            }
        }
        return false;
    }

    private void ensureFlushBeforeDeadline(long deadline) throws IOException {
        remainingFlushTime(deadline);
    }

    private long remainingFlushTime(long deadline) throws IOException {
        long remaining = deadline - System.currentTimeMillis();
        if (remaining <= 0) {
            throw new IOException(
                    "StreamingStageSinkWriter[subtask="
                            + subtaskId
                            + "] Stage drain or backpressure wait timed out ("
                            + flushTimeoutMs
                            + "ms, config sink.stage.flush-timeout-ms)");
        }
        return remaining;
    }

    private void cleanupInsertedStages() {
        List<String> insertedStageNames = new ArrayList<>();
        synchronized (stageStateLock) {
            for (Map.Entry<String, StageMeta> entry : stageStatusMap.entrySet()) {
                if (entry.getValue().status == StageStatus.INSERTED) {
                    insertedStageNames.add(entry.getKey());
                }
            }
        }

        for (String stageName : insertedStageNames) {
            StageHelper.cleanupStages(param, Collections.singletonList(stageName));
            synchronized (stageStateLock) {
                StageMeta meta = stageStatusMap.get(stageName);
                if (meta != null && meta.status == StageStatus.INSERTED) {
                    stageStatusMap.remove(stageName);
                }
            }
        }
    }

    private void discardInsertedStageMetadata() {
        synchronized (stageStateLock) {
            stageStatusMap
                    .entrySet()
                    .removeIf(entry -> entry.getValue().status == StageStatus.INSERTED);
        }
    }

    @Override
    protected void onClose() throws Exception {
        shutdownRequested = true;
        synchronized (stageStateLock) {
            stageStateLock.notifyAll();
        }
        insertExecutor.shutdown();
        try {
            if (!insertExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                insertExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            insertExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
