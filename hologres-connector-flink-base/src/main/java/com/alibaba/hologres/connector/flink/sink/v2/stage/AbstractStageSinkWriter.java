package com.alibaba.hologres.connector.flink.sink.v2.stage;

import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.copy.in.CopyInStageWrapper;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.sink.repartition.RowDataShardUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Common writer implementation shared by batch and streaming internal-stage sinks. */
public abstract class AbstractStageSinkWriter
        implements CommittingSinkWriter<RowData, HologresStageCommittable>,
                StatefulSinkWriter<RowData, HologresStreamingStageWriterState> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractStageSinkWriter.class);

    protected final HologresConnectionParam param;
    protected final String qualifiedTableName;
    protected final int maxArrowBatchSize;
    protected final int fileSizeLimit;
    protected final int stageTtl;
    protected final int subtaskId;
    protected final int attemptNumber;
    protected final int numTasks;
    protected final int shardCount;
    protected final RowType rowType;
    protected final LogicalType[] fieldTypes;
    protected final Integer[] distributionKeyIndexes;
    protected final List<String> columnNames;
    protected final boolean isBounded;
    protected final HoloConfig holoConfig;
    protected final TableSchema tableSchema;

    /** Streaming mode keeps one active stage per writer, covering one disjoint shard group. */
    protected StageContext currentStage;

    /** Empty means no target_shard_list, as used by batch and tables without distribution keys. */
    protected List<Integer> targetShards = Collections.emptyList();

    protected boolean closed;
    protected boolean endOfInput;
    protected long totalRecords;
    protected long startTime;

    /** Mutable state for the single active stage owned by a writer. */
    protected static class StageContext {
        public String stageName;
        public CopyInStageWrapper<RowData> wrapper;
        public long recordCount;
        public long startTime;

        StageContext(String stageName, CopyInStageWrapper<RowData> wrapper, long startTime) {
            this.stageName = stageName;
            this.wrapper = wrapper;
            this.startTime = startTime;
        }
    }

    /** Immutable metadata handed to the INSERT queue after a stage is sealed. */
    protected static class SealedStage {
        public final String stageName;
        public final long recordCount;
        public final List<Integer> targetShards;

        SealedStage(String stageName, long recordCount, List<Integer> targetShards) {
            this.stageName = stageName;
            this.recordCount = recordCount;
            this.targetShards = targetShards;
        }
    }

    protected AbstractStageSinkWriter(
            HologresConnectionParam param,
            String qualifiedTableName,
            int subtaskId,
            int attemptNumber,
            int numTasks,
            int shardCount,
            boolean isBounded,
            RowType rowType,
            Integer[] distributionKeyIndexes)
            throws IOException {
        this.param = param;
        this.qualifiedTableName = qualifiedTableName;
        this.maxArrowBatchSize = param.getMaxArrowBatchSize();
        this.fileSizeLimit = param.getStageFileSizeLimit();
        this.stageTtl = param.getStageTtl();
        this.subtaskId = subtaskId;
        this.attemptNumber = attemptNumber;
        this.numTasks = numTasks;
        this.shardCount = shardCount;
        this.rowType = rowType;
        this.fieldTypes = rowType.getChildren().toArray(new LogicalType[0]);
        this.distributionKeyIndexes = distributionKeyIndexes;
        this.isBounded = isBounded;
        this.startTime = System.currentTimeMillis();
        this.columnNames = new ArrayList<>();
        for (RowType.RowField field : rowType.getFields()) {
            columnNames.add(field.getName());
        }

        LOG.info(
                "{}[subtask={}] initialized, table={}, isBounded={}, columns={}",
                writerName(),
                subtaskId,
                qualifiedTableName,
                isBounded,
                columnNames);

        try {
            this.holoConfig = StageHelper.createHoloConfig(param);
            this.tableSchema = StageHelper.getTableSchema(holoConfig, qualifiedTableName);
            StageHelper.validateStageColumns(tableSchema, columnNames, qualifiedTableName);
        } catch (Exception e) {
            throw new IOException("Failed to initialize " + writerName(), e);
        }
    }

    protected abstract String writerName();

    protected void initializeStreamingRouting(RowData firstRow) throws IOException {
        if (currentStage != null) {
            return;
        }
        if (!isBounded && distributionKeyIndexes.length > 0) {
            int routedShard =
                    RowDataShardUtil.getShardId(
                            firstRow, distributionKeyIndexes, fieldTypes, shardCount);
            targetShards = RowDataShardUtil.getTargetShardList(routedShard, shardCount, numTasks);
        }
        try {
            openNewStage();
        } catch (Exception e) {
            throw new IOException("Failed to initialize Stage writer", e);
        }
    }

    protected StageContext openNewStage() throws Exception {
        String stageName = StageHelper.generateStageName(subtaskId, attemptNumber);
        StageHelper.createStage(param, stageName, stageTtl);
        try {
            CopyInStageWrapper<RowData> wrapper =
                    StageHelper.createCopyInStageWrapper(
                            holoConfig,
                            tableSchema,
                            stageName,
                            subtaskId,
                            columnNames,
                            maxArrowBatchSize,
                            fileSizeLimit,
                            rowType,
                            param.isStageCompressionEnabled());
            currentStage = new StageContext(stageName, wrapper, System.currentTimeMillis());
            LOG.info(
                    "{}[subtask={}] created new Stage {}, targetShards={}",
                    writerName(),
                    subtaskId,
                    stageName,
                    targetShards);
            return currentStage;
        } catch (Exception e) {
            StageHelper.cleanupStages(param, Collections.singletonList(stageName));
            throw e;
        }
    }

    protected SealedStage sealCurrentStage() throws IOException {
        StageContext ctx = currentStage;
        currentStage = null;
        if (ctx == null) {
            return null;
        }
        StageHelper.closeCopyInStageWrapper(ctx.wrapper, ctx.stageName, subtaskId);
        if (ctx.recordCount == 0) {
            StageHelper.cleanupStages(param, Collections.singletonList(ctx.stageName));
            LOG.info(
                    "{}[subtask={}] dropped empty Stage {}, targetShards={}",
                    writerName(),
                    subtaskId,
                    ctx.stageName,
                    targetShards);
            return null;
        }
        LOG.info(
                "{}[subtask={}] Stage {} sealed, targetShards={}, records={}",
                writerName(),
                subtaskId,
                ctx.stageName,
                targetShards,
                ctx.recordCount);
        return new SealedStage(ctx.stageName, ctx.recordCount, targetShards);
    }

    @Override
    public void write(RowData rowData, Context context) throws IOException, InterruptedException {
        initializeStreamingRouting(rowData);
        currentStage.wrapper.putRecord(rowData);
        currentStage.recordCount++;
        totalRecords++;
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (endOfInput) {
            this.endOfInput = true;
            LOG.info(
                    "{}[subtask={}] endOfInput, totalRecords={}",
                    writerName(),
                    subtaskId,
                    totalRecords);
        }
    }

    @Override
    public List<HologresStreamingStageWriterState> snapshotState(long checkpointId)
            throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        onClose();
        if (currentStage != null) {
            if (currentStage.recordCount > 0) {
                LOG.warn(
                        "{}[subtask={}] closing with Stage {} containing {} uncommitted records; "
                                + "the Stage will expire by TTL",
                        writerName(),
                        subtaskId,
                        currentStage.stageName,
                        currentStage.recordCount);
            }
            StageHelper.closeCopyInStageWrapperQuietly(
                    currentStage.wrapper, currentStage.stageName, subtaskId);
            currentStage = null;
        }
        LOG.info(
                "{}[subtask={}] closed, totalRecords={}, elapsedMs={}",
                writerName(),
                subtaskId,
                totalRecords,
                System.currentTimeMillis() - startTime);
    }

    protected void onClose() throws Exception {}
}
