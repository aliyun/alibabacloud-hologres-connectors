package com.alibaba.hologres.connector.flink.sink.v2.stage;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.StandardSinkTopologies;
import org.apache.flink.streaming.api.connector.sink2.SupportsPostCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.alibaba.hologres.connector.flink.api.HologresTableSchema;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.sink.repartition.HoloKeySelector;
import com.alibaba.hologres.connector.flink.sink.repartition.HoloPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Flink Sink V2 实现，基于 Hologres Stage 写入模式.
 *
 * <p>流模式通过 pre-write topology 按 Hologres shard 分区，但数据流仍保持 RowData。writer 从首条记录重新计算 shard，仅用于识别当前
 * partition 负责的完整 shard 集合，不依赖运行时 task id。
 *
 * <p>根据是否有界 (BATCH/STREAMING) 选择不同的 Writer 和提交路径:
 *
 * <ul>
 *   <li>BATCH: 所有 writer 各自写 stage, GlobalCommitter 统一 INSERT
 *   <li>STREAMING: per-writer multi-shard stage + 异步 INSERT；checkpoint 可同步排空，或使用 Writer State 实现异步
 *       checkpoint 提交
 * </ul>
 */
public class HologresStageSink
        implements Sink<RowData>,
                SupportsPreWriteTopology<RowData>,
                SupportsCommitter<HologresStageCommittable>,
                SupportsWriterState<RowData, HologresStreamingStageWriterState>,
                SupportsPostCommitTopology<HologresStageCommittable> {

    private static final long serialVersionUID = 5L;
    private static final Logger LOG = LoggerFactory.getLogger(HologresStageSink.class);

    private final HologresConnectionParam param;
    private final String qualifiedTableName;
    private final RowType rowType;
    private final List<String> columnNames;
    private final boolean isBounded;
    private final int shardCount;
    private final boolean internalStageSerialReadEnabled;
    private final Integer[] distributionKeyIndexes;
    private final LogicalType[] fieldTypes;
    private Integer parallelism;

    public HologresStageSink(
            HologresConnectionParam param,
            String qualifiedTableName,
            RowType rowType,
            HologresTableSchema hologresTableSchema,
            boolean isBounded) {
        this(
                param,
                qualifiedTableName,
                rowType,
                hologresTableSchema,
                isBounded,
                new Integer[0],
                rowType.getChildren().toArray(new LogicalType[0]));
    }

    public HologresStageSink(
            HologresConnectionParam param,
            String qualifiedTableName,
            RowType rowType,
            HologresTableSchema hologresTableSchema,
            boolean isBounded,
            Integer[] distributionKeyIndexes,
            LogicalType[] fieldTypes) {
        this.param = param;
        this.qualifiedTableName = qualifiedTableName;
        this.rowType = rowType;
        this.isBounded = isBounded;
        this.shardCount = hologresTableSchema.getShardCount();
        this.internalStageSerialReadEnabled =
                param.isStageInternalStageSerialReadEnabled(isBounded);
        this.distributionKeyIndexes = distributionKeyIndexes;
        this.fieldTypes = fieldTypes;

        this.columnNames = new ArrayList<>();
        for (RowType.RowField field : rowType.getFields()) {
            columnNames.add(field.getName());
        }
        validateDistributionKeys(hologresTableSchema, columnNames);

        String mode = isBounded ? "BATCH" : "STREAMING";
        LOG.info(
                "HologresStageSink created: table={}, mode={}, columns={}, shardCount={}, "
                        + "internalStageSerialReadEnabled={}\n{}",
                qualifiedTableName,
                mode,
                columnNames,
                shardCount,
                internalStageSerialReadEnabled,
                param);
    }

    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }

    @Override
    public DataStream<RowData> addPreWriteTopology(DataStream<RowData> input) {
        if (isBounded || distributionKeyIndexes.length == 0) {
            return input;
        }
        return input.partitionCustom(
                new HoloPartitioner(parallelism),
                new HoloKeySelector<>(shardCount, distributionKeyIndexes, fieldTypes));
    }

    @Override
    public SinkWriter<RowData> createWriter(Sink.InitContext context) throws IOException {
        return createWriter(
                context.getSubtaskId(),
                context.getAttemptNumber(),
                context.getNumberOfParallelSubtasks(),
                Collections.emptyList());
    }

    @Override
    public AbstractStageSinkWriter restoreWriter(
            WriterInitContext context, Collection<HologresStreamingStageWriterState> recoveredState)
            throws IOException {
        return createWriter(
                context.getSubtaskId(),
                context.getAttemptNumber(),
                context.getNumberOfParallelSubtasks(),
                recoveredState);
    }

    @Override
    public SimpleVersionedSerializer<HologresStreamingStageWriterState> getWriterStateSerializer() {
        return new HologresStreamingStageWriterStateSerializer();
    }

    private AbstractStageSinkWriter createWriter(
            int subtaskId,
            int attemptNumber,
            int numTasks,
            Collection<HologresStreamingStageWriterState> recoveredState)
            throws IOException {
        if (isBounded) {
            LOG.info("HologresStageSink: creating BatchStageSinkWriter, subtaskId={}", subtaskId);
            return new HologresBatchStageSinkWriter(
                    param,
                    qualifiedTableName,
                    subtaskId,
                    attemptNumber,
                    numTasks,
                    shardCount,
                    isBounded,
                    rowType);
        } else {
            LOG.info(
                    "HologresStageSink: creating StreamingStageSinkWriter, subtaskId={}",
                    subtaskId);
            return new HologresStreamingStageSinkWriter(
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
                    recoveredState);
        }
    }

    @Override
    public Committer<HologresStageCommittable> createCommitter(CommitterInitContext context)
            throws IOException {
        LOG.info("HologresStageSink: creating per-subtask no-op Committer");
        return new Committer<HologresStageCommittable>() {
            @Override
            public void commit(Collection<CommitRequest<HologresStageCommittable>> committables) {}

            @Override
            public void close() {}
        };
    }

    @Override
    public SimpleVersionedSerializer<HologresStageCommittable> getCommittableSerializer() {
        return new HologresStageCommittableSerializer();
    }

    @Override
    public void addPostCommitTopology(
            DataStream<CommittableMessage<HologresStageCommittable>> committables) {
        if (isBounded) {
            LOG.info("HologresStageSink: adding PostCommitTopology (BATCH GlobalCommitter)");
            StandardSinkTopologies.addGlobalCommitter(
                    committables,
                    () ->
                            new HologresBatchGlobalStageCommitter(
                                    param,
                                    qualifiedTableName,
                                    columnNames,
                                    internalStageSerialReadEnabled),
                    HologresStageCommittableSerializer::new);
        }
    }

    private static void validateDistributionKeys(
            HologresTableSchema hologresTableSchema, List<String> columnNames) {
        Set<String> fields = new HashSet<>(columnNames);
        for (String distributionKey : hologresTableSchema.get().getDistributionKeys()) {
            if (!fields.contains(distributionKey)) {
                throw new IllegalArgumentException(
                        "Distribution key '"
                                + distributionKey
                                + "' not found in sink fields: "
                                + columnNames);
            }
        }
    }
}
