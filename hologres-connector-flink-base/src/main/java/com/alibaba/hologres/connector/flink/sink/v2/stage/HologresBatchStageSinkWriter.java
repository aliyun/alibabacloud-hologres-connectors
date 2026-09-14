package com.alibaba.hologres.connector.flink.sink.v2.stage;

import org.apache.flink.table.types.logical.RowType;

import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * BATCH 模式的 SinkWriter 实现.
 *
 * <p>每个 writer 管理单个 Stage (shardId=-1, 不分 shard), 所有 stage 由 GlobalCommit 统一提交.
 */
public class HologresBatchStageSinkWriter extends AbstractStageSinkWriter {

    public HologresBatchStageSinkWriter(
            HologresConnectionParam param,
            String qualifiedTableName,
            int subtaskId,
            int attemptNumber,
            int numTasks,
            int shardCount,
            boolean isBounded,
            RowType rowType)
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
                new Integer[0]);
    }

    @Override
    protected String writerName() {
        return "BatchStageSinkWriter";
    }

    @Override
    public Collection<HologresStageCommittable> prepareCommit()
            throws IOException, InterruptedException {
        if (closed) {
            return Collections.emptyList();
        }

        SealedStage sealed = sealCurrentStage();
        if (sealed == null) {
            return Collections.emptyList();
        }

        List<HologresStageCommittable> committables = new ArrayList<>();
        committables.add(
                new HologresStageCommittable(
                        sealed.stageName, sealed.recordCount, subtaskId, attemptNumber));

        if (!endOfInput) {
            try {
                openNewStage();
            } catch (Exception e) {
                throw new IOException("Failed to create a new Stage", e);
            }
        }

        return committables;
    }
}
