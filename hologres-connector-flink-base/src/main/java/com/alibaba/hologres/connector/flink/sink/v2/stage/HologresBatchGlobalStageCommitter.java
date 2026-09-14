package com.alibaba.hologres.connector.flink.sink.v2.stage;

import org.apache.flink.api.connector.sink2.Committer;

import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * BATCH 模式的全局 Committer 实现.
 *
 * <p>通过 {@link
 * org.apache.flink.streaming.api.connector.sink2.StandardSinkTopologies#addGlobalCommitter} 注册到
 * PostCommitTopology，以 parallelism=1 运行.
 *
 * <p>仅用于 BATCH 模式. 流模式下不使用此 committer.
 *
 * <p>提交语义为 at-least-once. 即使只有一次 INSERT, Flink 重试或不确定失败也可能导致重复导入, 结果依赖目标表的 OnConflictAction
 * (upsert/ignore) 配置. Stage 清理为 best-effort, 失败依赖 TTL 自动过期.
 */
public class HologresBatchGlobalStageCommitter implements Committer<HologresStageCommittable> {

    private static final Logger LOG =
            LoggerFactory.getLogger(HologresBatchGlobalStageCommitter.class);

    private final HologresConnectionParam param;
    private final String qualifiedTableName;
    private final List<String> columnNames;
    private final boolean internalStageSerialReadEnabled;

    public HologresBatchGlobalStageCommitter(
            HologresConnectionParam param,
            String qualifiedTableName,
            List<String> columnNames,
            boolean internalStageSerialReadEnabled) {
        this.param = param;
        this.qualifiedTableName = qualifiedTableName;
        this.columnNames = columnNames;
        this.internalStageSerialReadEnabled = internalStageSerialReadEnabled;
        LOG.info("BatchGlobalStageCommitter: initialized, table={}", qualifiedTableName);
    }

    @Override
    public void commit(Collection<CommitRequest<HologresStageCommittable>> committables)
            throws IOException, InterruptedException {
        if (committables == null || committables.isEmpty()) {
            return;
        }

        List<String> stageNames = new ArrayList<>();
        long totalRecords = 0;
        for (CommitRequest<HologresStageCommittable> request : committables) {
            HologresStageCommittable c = request.getCommittable();
            stageNames.add(c.getStageName());
            totalRecords += c.getRecordCount();
        }

        LOG.info(
                "BatchGlobalStageCommitter: committing {} Stages, {} records, stages={}",
                stageNames.size(),
                totalRecords,
                stageNames);

        // 1. INSERT 是核心操作, 失败必须抛异常让框架重试
        try {
            StageHelper.executeInsert(
                    param,
                    qualifiedTableName,
                    columnNames,
                    stageNames,
                    totalRecords,
                    -1,
                    internalStageSerialReadEnabled);
        } catch (Exception e) {
            LOG.error(
                    "BatchGlobalStageCommitter: INSERT failed for {} Stages: {}",
                    stageNames.size(),
                    stageNames,
                    e);
            throw new IOException("Batch Stage INSERT failed", e);
        }

        // 2. Cleanup 是 best-effort, 失败不影响提交完成判定, 依赖 stage TTL 自动过期
        try {
            StageHelper.cleanupStages(param, stageNames);
        } catch (Exception e) {
            LOG.warn(
                    "BatchGlobalStageCommitter: cleanup failed (commit is unaffected), {} Stages: {}; "
                            + "the Stage files will expire by TTL",
                    stageNames.size(),
                    stageNames,
                    e);
        }

        LOG.info(
                "BatchGlobalStageCommitter: commit completed, {} Stages, {} records",
                stageNames.size(),
                totalRecords);
    }

    @Override
    public void close() throws Exception {
        LOG.info("BatchGlobalStageCommitter: closed");
    }
}
