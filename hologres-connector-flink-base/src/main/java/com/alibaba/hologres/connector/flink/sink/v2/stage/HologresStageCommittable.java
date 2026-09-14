package com.alibaba.hologres.connector.flink.sink.v2.stage;

import java.io.Serializable;

/**
 * BATCH Stage Sink 的 committable 对象.
 *
 * <p>每个 Batch SinkWriter 在 prepareCommit 时返回自己写入的 Stage 信息，GlobalCommitter 收集后执行 INSERT INTO target
 * SELECT FROM stage。流模式不产生 committable。
 *
 * <p>包含 subtaskId 和 attemptNumber 字段，用于调试和问题排查.
 */
public class HologresStageCommittable implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 该 writer 写入的 Stage 名称. */
    private final String stageName;

    /** 该 writer 写入的记录数. */
    private final long recordCount;

    /** 产生此 committable 的 subtask ID, 用于调试. */
    private final int subtaskId;

    /** 产生此 committable 的 attempt 编号, 用于调试. */
    private final int attemptNumber;

    public HologresStageCommittable(
            String stageName, long recordCount, int subtaskId, int attemptNumber) {
        this.stageName = stageName;
        this.recordCount = recordCount;
        this.subtaskId = subtaskId;
        this.attemptNumber = attemptNumber;
    }

    public String getStageName() {
        return stageName;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    @Override
    public String toString() {
        return "HologresStageCommittable{stageName='"
                + stageName
                + "', recordCount="
                + recordCount
                + ", subtaskId="
                + subtaskId
                + ", attemptNumber="
                + attemptNumber
                + "}";
    }
}
