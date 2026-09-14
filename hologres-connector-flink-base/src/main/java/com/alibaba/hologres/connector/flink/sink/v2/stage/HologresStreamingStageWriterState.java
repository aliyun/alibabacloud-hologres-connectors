package com.alibaba.hologres.connector.flink.sink.v2.stage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Recoverable metadata for sealed Streaming Stage files awaiting asynchronous INSERT. */
public class HologresStreamingStageWriterState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<PendingStage> pendingStages;
    private final long nextSequenceId;

    public HologresStreamingStageWriterState(
            List<PendingStage> pendingStages, long nextSequenceId) {
        this.pendingStages = new ArrayList<>(pendingStages);
        this.nextSequenceId = nextSequenceId;
    }

    public List<PendingStage> getPendingStages() {
        return Collections.unmodifiableList(pendingStages);
    }

    public long getNextSequenceId() {
        return nextSequenceId;
    }

    /** Immutable metadata for one sealed Stage. */
    public static class PendingStage implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String stageName;
        private final long recordCount;
        private final List<Integer> targetShards;
        private final long sequenceId;

        public PendingStage(
                String stageName, long recordCount, List<Integer> targetShards, long sequenceId) {
            this.stageName = stageName;
            this.recordCount = recordCount;
            this.targetShards = new ArrayList<>(targetShards);
            this.sequenceId = sequenceId;
        }

        public String getStageName() {
            return stageName;
        }

        public long getRecordCount() {
            return recordCount;
        }

        public List<Integer> getTargetShards() {
            return Collections.unmodifiableList(targetShards);
        }

        public long getSequenceId() {
            return sequenceId;
        }
    }
}
