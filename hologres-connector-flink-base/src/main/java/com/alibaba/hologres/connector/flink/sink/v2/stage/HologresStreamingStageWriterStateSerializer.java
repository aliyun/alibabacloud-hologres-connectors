package com.alibaba.hologres.connector.flink.sink.v2.stage;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Serializer for recoverable Streaming Stage metadata. */
public class HologresStreamingStageWriterStateSerializer
        implements SimpleVersionedSerializer<HologresStreamingStageWriterState> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(HologresStreamingStageWriterState state) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeLong(state.getNextSequenceId());
        List<HologresStreamingStageWriterState.PendingStage> stages = state.getPendingStages();
        out.writeInt(stages.size());
        for (HologresStreamingStageWriterState.PendingStage stage : stages) {
            out.writeUTF(stage.getStageName());
            out.writeLong(stage.getRecordCount());
            out.writeLong(stage.getSequenceId());
            List<Integer> targetShards = stage.getTargetShards();
            out.writeInt(targetShards.size());
            for (Integer targetShard : targetShards) {
                out.writeInt(targetShard);
            }
        }
        out.flush();
        return bytes.toByteArray();
    }

    @Override
    public HologresStreamingStageWriterState deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported Streaming Stage writer state version: " + version);
        }
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized));
        long nextSequenceId = in.readLong();
        int stageCount = in.readInt();
        List<HologresStreamingStageWriterState.PendingStage> stages = new ArrayList<>(stageCount);
        for (int i = 0; i < stageCount; i++) {
            String stageName = in.readUTF();
            long recordCount = in.readLong();
            long sequenceId = in.readLong();
            int shardCount = in.readInt();
            List<Integer> targetShards = new ArrayList<>(shardCount);
            for (int j = 0; j < shardCount; j++) {
                targetShards.add(in.readInt());
            }
            stages.add(
                    new HologresStreamingStageWriterState.PendingStage(
                            stageName, recordCount, targetShards, sequenceId));
        }
        return new HologresStreamingStageWriterState(stages, nextSequenceId);
    }
}
