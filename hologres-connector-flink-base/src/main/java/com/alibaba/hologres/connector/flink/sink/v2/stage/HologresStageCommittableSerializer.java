package com.alibaba.hologres.connector.flink.sink.v2.stage;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** BATCH Stage committable 的序列化器. */
public class HologresStageCommittableSerializer
        implements SimpleVersionedSerializer<HologresStageCommittable> {

    /** V1: stageName + recordCount + subtaskId + attemptNumber. */
    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(HologresStageCommittable committable) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeUTF(committable.getStageName());
        out.writeLong(committable.getRecordCount());
        out.writeInt(committable.getSubtaskId());
        out.writeInt(committable.getAttemptNumber());
        out.flush();
        return baos.toByteArray();
    }

    @Override
    public HologresStageCommittable deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported serializer version: " + version);
        }
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized));
        String stageName = in.readUTF();
        long recordCount = in.readLong();
        int subtaskId = in.readInt();
        int attemptNumber = in.readInt();
        return new HologresStageCommittable(stageName, recordCount, subtaskId, attemptNumber);
    }
}
