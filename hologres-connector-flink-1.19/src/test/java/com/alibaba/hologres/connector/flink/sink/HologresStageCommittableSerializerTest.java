package com.alibaba.hologres.connector.flink.sink;

import com.alibaba.hologres.connector.flink.sink.v2.stage.HologresStageCommittable;
import com.alibaba.hologres.connector.flink.sink.v2.stage.HologresStageCommittableSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/** Unit tests for HologresStageCommittableSerializer. */
public class HologresStageCommittableSerializerTest {

    @Test
    public void testSerializeAndDeserialize() throws IOException {
        HologresStageCommittableSerializer serializer = new HologresStageCommittableSerializer();

        HologresStageCommittable original =
                new HologresStageCommittable("flink_stage_0_0_1234567890_1", 42L, -1, -1);

        byte[] serialized = serializer.serialize(original);
        Assert.assertNotNull(serialized);
        Assert.assertTrue(serialized.length > 0);

        HologresStageCommittable deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);
        Assert.assertNotNull(deserialized);
        Assert.assertEquals(original.getStageName(), deserialized.getStageName());
        Assert.assertEquals(original.getRecordCount(), deserialized.getRecordCount());
        Assert.assertEquals(original.getSubtaskId(), deserialized.getSubtaskId());
        Assert.assertEquals(original.getAttemptNumber(), deserialized.getAttemptNumber());
    }

    @Test
    public void testSerializeEmptyStageName() throws IOException {
        HologresStageCommittableSerializer serializer = new HologresStageCommittableSerializer();

        HologresStageCommittable original = new HologresStageCommittable("", 0L, -1, -1);

        byte[] serialized = serializer.serialize(original);
        HologresStageCommittable deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);

        Assert.assertEquals("", deserialized.getStageName());
        Assert.assertEquals(0L, deserialized.getRecordCount());
    }

    @Test
    public void testSerializeLargeRecordCount() throws IOException {
        HologresStageCommittableSerializer serializer = new HologresStageCommittableSerializer();

        HologresStageCommittable original =
                new HologresStageCommittable("stage_large", Long.MAX_VALUE, -1, -1);

        byte[] serialized = serializer.serialize(original);
        HologresStageCommittable deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);

        Assert.assertEquals("stage_large", deserialized.getStageName());
        Assert.assertEquals(Long.MAX_VALUE, deserialized.getRecordCount());
    }

    @Test(expected = IOException.class)
    public void testDeserializeWithWrongVersion() throws IOException {
        HologresStageCommittableSerializer serializer = new HologresStageCommittableSerializer();

        HologresStageCommittable original = new HologresStageCommittable("stage_test", 100L, 0, 0);
        byte[] serialized = serializer.serialize(original);

        // Should throw IOException for unsupported version
        serializer.deserialize(999, serialized);
    }

    @Test
    public void testGetVersion() {
        HologresStageCommittableSerializer serializer = new HologresStageCommittableSerializer();
        Assert.assertEquals(1, serializer.getVersion());
    }

    @Test
    public void testSerializeWithSubtaskInfo() throws IOException {
        HologresStageCommittableSerializer serializer = new HologresStageCommittableSerializer();

        HologresStageCommittable original =
                new HologresStageCommittable("stage_with_info", 500L, 3, 1);

        byte[] serialized = serializer.serialize(original);
        HologresStageCommittable deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);

        Assert.assertEquals("stage_with_info", deserialized.getStageName());
        Assert.assertEquals(500L, deserialized.getRecordCount());
        Assert.assertEquals(3, deserialized.getSubtaskId());
        Assert.assertEquals(1, deserialized.getAttemptNumber());
    }

    @Test
    public void testCommittableToString() {
        HologresStageCommittable committable =
                new HologresStageCommittable("my_stage", 123L, -1, -1);
        String str = committable.toString();
        Assert.assertTrue(str.contains("my_stage"));
        Assert.assertTrue(str.contains("123"));
        Assert.assertTrue(str.contains("subtaskId"));
        Assert.assertTrue(str.contains("attemptNumber"));
    }
}
