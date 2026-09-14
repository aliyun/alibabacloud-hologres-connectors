package com.alibaba.hologres.connector.flink.sink.v2.stage;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/** Tests for recoverable Streaming Stage writer state. */
public class HologresStreamingStageWriterStateSerializerTest {

    @Test
    public void testRoundTrip() throws IOException {
        HologresStreamingStageWriterState state =
                new HologresStreamingStageWriterState(
                        Arrays.asList(
                                new HologresStreamingStageWriterState.PendingStage(
                                        "stage-1", 123L, Arrays.asList(1, 3, 5), 7L),
                                new HologresStreamingStageWriterState.PendingStage(
                                        "stage-2", 456L, Collections.emptyList(), 8L)),
                        9L);
        HologresStreamingStageWriterStateSerializer serializer =
                new HologresStreamingStageWriterStateSerializer();

        HologresStreamingStageWriterState restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(state));

        assertEquals(9L, restored.getNextSequenceId());
        assertEquals(2, restored.getPendingStages().size());
        assertPendingStage(restored.getPendingStages().get(0), "stage-1", 123L, 7L, 1, 3, 5);
        assertPendingStage(restored.getPendingStages().get(1), "stage-2", 456L, 8L);
    }

    @Test(expected = IOException.class)
    public void testRejectsUnknownVersion() throws IOException {
        new HologresStreamingStageWriterStateSerializer().deserialize(2, new byte[0]);
    }

    private static void assertPendingStage(
            HologresStreamingStageWriterState.PendingStage stage,
            String stageName,
            long recordCount,
            long sequenceId,
            Integer... targetShards) {
        assertEquals(stageName, stage.getStageName());
        assertEquals(recordCount, stage.getRecordCount());
        assertEquals(sequenceId, stage.getSequenceId());
        assertEquals(Arrays.asList(targetShards), stage.getTargetShards());
    }
}
