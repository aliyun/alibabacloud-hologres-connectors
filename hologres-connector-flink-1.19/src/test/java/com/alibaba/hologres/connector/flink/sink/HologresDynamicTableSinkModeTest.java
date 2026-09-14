package com.alibaba.hologres.connector.flink.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;

import com.alibaba.hologres.connector.flink.config.HologresConfigs;
import com.alibaba.hologres.connector.flink.config.StageExecutionMode;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for stage execution mode resolution priority. */
public class HologresDynamicTableSinkModeTest {

    private static DynamicTableSink.Context mockContext(boolean bounded) {
        DynamicTableSink.Context context = mock(DynamicTableSink.Context.class);
        when(context.isBounded()).thenReturn(bounded);
        return context;
    }

    @Test
    public void testAutoFollowsBoundedness() {
        Configuration config = new Configuration();
        config.set(HologresConfigs.STAGE_EXECUTION_MODE, StageExecutionMode.AUTO);

        assertTrue(HologresDynamicTableSink.resolveStageBoundedMode(config, mockContext(true)));
        assertFalse(HologresDynamicTableSink.resolveStageBoundedMode(config, mockContext(false)));
    }

    @Test
    public void testExplicitBatchOverridesRuntimeMode() {
        Configuration config = new Configuration();
        config.set(HologresConfigs.STAGE_EXECUTION_MODE, StageExecutionMode.BATCH);
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);

        assertTrue(HologresDynamicTableSink.resolveStageBoundedMode(config, mockContext(false)));
    }

    @Test
    public void testExplicitStreamingOverridesRuntimeMode() {
        Configuration config = new Configuration();
        config.set(HologresConfigs.STAGE_EXECUTION_MODE, StageExecutionMode.STREAMING);
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

        assertFalse(HologresDynamicTableSink.resolveStageBoundedMode(config, mockContext(true)));
    }
}
