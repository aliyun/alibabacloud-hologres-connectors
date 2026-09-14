package com.alibaba.hologres.connector.flink.sink;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.alibaba.hologres.connector.flink.api.HologresTableSchema;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.config.JDBCOptions;
import com.alibaba.hologres.connector.flink.sink.v2.stage.HologresBatchGlobalStageCommitter;
import com.alibaba.hologres.connector.flink.sink.v2.stage.HologresStageCommittable;
import com.alibaba.hologres.connector.flink.sink.v2.stage.HologresStageSink;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for HologresStageSink routing logic and per-subtask no-op committer creation. */
public class HologresStageSinkRoutingTest {

    private static RowType createSimpleRowType() {
        return RowType.of(
                new org.apache.flink.table.types.logical.LogicalType[] {
                    new IntType(), new VarCharType(100)
                },
                new String[] {"id", "name"});
    }

    private static HologresConnectionParam createMockParam() {
        HologresConnectionParam param = mock(HologresConnectionParam.class);
        JDBCOptions jdbcOptions = mock(JDBCOptions.class);
        when(jdbcOptions.getDbUrl()).thenReturn("jdbc:postgresql://localhost:5432/test");
        when(param.getJdbcOptions()).thenReturn(jdbcOptions);
        when(param.getUsername()).thenReturn("test");
        when(param.getPassword()).thenReturn("test");
        return param;
    }

    private static HologresTableSchema createMockTableSchema(String... distributionKeys) {
        HologresTableSchema hologresTableSchema = mock(HologresTableSchema.class);
        com.alibaba.hologres.client.model.TableSchema tableSchema =
                mock(com.alibaba.hologres.client.model.TableSchema.class);
        when(tableSchema.getDistributionKeys()).thenReturn(distributionKeys);
        when(hologresTableSchema.get()).thenReturn(tableSchema);
        when(hologresTableSchema.getShardCount()).thenReturn(4);
        return hologresTableSchema;
    }

    private static CommitterInitContext createMockCommitterInitContext(
            int subtaskId, int numTasks) {
        CommitterInitContext context = mock(CommitterInitContext.class);
        when(context.getSubtaskId()).thenReturn(subtaskId);
        when(context.getNumberOfParallelSubtasks()).thenReturn(numTasks);
        return context;
    }

    @Test
    public void testBatchModeCreateCommitter() throws Exception {
        HologresConnectionParam param = createMockParam();
        HologresTableSchema tableSchema = createMockTableSchema("id");
        RowType rowType = createSimpleRowType();

        HologresStageSink sink =
                new HologresStageSink(param, "test_table", rowType, tableSchema, true);

        Committer<HologresStageCommittable> committer = sink.createCommitter(null);
        assertNotNull(committer);
        assertTrue(
                "BATCH mode should use no-op committer, not BatchGlobalStageCommitter",
                !(committer instanceof HologresBatchGlobalStageCommitter));
    }

    @Test
    public void testStreamingModeCreateCommitter() throws Exception {
        HologresConnectionParam param = createMockParam();
        HologresTableSchema tableSchema = createMockTableSchema("id");
        RowType rowType = createSimpleRowType();

        HologresStageSink sink =
                new HologresStageSink(param, "test_table", rowType, tableSchema, false);

        Committer<HologresStageCommittable> committer =
                sink.createCommitter(createMockCommitterInitContext(0, 2));
        assertNotNull(committer);
        assertTrue(
                "STREAMING mode should use no-op committer",
                !(committer instanceof HologresBatchGlobalStageCommitter));
    }

    @Test
    public void testDistributionKeyIndexResolution() {
        HologresConnectionParam param = createMockParam();
        // distribution keys 引用 RowType 中的字段
        HologresTableSchema tableSchema = createMockTableSchema("name");
        RowType rowType = createSimpleRowType();

        // 构造函数应正确解析 "name" 到 RowType 索引 1
        HologresStageSink sink =
                new HologresStageSink(param, "test_table", rowType, tableSchema, false);
        assertNotNull(sink);
        assertTrue(SupportsPreWriteTopology.class.isAssignableFrom(sink.getClass()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidDistributionKeyThrows() {
        HologresConnectionParam param = createMockParam();
        // distribution key 不在 RowType 中
        HologresTableSchema tableSchema = createMockTableSchema("nonexistent_col");
        RowType rowType = createSimpleRowType();

        // 应抛出 IllegalArgumentException
        new HologresStageSink(param, "test_table", rowType, tableSchema, false);
    }
}
