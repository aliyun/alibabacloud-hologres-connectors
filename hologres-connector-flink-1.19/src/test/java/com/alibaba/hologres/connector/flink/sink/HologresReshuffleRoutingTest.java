package com.alibaba.hologres.connector.flink.sink;

import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;

import com.alibaba.hologres.client.impl.util.ShardUtil;
import com.alibaba.hologres.connector.flink.api.HologresTableSchema;
import com.alibaba.hologres.connector.flink.sink.repartition.HoloKeySelector;
import com.alibaba.hologres.connector.flink.sink.repartition.HoloPartitioner;
import com.alibaba.hologres.connector.flink.sink.repartition.RowDataShardUtil;
import com.alibaba.hologres.connector.flink.sink.v2.HologresSink;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for task-id-independent pre-write shard routing. */
public class HologresReshuffleRoutingTest {

    @Test
    public void testKeySelectorComputesStableShardWithoutChangingRowType() throws Exception {
        HoloKeySelector<GenericRowData> selector =
                new HoloKeySelector<>(4, new Integer[] {0}, new LogicalType[] {new IntType()});
        GenericRowData row = GenericRowData.of(42);

        int first = selector.getKey(row);
        int second = selector.getKey(row);

        assertTrue(first >= 0 && first < 4);
        assertEquals(first, second);
    }

    @Test
    public void testPartitionerUsesComputedShardId() {
        HoloPartitioner partitioner = new HoloPartitioner(null);
        assertEquals(0, partitioner.partition(0, 2));
        assertEquals(1, partitioner.partition(1, 2));
        assertEquals(0, partitioner.partition(2, 2));
        assertEquals(1, partitioner.partition(3, 2));
    }

    @Test
    public void testTargetShardListDerivedFromDataPartitionInsteadOfTaskId() {
        assertEquals(Arrays.asList(3, 7), RowDataShardUtil.getTargetShardList(3, 10, 4));
        assertEquals(Arrays.asList(0, 4, 8), RowDataShardUtil.getTargetShardList(8, 10, 4));
        assertEquals(Arrays.asList(0, 10), RowDataShardUtil.getTargetShardList(0, 20, 10));
        assertEquals(Arrays.asList(7, 17), RowDataShardUtil.getTargetShardList(17, 20, 10));
    }

    @Test
    public void testSinkUsesPreWriteTopology() {
        assertTrue(SupportsPreWriteTopology.class.isAssignableFrom(HologresSink.class));
    }

    @Test
    public void testDistributionKeyIndexesFollowInputFieldOrder() throws Exception {
        HologresTableSchema tableSchema = mock(HologresTableSchema.class);
        com.alibaba.hologres.client.model.TableSchema schema =
                mock(com.alibaba.hologres.client.model.TableSchema.class);
        when(tableSchema.get()).thenReturn(schema);
        when(schema.getDistributionKeys()).thenReturn(new String[] {"id", "name"});

        assertArrayEquals(
                new Integer[] {1, 0},
                getDistributionKeyIndexes(new String[] {"name", "id"}, tableSchema));
    }

    @Test
    public void testMissingDistributionKeyFailsFast() throws Exception {
        HologresTableSchema tableSchema = mock(HologresTableSchema.class);
        com.alibaba.hologres.client.model.TableSchema schema =
                mock(com.alibaba.hologres.client.model.TableSchema.class);
        when(tableSchema.get()).thenReturn(schema);
        when(schema.getDistributionKeys()).thenReturn(new String[] {"id"});

        try {
            getDistributionKeyIndexes(new String[] {"name"}, tableSchema);
            fail("Expected missing distribution key to fail");
        } catch (InvocationTargetException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains("missing from sink input fields"));
        }
    }

    @Test
    public void testTimestampDistributionKeyUsesMicrosecondPrecision() {
        GenericRowData row = new GenericRowData(1);
        row.setField(0, TimestampData.fromEpochMillis(1_000L, 123_000));

        assertEquals(
                ShardUtil.hash(1_000_123L),
                RowDataShardUtil.hash(
                        row, new Integer[] {0}, new LogicalType[] {new TimestampType(3)}));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTimeDistributionKeyFailsFast() {
        GenericRowData row = new GenericRowData(1);
        row.setField(0, 12345);

        RowDataShardUtil.hash(row, new Integer[] {0}, new LogicalType[] {new TimeType(0)});
    }

    private static Integer[] getDistributionKeyIndexes(
            String[] inputFieldNames, HologresTableSchema tableSchema) throws Exception {
        Method method =
                HologresDynamicTableSink.class.getDeclaredMethod(
                        "getDistributionKeyIndexes", String[].class, HologresTableSchema.class);
        method.setAccessible(true);
        return (Integer[]) method.invoke(null, inputFieldNames, tableSchema);
    }
}
