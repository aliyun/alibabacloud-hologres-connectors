/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.impl.binlog.BinlogOffset;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.niagara.client.table.ServiceContractMsg;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Types;

/**
 * 测试 Subscribe.OffsetBuilder 和 Subscribe.StartTimeBuilder 的 build 方法.
 *
 * <p>这些测试不需要真实的数据库连接.
 */
public class SubscribeBuilderTest {

    // sequence=0 表示从起始点消费，timestamp=-1 表示不指定时间
    private static final BinlogOffset TEST_OFFSET = new BinlogOffset(0, -1);

    private Column makeColumn(String name, int sqlType) {
        Column col = new Column();
        col.setName(name);
        col.setType(sqlType);
        return col;
    }

    // ======================== OffsetBuilder tests ========================

    /** OffsetBuilder: 非分区表（无 partitionValues），filter 应正常传递. */
    @Test
    public void testOffsetBuilderNoPartitionFilterPassed() {
        Column col = makeColumn("status", Types.INTEGER);
        Subscribe subscribe =
                Subscribe.newOffsetBuilder("my_table")
                        .addShardStartOffset(0, TEST_OFFSET)
                        .addBinlogFilter(col, ServiceContractMsg.OperatorType.GREATER, 1)
                        .build();

        Assert.assertNotNull(subscribe.getBinlogFilters());
        Assert.assertEquals(subscribe.getBinlogFilters().size(), 1);
    }

    /**
     * OffsetBuilder: 物理分区表（有 partitionValues）+ filter，filter 应被忽略，getBinlogFilters() 返回 null.
     *
     * <p>修复前：getBinlogFilters() 返回非 null，消费时 filter 会过滤最后一条 LSN 导致分区消费永不结束.
     *
     * <p>修复后：getBinlogFilters() 返回 null，filter 不被下推到 server 端.
     */
    @Test
    public void testOffsetBuilderPhysicalPartitionFilterIgnored() {
        Column col = makeColumn("status", Types.INTEGER);
        Subscribe subscribe =
                Subscribe.newOffsetBuilder("my_table")
                        .addShardStartOffset(0, TEST_OFFSET)
                        .addPartitionValuesToSubscribe(new String[] {"2024"})
                        .addBinlogFilter(col, ServiceContractMsg.OperatorType.GREATER, 1)
                        .build();

        // 物理分区模式下 filter 必须被忽略，否则最后一条 LSN 记录可能被过滤导致分区消费永不结束
        Assert.assertNull(subscribe.getBinlogFilters());
    }

    /**
     * OffsetBuilder: 物理分区表 + filter，toString() 不应抛 NullPointerException.
     *
     * <p>修复前：binlogFilters 为 null 时 toString() 调用 binlogFilters.toString() 抛 NPE.
     *
     * <p>修复后：toString() 安全输出 "null".
     */
    @Test
    public void testOffsetBuilderPhysicalPartitionToStringNoNPE() {
        Column col = makeColumn("status", Types.INTEGER);
        Subscribe subscribe =
                Subscribe.newOffsetBuilder("my_table")
                        .addShardStartOffset(0, TEST_OFFSET)
                        .addPartitionValuesToSubscribe(new String[] {"2024"})
                        .addBinlogFilter(col, ServiceContractMsg.OperatorType.GREATER, 1)
                        .build();

        // toString() 在 binlogFilters 为 null 时不应抛 NPE
        String str = subscribe.toString();
        Assert.assertNotNull(str);
        Assert.assertTrue(str.contains("my_table"));
    }

    /** OffsetBuilder: 逻辑分区表（有 logicalPartitionNames 但无 partitionValues），filter 应正常传递. */
    @Test
    public void testOffsetBuilderLogicalPartitionFilterPassed() {
        Column col = makeColumn("status", Types.INTEGER);
        Subscribe subscribe =
                Subscribe.newOffsetBuilder("my_table")
                        .addShardStartOffset(0, TEST_OFFSET)
                        .addLogicalPartitionNamesToSubscribe(new String[] {"region"})
                        .addLogicalPartitionValuesToSubscribe(new String[][] {{"CN"}, {"US"}})
                        .addBinlogFilter(col, ServiceContractMsg.OperatorType.GREATER, 1)
                        .build();

        Assert.assertNotNull(subscribe.getBinlogFilters());
        Assert.assertEquals(subscribe.getBinlogFilters().size(), 1);
    }

    /** OffsetBuilder: 物理分区表 + 显式 per-partition 偏移量，子 Subscribe 的 filter 也应被忽略. */
    @Test
    public void testOffsetBuilderPhysicalPartitionSubPartitionFilterIgnored() {
        Column col = makeColumn("status", Types.INTEGER);
        Subscribe subscribe =
                Subscribe.newOffsetBuilder("my_table")
                        .addShardStartOffset(0, TEST_OFFSET)
                        .addPartitionValuesToSubscribe(new String[] {"2024"})
                        .addShardStartOffsetForPartition("my_table_2024", 0, TEST_OFFSET)
                        .addBinlogFilter(col, ServiceContractMsg.OperatorType.GREATER, 1)
                        .build();

        Assert.assertNull(subscribe.getBinlogFilters());
        // 子 Subscribe 的 filter 也应被忽略
        Subscribe partitionSubscribe = subscribe.getPartitionToSubscribeMap().get("my_table_2024");
        Assert.assertNotNull(partitionSubscribe);
        Assert.assertNull(partitionSubscribe.getBinlogFilters());
    }

    // ======================== StartTimeBuilder tests ========================

    /** StartTimeBuilder: 非分区表（无 partitionValues），filter 应正常传递. */
    @Test
    public void testStartTimeBuilderNoPartitionFilterPassed() {
        Column col = makeColumn("status", Types.INTEGER);
        Subscribe subscribe =
                Subscribe.newStartTimeBuilder("my_table")
                        .setBinlogReadStartTime("2024-01-01 00:00:00")
                        .addBinlogFilter(col, ServiceContractMsg.OperatorType.GREATER, 1)
                        .build();

        Assert.assertNotNull(subscribe.getBinlogFilters());
        Assert.assertEquals(subscribe.getBinlogFilters().size(), 1);
    }

    /**
     * StartTimeBuilder: 物理分区表（有 partitionValues）+ filter，filter 应被忽略，getBinlogFilters() 返回 null.
     *
     * <p>修复前：getBinlogFilters() 返回非 null，消费时 filter 可能过滤最后一条 LSN 导致分区消费卡死.
     *
     * <p>修复后：getBinlogFilters() 返回 null，filter 不被下推.
     */
    @Test
    public void testStartTimeBuilderPhysicalPartitionFilterIgnored() {
        Column col = makeColumn("status", Types.INTEGER);
        Subscribe subscribe =
                Subscribe.newStartTimeBuilder("my_table")
                        .setBinlogReadStartTime("2024-01-01 00:00:00")
                        .addPartitionValuesToSubscribe(new String[] {"2024"})
                        .addBinlogFilter(col, ServiceContractMsg.OperatorType.GREATER, 1)
                        .build();

        Assert.assertNull(subscribe.getBinlogFilters());
    }

    /**
     * StartTimeBuilder: 物理分区表 + filter，toString() 不应抛 NullPointerException.
     *
     * <p>修复前：binlogFilters 为 null 时 toString() 调用 binlogFilters.toString() 抛 NPE.
     *
     * <p>修复后：toString() 安全输出 "null".
     */
    @Test
    public void testStartTimeBuilderPhysicalPartitionToStringNoNPE() {
        Column col = makeColumn("status", Types.INTEGER);
        Subscribe subscribe =
                Subscribe.newStartTimeBuilder("my_table")
                        .setBinlogReadStartTime("2024-01-01 00:00:00")
                        .addPartitionValuesToSubscribe(new String[] {"2024"})
                        .addBinlogFilter(col, ServiceContractMsg.OperatorType.GREATER, 1)
                        .build();

        String str = subscribe.toString();
        Assert.assertNotNull(str);
        Assert.assertTrue(str.contains("my_table"));
    }

    /** StartTimeBuilder: 逻辑分区表（有 logicalPartitionNames 但无 partitionValues），filter 应正常传递. */
    @Test
    public void testStartTimeBuilderLogicalPartitionFilterPassed() {
        Column col = makeColumn("status", Types.INTEGER);
        Subscribe subscribe =
                Subscribe.newStartTimeBuilder("my_table")
                        .setBinlogReadStartTime("2024-01-01 00:00:00")
                        .addLogicalPartitionNamesToSubscribe(new String[] {"region"})
                        .addLogicalPartitionValuesToSubscribe(new String[][] {{"CN"}, {"US"}})
                        .addBinlogFilter(col, ServiceContractMsg.OperatorType.GREATER, 1)
                        .build();

        Assert.assertNotNull(subscribe.getBinlogFilters());
        Assert.assertEquals(subscribe.getBinlogFilters().size(), 1);
    }
}
