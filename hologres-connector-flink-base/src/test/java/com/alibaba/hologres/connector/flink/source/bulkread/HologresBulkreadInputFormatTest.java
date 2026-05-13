package com.alibaba.hologres.connector.flink.source.bulkread;

import org.junit.Assert;
import org.junit.Test;

/** Tests for Hologres bulk read split planning. */
public class HologresBulkreadInputFormatTest {

    @Test
    public void testDefaultSplitCountKeepsOneSplitPerShard() {
        HologresShardInputSplit[] splits =
                HologresBulkreadInputFormat.createShardInputSplits(32, -1);

        Assert.assertEquals(32, splits.length);
        Assert.assertArrayEquals(new String[] {"0"}, splits[0].getShardIds());
        Assert.assertArrayEquals(new String[] {"31"}, splits[31].getShardIds());
    }

    @Test
    public void testConfiguredSplitCountMergesShards() {
        HologresShardInputSplit[] splits =
                HologresBulkreadInputFormat.createShardInputSplits(32, 4);

        Assert.assertEquals(4, splits.length);
        Assert.assertArrayEquals(
                new String[] {"0", "1", "2", "3", "4", "5", "6", "7"}, splits[0].getShardIds());
        Assert.assertArrayEquals(
                new String[] {"24", "25", "26", "27", "28", "29", "30", "31"},
                splits[3].getShardIds());
    }

    @Test
    public void testSplitCountLargerThanShardCountKeepsOneSplitPerShard() {
        HologresShardInputSplit[] splits =
                HologresBulkreadInputFormat.createShardInputSplits(3, 10);

        Assert.assertEquals(3, splits.length);
        Assert.assertArrayEquals(new String[] {"0"}, splits[0].getShardIds());
        Assert.assertArrayEquals(new String[] {"1"}, splits[1].getShardIds());
        Assert.assertArrayEquals(new String[] {"2"}, splits[2].getShardIds());
    }

    @Test
    public void testSingleSplitCanReadAllShards() {
        HologresShardInputSplit[] splits = HologresBulkreadInputFormat.createShardInputSplits(4, 1);

        Assert.assertEquals(1, splits.length);
        Assert.assertArrayEquals(new String[] {"0", "1", "2", "3"}, splits[0].getShardIds());
    }
}
