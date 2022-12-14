/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.utils;

import com.alibaba.hologres.client.impl.util.ShardUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * ShardUtil单元测试用例.
 */
public class ShardUtilTest {

	@Test
	public void testSplit001() {
		int[][] shardCount = ShardUtil.split(2);
		Assert.assertEquals(2, shardCount.length);
		Assert.assertEquals(0, shardCount[0][0]);
		Assert.assertEquals(32768, shardCount[0][1]);
		Assert.assertEquals(32768, shardCount[1][0]);
		Assert.assertEquals(65536, shardCount[1][1]);
	}

	@Test
	public void testBytes() {
		byte[] a = new byte[]{1, 2};
		byte[] b = new byte[]{1, 2};
		int shardCount = ShardUtil.hash(b);
		System.out.println(shardCount);
		Assert.assertEquals(ShardUtil.hash(a), ShardUtil.hash(b));
	}

	@Test
	public void testIntArray() {
		int[] a = new int[]{1, 2};
		int[] b = new int[]{1, 2};
		int shardCount = ShardUtil.hash(b);
		System.out.println(shardCount);
		Assert.assertEquals(ShardUtil.hash(a), ShardUtil.hash(b));
	}
}
