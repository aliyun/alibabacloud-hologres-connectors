/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.HoloClientTestBase;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.checkandput.CheckAndPutCondition;
import com.alibaba.hologres.client.model.checkandput.CheckCompareOp;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Types;

/**
 * UT for UpsertStatementBuilder.
 */
public class UpsertStatementBuilderTest extends HoloClientTestBase {

	@Test
	public void testBuildCheckAndPutPattern() {
		HoloConfig config = buildConfig();
		UpsertStatementBuilder builder = new UpsertStatementBuilder(config);
		// 准备插入的数据与已有数据做比较：new column checkOp old column
		for (CheckCompareOp op : CheckCompareOp.values()) {
			Column checkColumn1 = new Column();
			checkColumn1.setName("TEST_Column");
			checkColumn1.setType(Types.BIGINT);
			checkColumn1.setTypeName("bigint");
			CheckAndPutCondition checkAndPutCondition1 = new CheckAndPutCondition(checkColumn1, op, null, null);
			if (op == CheckCompareOp.IS_NULL || op == CheckCompareOp.IS_NOT_NULL) {
				// 已有数据与null做比较：old column is null
				Assert.assertEquals(" old.\"TEST_Column\" " + op.getOperatorString(), builder.buildCheckAndPutPattern(checkAndPutCondition1));
			} else {
				Assert.assertEquals(" excluded.\"TEST_Column\" " + op.getOperatorString() + " old.\"TEST_Column\"", builder.buildCheckAndPutPattern(checkAndPutCondition1));
			}
		}

		// 准备插入的数据与已有数据做比较：new column checkOp old column, 已有数据为null时进行coalesce
		for (CheckCompareOp op : CheckCompareOp.values()) {
			Column checkColumn1 = new Column();
			checkColumn1.setName("TEST_Column");
			checkColumn1.setType(Types.BIGINT);
			checkColumn1.setTypeName("bigint");
			CheckAndPutCondition checkAndPutCondition1 = new CheckAndPutCondition(checkColumn1, op, null, 0L);
			if (op == CheckCompareOp.IS_NULL || op == CheckCompareOp.IS_NOT_NULL) {
				Assert.assertEquals(" coalesce(old.\"TEST_Column\", '0'::bigint) " + op.getOperatorString(), builder.buildCheckAndPutPattern(checkAndPutCondition1));
			} else {
				Assert.assertEquals(" excluded.\"TEST_Column\" " + op.getOperatorString() + " coalesce(old.\"TEST_Column\", '0'::bigint)", builder.buildCheckAndPutPattern(checkAndPutCondition1));
			}
		}

		// 常量与已有数据做比较：checkValue checkOp old column
		for (CheckCompareOp op : CheckCompareOp.values()) {
			Column checkColumn1 = new Column();
			checkColumn1.setName("TEST_Column");
			checkColumn1.setType(Types.BIGINT);
			checkColumn1.setTypeName("bigint");
			CheckAndPutCondition checkAndPutCondition1 = new CheckAndPutCondition(checkColumn1, op, 1234L, null);
			if (op == CheckCompareOp.IS_NULL || op == CheckCompareOp.IS_NOT_NULL) {
				// 已有数据与null做比较：old column is null
				Assert.assertEquals(" old.\"TEST_Column\" " + op.getOperatorString(), builder.buildCheckAndPutPattern(checkAndPutCondition1));
			} else {
				Assert.assertEquals(" '1234'::bigint " + op.getOperatorString() + " old.\"TEST_Column\"", builder.buildCheckAndPutPattern(checkAndPutCondition1));
			}
		}

		// 常量与已有数据做比较：checkValue checkOp old column, 已有数据为null时进行coalesce
		for (CheckCompareOp op : CheckCompareOp.values()) {
			Column checkColumn1 = new Column();
			checkColumn1.setName("TEST_Column");
			checkColumn1.setType(Types.BIGINT);
			checkColumn1.setTypeName("bigint");
			CheckAndPutCondition checkAndPutCondition1 = new CheckAndPutCondition(checkColumn1, op, 1234L, 0L);
			if (op == CheckCompareOp.IS_NULL || op == CheckCompareOp.IS_NOT_NULL) {
                Assert.assertEquals(" coalesce(old.\"TEST_Column\", '0'::bigint) " + op.getOperatorString(), builder.buildCheckAndPutPattern(checkAndPutCondition1));
            } else {
				Assert.assertEquals(" '1234'::bigint " + op.getOperatorString() + " coalesce(old.\"TEST_Column\", '0'::bigint)", builder.buildCheckAndPutPattern(checkAndPutCondition1));
			}
		}
	}

	@Test
	public void testBuildCheckAndDeletePattern() {
		HoloConfig config = buildConfig();
		UpsertStatementBuilder builder = new UpsertStatementBuilder(config);
		// 准备删除的数据与已有数据做比较： new column checkOp old column
		for (CheckCompareOp op : CheckCompareOp.values()) {
			Column checkColumn1 = new Column();
			checkColumn1.setName("TEST_Column");
			checkColumn1.setType(Types.BIGINT);
			checkColumn1.setTypeName("bigint");
			CheckAndPutCondition checkAndPutCondition1 = new CheckAndPutCondition(checkColumn1, op, null, null);
			if (op == CheckCompareOp.IS_NULL || op == CheckCompareOp.IS_NOT_NULL) {
				// 已有数据与null做比较：old column is null
				Assert.assertEquals(" \"TEST_Column\" " + op.getOperatorString(), builder.buildCheckAndDeletePattern(checkAndPutCondition1));
			} else {
				Assert.assertEquals(" ? " + op.getOperatorString() + " \"TEST_Column\"", builder.buildCheckAndDeletePattern(checkAndPutCondition1));
			}
		}

		// 准备删除的数据与已有数据做比较： new column checkOp old column, 已有数据为null时进行coalesce
		for (CheckCompareOp op : CheckCompareOp.values()) {
			Column checkColumn1 = new Column();
			checkColumn1.setName("TEST_Column");
			checkColumn1.setType(Types.BIGINT);
			checkColumn1.setTypeName("bigint");
			CheckAndPutCondition checkAndPutCondition1 = new CheckAndPutCondition(checkColumn1, op, null, 0L);
			if (op == CheckCompareOp.IS_NULL || op == CheckCompareOp.IS_NOT_NULL) {
				// 已有数据与null做比较：old column is null
				Assert.assertEquals(" coalesce(\"TEST_Column\", '0'::bigint) " + op.getOperatorString(), builder.buildCheckAndDeletePattern(checkAndPutCondition1));
			} else {
				Assert.assertEquals(" ? " + op.getOperatorString() + " coalesce(\"TEST_Column\", '0'::bigint)", builder.buildCheckAndDeletePattern(checkAndPutCondition1));
			}
		}

		// 常量与已有数据做比较：checkValue checkOp old column
		for (CheckCompareOp op : CheckCompareOp.values()) {
			Column checkColumn1 = new Column();
			checkColumn1.setName("TEST_Column");
			checkColumn1.setType(Types.BIGINT);
			checkColumn1.setTypeName("bigint");
			CheckAndPutCondition checkAndPutCondition1 = new CheckAndPutCondition(checkColumn1, op, 1234L, null);
			if (op == CheckCompareOp.IS_NULL || op == CheckCompareOp.IS_NOT_NULL) {
				// 已有数据与null做比较：old column is null
				Assert.assertEquals(" \"TEST_Column\" " + op.getOperatorString(), builder.buildCheckAndDeletePattern(checkAndPutCondition1));
			} else {
				Assert.assertEquals(" '1234'::bigint " + op.getOperatorString() + " \"TEST_Column\"", builder.buildCheckAndDeletePattern(checkAndPutCondition1));
			}
		}

		// 常量与已有数据做比较：checkValue checkOp old column, 已有数据为null时进行coalesce
		for (CheckCompareOp op : CheckCompareOp.values()) {
			Column checkColumn1 = new Column();
			checkColumn1.setName("TEST_Column");
			checkColumn1.setType(Types.BIGINT);
			checkColumn1.setTypeName("bigint");
			CheckAndPutCondition checkAndPutCondition1 = new CheckAndPutCondition(checkColumn1, op, 1234L, 0L);
			if (op == CheckCompareOp.IS_NULL || op == CheckCompareOp.IS_NOT_NULL) {
				// 已有数据与null做比较：old column is null
				Assert.assertEquals(" coalesce(\"TEST_Column\", '0'::bigint) " + op.getOperatorString(), builder.buildCheckAndDeletePattern(checkAndPutCondition1));
			} else {
				Assert.assertEquals(" '1234'::bigint " + op.getOperatorString() + " coalesce(\"TEST_Column\", '0'::bigint)", builder.buildCheckAndDeletePattern(checkAndPutCondition1));
			}
		}
	}
}
