package com.alibaba.hologres.client.utils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test Postgres Identifier quotation.
 * */
public class IdentifierTest {

	@Test
	public void testLowercase() {
		Assert.assertEquals("abc", IdentifierUtil.quoteIdentifier("abc"));
		Assert.assertEquals("Abc", IdentifierUtil.quoteIdentifier("Abc"));
		Assert.assertEquals("\"Abc\"", IdentifierUtil.quoteIdentifier("Abc", true));
		Assert.assertEquals("\"$Abc\"", IdentifierUtil.quoteIdentifier("$Abc"));
		Assert.assertEquals("\"$abc\"", IdentifierUtil.quoteIdentifier("$abc"));
		Assert.assertEquals("\"abc$\"", IdentifierUtil.quoteIdentifier("abc$"));
		Assert.assertEquals("_abc", IdentifierUtil.quoteIdentifier("_abc"));
		Assert.assertEquals("\"9abc\"", IdentifierUtil.quoteIdentifier("9abc"));
		Assert.assertEquals("\"ab\"\"c\"", IdentifierUtil.quoteIdentifier("ab\"c"));
	}
}
