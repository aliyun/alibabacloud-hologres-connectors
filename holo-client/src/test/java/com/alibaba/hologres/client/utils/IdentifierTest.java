package com.alibaba.hologres.client.utils;

import org.testng.Assert;
import org.testng.annotations.Test;

/** Test Postgres Identifier quotation. */
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

    @Test
    public void testQuoteIdentifierReservedKeywords() {
        Assert.assertEquals("\"all\"", IdentifierUtil.quoteIdentifier("all"));
        Assert.assertEquals("\"AND\"", IdentifierUtil.quoteIdentifier("AND"));
        Assert.assertEquals("\"ARRAY\"", IdentifierUtil.quoteIdentifier("ARRAY"));
        Assert.assertEquals("\"AS\"", IdentifierUtil.quoteIdentifier("AS"));
        Assert.assertEquals("\"ASC\"", IdentifierUtil.quoteIdentifier("ASC"));
        Assert.assertEquals("\"ASYMMETRIC\"", IdentifierUtil.quoteIdentifier("ASYMMETRIC"));
        Assert.assertEquals("\"AUTHORIZATION\"", IdentifierUtil.quoteIdentifier("AUTHORIZATION"));
    }

    @Test
    public void testForceQuoteOption() {
        Assert.assertEquals("\"normal\"", IdentifierUtil.quoteIdentifier("normal", false, true));
        Assert.assertEquals("\"NORMAL\"", IdentifierUtil.quoteIdentifier("NORMAL", true, true));
        Assert.assertEquals(
                "\"MixedCase\"", IdentifierUtil.quoteIdentifier("MixedCase", true, true));
        Assert.assertEquals(
                "\"_underscore\"", IdentifierUtil.quoteIdentifier("_underscore", false, true));
        Assert.assertEquals(
                "\"123StartWithNumber\"",
                IdentifierUtil.quoteIdentifier("123StartWithNumber", true, true));
        Assert.assertEquals(
                "\"\"\"DoubleQuotes\"\"\"",
                IdentifierUtil.quoteIdentifier("\"DoubleQuotes\"", true, true));
    }
}
