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
        Assert.assertEquals("\"lambda\"", IdentifierUtil.quoteIdentifier("lambda"));
    }

    @Test
    public void testHologresKeywords() {
        Object[][] keywords = {
            {"async", IdentifierUtil.UNRESERVED_KEYWORD},
            {"branch", IdentifierUtil.UNRESERVED_KEYWORD},
            {"dimensions", IdentifierUtil.UNRESERVED_KEYWORD},
            {"dynamic", IdentifierUtil.UNRESERVED_KEYWORD},
            {"external_files", IdentifierUtil.RESERVED_KEYWORD},
            {"hint", IdentifierUtil.UNRESERVED_KEYWORD},
            {"logical", IdentifierUtil.UNRESERVED_KEYWORD},
            {"metrics", IdentifierUtil.UNRESERVED_KEYWORD},
            {"mirrors", IdentifierUtil.UNRESERVED_KEYWORD},
            {"overwrite", IdentifierUtil.UNRESERVED_KEYWORD},
            {"parameter", IdentifierUtil.UNRESERVED_KEYWORD},
            {"proctime", IdentifierUtil.UNRESERVED_KEYWORD},
            {"purge", IdentifierUtil.UNRESERVED_KEYWORD},
            {"qualify", IdentifierUtil.RESERVED_KEYWORD},
            {"rebuild", IdentifierUtil.UNRESERVED_KEYWORD},
            {"recover", IdentifierUtil.UNRESERVED_KEYWORD},
            {"relationships", IdentifierUtil.UNRESERVED_KEYWORD},
            {"resume", IdentifierUtil.UNRESERVED_KEYWORD},
            {"semantic", IdentifierUtil.UNRESERVED_KEYWORD},
            {"stored", IdentifierUtil.UNRESERVED_KEYWORD},
            {"suspend", IdentifierUtil.UNRESERVED_KEYWORD},
            {"system_time", IdentifierUtil.UNRESERVED_KEYWORD},
            {"tag", IdentifierUtil.UNRESERVED_KEYWORD},
            {"try_cast", IdentifierUtil.COL_NAME_KEYWORD}
        };
        for (Object[] keyword : keywords) {
            Assert.assertEquals(
                    IdentifierUtil.keywordMap.get(keyword[0]),
                    keyword[1],
                    "Unexpected category for keyword " + keyword[0]);
        }
        Assert.assertEquals("\"external_files\"", IdentifierUtil.quoteIdentifier("external_files"));
        Assert.assertEquals("\"qualify\"", IdentifierUtil.quoteIdentifier("qualify"));
        Assert.assertEquals("\"try_cast\"", IdentifierUtil.quoteIdentifier("try_cast"));
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
