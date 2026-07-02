package com.alibaba.hologres.client.utils;

import org.testng.Assert;
import org.testng.annotations.Test;

/** Tests for {@link CommonUtil} parsing utilities. */
public class CommonUtilTest {

    // -------------------- parseLogicalPartitionColumnNames --------------------

    @Test
    public void testParseColumnNamesSingle() {
        Assert.assertEquals(
                new String[] {"ds"}, CommonUtil.parseLogicalPartitionColumnNames("\"ds\""));
    }

    @Test
    public void testParseColumnNamesMultiple() {
        Assert.assertEquals(
                new String[] {"ds", "kind"},
                CommonUtil.parseLogicalPartitionColumnNames("\"ds\",\"kind\""));
    }

    @Test
    public void testParseColumnNamesSkipsWhitespace() {
        Assert.assertEquals(
                new String[] {"ds", "kind"},
                CommonUtil.parseLogicalPartitionColumnNames("  \"ds\" , \"kind\"  "));
    }

    @Test
    public void testParseColumnNamesWithCommaInside() {
        Assert.assertEquals(
                new String[] {"co,lu", "kind"},
                CommonUtil.parseLogicalPartitionColumnNames("\"co,lu\", \"kind\""));
    }

    @Test
    public void testParseColumnNamesWithEscapedQuote() {
        Assert.assertEquals(
                new String[] {"co\"lu", "mn\"\"x"},
                CommonUtil.parseLogicalPartitionColumnNames("\"co\"\"lu\", \"mn\"\"\"\"x\""));
    }

    @Test
    public void testParseColumnNamesEmpty() {
        Assert.assertEquals(new String[] {}, CommonUtil.parseLogicalPartitionColumnNames(""));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParseColumnNamesUnquoted() {
        CommonUtil.parseLogicalPartitionColumnNames("ds, kind");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParseColumnNamesUnterminatedQuote() {
        CommonUtil.parseLogicalPartitionColumnNames("\"ds");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParseColumnNamesRejectSemicolon() {
        // names parser does not support `;`
        CommonUtil.parseLogicalPartitionColumnNames("\"ds\"; \"kind\"");
    }

    // -------------------- parseLogicalPartitionColumnValues --------------------

    @Test
    public void testParseValuesSingleRowSingleColumn() {
        String[][] rows = CommonUtil.parseLogicalPartitionColumnValues("\"20250101\"");
        Assert.assertEquals(1, rows.length);
        Assert.assertEquals(new String[] {"20250101"}, rows[0]);
    }

    @Test
    public void testParseValuesSingleRowMultiColumn() {
        String[][] rows = CommonUtil.parseLogicalPartitionColumnValues("\"20250101\", \"100\"");
        Assert.assertEquals(1, rows.length);
        Assert.assertEquals(new String[] {"20250101", "100"}, rows[0]);
    }

    @Test
    public void testParseValuesMultiRowSingleColumn() {
        String[][] rows =
                CommonUtil.parseLogicalPartitionColumnValues(
                        "\"20250101\"; \"20250102\"; \"20250103\"");
        Assert.assertEquals(3, rows.length);
        Assert.assertEquals(new String[] {"20250101"}, rows[0]);
        Assert.assertEquals(new String[] {"20250102"}, rows[1]);
        Assert.assertEquals(new String[] {"20250103"}, rows[2]);
    }

    @Test
    public void testParseValuesMultiRowMultiColumn() {
        String[][] rows =
                CommonUtil.parseLogicalPartitionColumnValues(
                        "\"2025-01-11\", \"100\"; \"2025-01-12\", \"200\"");
        Assert.assertEquals(2, rows.length);
        Assert.assertEquals(new String[] {"2025-01-11", "100"}, rows[0]);
        Assert.assertEquals(new String[] {"2025-01-12", "200"}, rows[1]);
    }

    @Test
    public void testParseValuesSkipsWhitespace() {
        String[][] rows =
                CommonUtil.parseLogicalPartitionColumnValues("  \"a\" , \"b\" ;  \"c\" , \"d\"  ");
        Assert.assertEquals(2, rows.length);
        Assert.assertEquals(new String[] {"a", "b"}, rows[0]);
        Assert.assertEquals(new String[] {"c", "d"}, rows[1]);
    }

    @Test
    public void testParseValuesWithSpecialCharsInside() {
        String[][] rows =
                CommonUtil.parseLogicalPartitionColumnValues("\"a,b\", \"c\"\"d\"; \"e;f\", \"g\"");
        Assert.assertEquals(2, rows.length);
        Assert.assertEquals(new String[] {"a,b", "c\"d"}, rows[0]);
        Assert.assertEquals(new String[] {"e;f", "g"}, rows[1]);
    }

    @Test
    public void testParseValuesEmpty() {
        Assert.assertEquals(new String[][] {}, CommonUtil.parseLogicalPartitionColumnValues(""));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParseValuesUnquoted() {
        CommonUtil.parseLogicalPartitionColumnValues("a, b");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParseValuesUnterminatedQuote() {
        CommonUtil.parseLogicalPartitionColumnValues("\"abc");
    }
}
