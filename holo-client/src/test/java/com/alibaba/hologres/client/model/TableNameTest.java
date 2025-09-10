package com.alibaba.hologres.client.model;

import com.alibaba.hologres.client.exception.InvalidIdentifierException;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * TableName Tester.
 *
 * @version 1.0
 * @since
 *     <pre>12月 2, 2020</pre>
 */
public class TableNameTest {

    /** Method: valueOf(String schemaName, String tableName). */
    @Test
    public void testValueOfForSchemaNameTableName() throws Exception {
        TableName tn = TableName.valueOf("Test", "AAA");
        Assert.assertEquals("test", tn.getSchemaName());
        Assert.assertEquals("aaa", tn.getTableName());
        Assert.assertEquals("\"test\".\"aaa\"", tn.getFullName());
        tn = TableName.valueOf("\"Test\"", "\"AAA\"");
        Assert.assertEquals("Test", tn.getSchemaName());
        Assert.assertEquals("AAA", tn.getTableName());
        Assert.assertEquals("\"Test\".\"AAA\"", tn.getFullName());
        tn = TableName.valueOf("\"Te st\"", "\"A\"\"AA\"");
        Assert.assertEquals("Te st", tn.getSchemaName());
        Assert.assertEquals("A\"AA", tn.getTableName());
        Assert.assertEquals("\"Te st\".\"A\"\"AA\"", tn.getFullName());
    }

    /** 无引号，小写，无非法字符. Method: valueOf(String name). */
    @Test
    public void testValueOfName001() throws Exception {
        TableName tn = TableName.valueOf("aaa");
        Assert.assertEquals("public", tn.getSchemaName());
        Assert.assertEquals("aaa", tn.getTableName());
        Assert.assertEquals("\"public\".\"aaa\"", tn.getFullName());
    }

    /** 无引号，小写，有非法字符. Method: valueOf(String name). */
    @Test
    public void testValueOfName002() throws Exception {
        Assert.assertThrows(
                InvalidIdentifierException.class,
                () -> {
                    TableName tn = TableName.valueOf("a aa");
                });
    }

    /** 无引号，大写，无非法字符. Method: valueOf(String name). */
    @Test
    public void testValueOfName003() throws Exception {
        TableName tn = TableName.valueOf("aAa");
        Assert.assertEquals("public", tn.getSchemaName());
        Assert.assertEquals("aaa", tn.getTableName());
        Assert.assertEquals("\"public\".\"aaa\"", tn.getFullName());
    }

    /** 无引号，大写，有非法字符. Method: valueOf(String name). */
    @Test
    public void testValueOfName004() throws Exception {
        Assert.assertThrows(
                InvalidIdentifierException.class,
                () -> {
                    TableName tn = TableName.valueOf("a Aa");
                });
    }

    /** 无引号，小写，有非法字符. Method: valueOf(String name). */
    @Test
    public void testValueOfName005() throws Exception {
        Assert.assertThrows(
                InvalidIdentifierException.class,
                () -> {
                    TableName tn = TableName.valueOf("a\"aa");
                });
    }

    /** 有引号，小写，无特殊字符. Method: valueOf(String name). */
    @Test
    public void testValueOfName006() throws Exception {
        TableName tn = TableName.valueOf("\"aaa\"");
        Assert.assertEquals("public", tn.getSchemaName());
        Assert.assertEquals("aaa", tn.getTableName());
        Assert.assertEquals("\"public\".\"aaa\"", tn.getFullName());
    }

    /** 有引号，小写，有特殊字符. Method: valueOf(String name). */
    @Test
    public void testValueOfName007() throws Exception {
        TableName tn = TableName.valueOf("\"a aa\"");
        Assert.assertEquals("public", tn.getSchemaName());
        Assert.assertEquals("a aa", tn.getTableName());
        Assert.assertEquals("\"public\".\"a aa\"", tn.getFullName());
    }

    /** 有引号，大写，有特殊字符. Method: valueOf(String name). */
    @Test
    public void testValueOfName008() throws Exception {
        TableName tn = TableName.valueOf("\"a\"\"A\"");
        Assert.assertEquals("public", tn.getSchemaName());
        Assert.assertEquals("a\"A", tn.getTableName());
        Assert.assertEquals("\"public\".\"a\"\"A\"", tn.getFullName());
    }

    /** 有引号，大写，有.特殊字符. Method: valueOf(String name). */
    @Test
    public void testValueOfName009() throws Exception {
        TableName tn = TableName.valueOf("\"a.A\"");
        Assert.assertEquals("public", tn.getSchemaName());
        Assert.assertEquals("a.A", tn.getTableName());
        Assert.assertEquals("\"public\".\"a.A\"", tn.getFullName());
    }

    /** 无引号，有大写, 无特殊字符. Method: valueOf(String name). */
    @Test
    public void testValueOfName010() throws Exception {
        TableName tn = TableName.valueOf("a.B");
        Assert.assertEquals("a", tn.getSchemaName());
        Assert.assertEquals("b", tn.getTableName());
        Assert.assertEquals("\"a\".\"b\"", tn.getFullName());
    }

    /** 无引号，有大写, 无特殊字符. Method: valueOf(String name). */
    @Test
    public void testValueOfName011() throws Exception {
        TableName tn = TableName.valueOf("\"A\".B");
        Assert.assertEquals("A", tn.getSchemaName());
        Assert.assertEquals("b", tn.getTableName());
        Assert.assertEquals("\"A\".\"b\"", tn.getFullName());
    }

    /** 无引号，有大写, 无特殊字符. Method: valueOf(String name). */
    @Test
    public void testValueOfName012() throws Exception {
        TableName tn = TableName.valueOf("Ab.\"B\"");
        Assert.assertEquals("ab", tn.getSchemaName());
        Assert.assertEquals("B", tn.getTableName());
        Assert.assertEquals("\"ab\".\"B\"", tn.getFullName());
    }

    /** Method: equals(Object o). */
    @Test
    public void testEquals() throws Exception {
        Assert.assertEquals(TableName.valueOf("a.b"), TableName.valueOf("a", "b"));
        Assert.assertEquals(TableName.valueOf("A.B"), TableName.valueOf("a", "b"));
        Assert.assertEquals(TableName.valueOf("\"A\".\"B\""), TableName.valueOf("\"A\"", "\"B\""));
        Assert.assertEquals(
                TableName.valueOf("\"A A\".\"B\"\"B\""),
                TableName.valueOf("\"A A\"", "\"B\"\"B\""));
    }
}
