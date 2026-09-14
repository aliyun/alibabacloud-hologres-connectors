package com.alibaba.hologres.client.copy;

import com.alibaba.hologres.client.model.OnConflictAction;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

/** Unit tests for COPY SQL generation that do not require a Hologres instance. */
public class CopyUtilTest {
    @DataProvider(name = "bulkLoadBinaryCases")
    public Object[][] bulkLoadBinaryCases() {
        return new Object[][] {
            {
                CopyMode.BULK_LOAD,
                OnConflictAction.INSERT_OR_UPDATE,
                "copy test_table(id,name) from stdin with(format binary)"
            },
            {
                CopyMode.BULK_LOAD_ON_CONFLICT,
                OnConflictAction.INSERT_OR_UPDATE,
                "copy test_table(id,name) from stdin with(format binary, on_conflict update)"
            },
            {
                CopyMode.BULK_LOAD_ON_CONFLICT,
                OnConflictAction.INSERT_OR_IGNORE,
                "copy test_table(id,name) from stdin with(format binary, on_conflict ignore)"
            }
        };
    }

    @Test(dataProvider = "bulkLoadBinaryCases")
    public void testBuildBulkLoadBinarySql(
            CopyMode copyMode, OnConflictAction onConflictAction, String expectedSql) {
        String actualSql =
                CopyUtil.buildCopyInSql(
                        "test_table",
                        Arrays.asList("id", "name"),
                        CopyFormat.BINARY,
                        true,
                        "1",
                        onConflictAction,
                        copyMode,
                        false,
                        "",
                        true);
        Assert.assertEquals(actualSql, expectedSql);
    }

    @Test
    public void testBulkLoadCsvSqlIsUnchanged() {
        String actualSql =
                CopyUtil.buildCopyInSql(
                        "test_table",
                        Arrays.asList("id", "name"),
                        CopyFormat.CSV,
                        true,
                        "1",
                        OnConflictAction.INSERT_OR_UPDATE,
                        CopyMode.BULK_LOAD,
                        false);
        Assert.assertEquals(
                actualSql,
                "copy test_table(id,name) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N')");
    }

    @Test
    public void testCopyOutSqlWithEmptyColumnsUsesConstantProjection() {
        // 空投影会生成 "select  from tbl", 服务端不报语法错而是让 ORCA 崩在 arity 断言上,
        // 因此退化为常量投影
        String actualSql =
                CopyUtil.buildCopyOutSql(
                        "\"public\".\"test_table\"",
                        Collections.emptyList(),
                        null,
                        CopyFormat.ARROW,
                        null,
                        new HashSet<>());
        Assert.assertEquals(
                actualSql,
                "copy (select 1 from \"public\".\"test_table\") to stdout with (format arrow)");
        Assert.assertFalse(actualSql.contains("select  from"));
    }

    @Test
    public void testCopyOutSqlWithEmptyColumnsKeepsShardAndFilter() {
        String actualSql =
                CopyUtil.buildCopyOutSql(
                        "test_table",
                        Collections.emptyList(),
                        Arrays.asList(1, 2),
                        CopyFormat.ARROW_LZ4,
                        "where id > 0",
                        new HashSet<>());
        Assert.assertEquals(
                actualSql,
                "copy (select 1 from test_table where hg_shard_id in (1,2) and id > 0)"
                        + " to stdout with (format arrow_lz4)");
    }

    @Test
    public void testCopyOutSqlQuotesOnlyWhenNeeded() {
        // quoteIdentifier(column, true) 仅在必要时加引号: 小写简单标识符不加, 含大写的才加
        String actualSql =
                CopyUtil.buildCopyOutSql(
                        "test_table",
                        Arrays.asList("id", "MixedCase"),
                        null,
                        CopyFormat.ARROW,
                        null,
                        new HashSet<>());
        Assert.assertEquals(
                actualSql,
                "copy (select id,\"MixedCase\" from test_table) to stdout with (format arrow)");
    }

    @Test
    public void testCopyOutSqlWithColumnsIsUnchanged() {
        String actualSql =
                CopyUtil.buildCopyOutSql(
                        "test_table",
                        Arrays.asList("id", "name"),
                        null,
                        CopyFormat.ARROW,
                        null,
                        new HashSet<>());
        Assert.assertEquals(
                actualSql, "copy (select id,name from test_table) to stdout with (format arrow)");
    }
}
