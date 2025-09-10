/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.utils;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloClientTestBase;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.TableName;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;

/** ConnectionUtil单元测试用例. */
public class ConnectionUtilTest extends HoloClientTestBase {

    @Test
    public void testReplaceJdbcUrlEndpoint() {
        String originalJdbcUrl =
                "jdbc:postgresql://{ENDPOINT}:{PORT}/{DBNAME}?ApplicationName={APPLICATION_NAME}&reWriteBatchedInserts=true";
        String newEndpoint = "127.0.0.1:80";
        String expect =
                "jdbc:postgresql://127.0.0.1:80/{DBNAME}?ApplicationName={APPLICATION_NAME}&reWriteBatchedInserts=true";
        String newJdbcUrl = ConnectionUtil.replaceJdbcUrlEndpoint(originalJdbcUrl, newEndpoint);
        Assert.assertEquals(newJdbcUrl, expect);
    }

    @Test
    public void testMetaDataGetColumns() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String schema1 = "test_schema";
            String createSchema1 = "create schema if not exists " + schema1;
            String tableName1 = schema1 + "." + "holo_client_get_columns_001";
            String dropSql1 = "drop table if exists " + tableName1;
            String createSql1 =
                    "create table " + tableName1 + "(a int not null,b int,primary key(a))";
            execute(conn, new String[] {createSchema1, dropSql1, createSql1});

            String schema2 = "\"test%schema\"";
            String createSchema2 = "create schema if not exists " + schema2;
            String tableName2 = schema1 + "." + "holo_client_get_columns_0_1";
            String dropSql2 = "drop table if exists " + tableName2;
            String createSql2 =
                    "create table " + tableName2 + "(c text not null,d text,primary key(c))";
            execute(conn, new String[] {createSchema2, dropSql2, createSql2});

            String schema3 = "test_____ma";
            String createSchema3 = "create schema if not exists " + schema3;
            String tableName3 = schema3 + "." + "\"holo_client_get_co___ns_0\\%1\"";
            String dropSql3 = "drop table if exists " + tableName3;
            String createSql3 = "create table " + tableName3 + "(e bigint,f bigint,primary key(e))";
            execute(conn, new String[] {createSchema3, dropSql3, createSql3});

            String schema4 = "\"test%a\"";
            String createSchema4 = "create schema if not exists " + schema4;
            String tableName4 = schema4 + "." + "\"holo_client_get_columns\\%1\"";
            String dropSql4 = "drop table if exists " + tableName4;
            String createSql4 =
                    "create table " + tableName4 + "(g boolean,h boolean,primary key(g))";
            execute(conn, new String[] {createSchema4, dropSql4, createSql4});

            String schema5 = "\"test\\\\%\\a\"";
            String createSchema5 = "create schema if not exists " + schema5;
            String tableName5 = schema5 + "." + "\"holo_client_get_columns\\\\%\\1\"";
            String dropSql5 = "drop table if exists " + tableName5;
            String createSql5 =
                    "create table " + tableName5 + "(i timestamptz,j timestamptz,primary key(i))";
            execute(conn, new String[] {createSchema5, dropSql5, createSql5});

            try {
                Column[] table1Columns =
                        ConnectionUtil.getTableSchema(conn, TableName.valueOf(tableName1))
                                .getColumnSchema();
                Assert.assertEquals(table1Columns.length, 2);
                Assert.assertEquals(table1Columns[0].getName(), "a");
                Assert.assertEquals(table1Columns[1].getName(), "b");
                Column[] table2Columns =
                        ConnectionUtil.getTableSchema(conn, TableName.valueOf(tableName2))
                                .getColumnSchema();
                Assert.assertEquals(table2Columns.length, 2);
                Assert.assertEquals(table2Columns[0].getName(), "c");
                Assert.assertEquals(table2Columns[1].getName(), "d");
                Column[] table3Columns =
                        ConnectionUtil.getTableSchema(conn, TableName.valueOf(tableName3))
                                .getColumnSchema();
                Assert.assertEquals(table3Columns.length, 2);
                Assert.assertEquals(table3Columns[0].getName(), "e");
                Assert.assertEquals(table3Columns[1].getName(), "f");
                Column[] table4Columns =
                        ConnectionUtil.getTableSchema(conn, TableName.valueOf(tableName4))
                                .getColumnSchema();
                Assert.assertEquals(table4Columns.length, 2);
                Assert.assertEquals(table4Columns[0].getName(), "g");
                Assert.assertEquals(table4Columns[1].getName(), "h");
                Column[] table5Columns =
                        ConnectionUtil.getTableSchema(conn, TableName.valueOf(tableName5))
                                .getColumnSchema();
                Assert.assertEquals(table5Columns.length, 2);
                Assert.assertEquals(table5Columns[0].getName(), "i");
                Assert.assertEquals(table5Columns[1].getName(), "j");
            } finally {
                execute(conn, new String[] {dropSql1});
                execute(conn, new String[] {dropSql2});
                execute(conn, new String[] {dropSql3});
                execute(conn, new String[] {dropSql4});
                execute(conn, new String[] {dropSql5});
            }
        }
    }
}
