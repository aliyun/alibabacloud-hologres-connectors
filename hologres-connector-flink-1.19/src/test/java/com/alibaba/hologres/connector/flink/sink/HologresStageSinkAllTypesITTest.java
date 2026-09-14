package com.alibaba.hologres.connector.flink.sink;

import com.alibaba.hologres.connector.flink.HologresTestBase;
import com.alibaba.hologres.connector.flink.utils.JDBCUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;

import static com.alibaba.hologres.connector.flink.HologresTestUtils.checkResultWithTimeout;
import static com.alibaba.hologres.connector.flink.HologresTestUtils.expectedRowsToString;

/** Integration tests for STAGE mode sink with all supported data types. */
public class HologresStageSinkAllTypesITTest extends HologresTestBase {

    private String stageTable;

    public HologresStageSinkAllTypesITTest() throws IOException {}

    String prepareCreateTableSql =
            "CREATE TABLE TABLE_NAME (\n"
                    + "a integer NOT NULL,\n"
                    + "b text NOT NULL,\n"
                    + "c double precision,\n"
                    + "d boolean,\n"
                    + "e bigint,\n"
                    + "f date,\n"
                    + "g character varying,\n"
                    + "h timestamp with time zone,\n"
                    + "i real,\n"
                    + "j integer[],\n"
                    + "k bigint[],\n"
                    + "l real[],\n"
                    + "m double precision[],\n"
                    + "n boolean[],\n"
                    + "o text[],\n"
                    + "q numeric(6, 2),\n"
                    + "r timestamp without time zone,\n"
                    + "s smallint,\n"
                    + "t json,\n"
                    + "u jsonb,\n"
                    + "PRIMARY KEY (a)\n"
                    + ") with (table_group='tg_3');\n";

    public static final Object[][] EXPECTED =
            new Object[][] {
                new Object[] {
                    0,
                    "dim",
                    20.2007,
                    false,
                    652482,
                    new java.sql.Date(120, 6, 8),
                    "source_test",
                    Timestamp.valueOf("2020-07-10 16:28:07.737"),
                    8.58965,
                    "{464,98661,32489}",
                    "{8589934592,8589934593,8589934594}",
                    "{8.58967018,96.4666977,9345.16016}",
                    "{587897.464674600051,792343.64644599997,76.4646400000000028}",
                    "{t,t,f,t}",
                    "{monday,saturday,sunday}",
                    new BigDecimal("8119.21"),
                    Timestamp.valueOf("2020-07-10 16:28:07.737"),
                    2,
                    "{\"a\":\"bbbb\", \"c\":\"dddd\"}",
                    "{\"a\": \"bbbb\", \"c\": \"dddd\"}"
                },
            };

    public static final String[] FIELD_NAMES = {
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "q", "r", "s",
        "t", "u"
    };

    private static final String INSERT_STATEMENT =
            "INSERT INTO %s "
                    + " (a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,q,r,s,t,u) values ("
                    + "0,'dim',cast(20.2007 as double),false,652482,cast('2020-07-08' as date),"
                    + "'source_test',cast('2020-07-10 16:28:07.737' as TIMESTAMP_LTZ(6)),"
                    + "cast(8.58965 as float),"
                    + "cast(ARRAY [464,98661,32489] as array<int>),"
                    + "cast(ARRAY [8589934592,8589934593,8589934594] as array<bigint>),"
                    + "ARRAY[cast(8.58967 as float),cast(96.4667 as float),cast(9345.16 as float)],"
                    + "ARRAY[cast(587897.4646746 as double),cast(792343.646446 as double),cast(76.46464 as double)],"
                    + "cast(ARRAY [true,true,false,true] as array<boolean>),"
                    + "cast(ARRAY ['monday','saturday','sunday'] as array<STRING>),"
                    + "cast(8119.21 as numeric(6,2)),"
                    + "cast('2020-07-10 16:28:07.737' as timestamp),"
                    + "cast(2 as smallint),"
                    + "cast('{\"a\":\"bbbb\", \"c\":\"dddd\"}' as varchar),"
                    + "cast('{\"a\":\"bbbb\", \"c\":\"dddd\"}' as varchar)"
                    + ")";

    @Before
    public void setup() throws Exception {
        this.stageTable = "test_stage_all_type_" + randomSuffix;
        executeSql(prepareCreateTableSql.replace("TABLE_NAME", stageTable), false);
    }

    @After
    public void cleanup() throws SQLException {
        dropTable(stageTable);
    }

    @Test
    public void testStageSinkAllTypes() throws Exception {
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE stageAllTypes ("
                                + "a INT NOT NULL,"
                                + "b STRING NOT NULL,"
                                + "c DOUBLE,"
                                + "d BOOLEAN,"
                                + "e BIGINT,"
                                + "f DATE,"
                                + "g VARCHAR,"
                                + "h TIMESTAMP_LTZ(6),"
                                + "i FLOAT,"
                                + "j ARRAY<INT> NOT NULL,"
                                + "k ARRAY<BIGINT> NOT NULL,"
                                + "l ARRAY<FLOAT>,"
                                + "m ARRAY<DOUBLE>,"
                                + "n ARRAY<BOOLEAN>,"
                                + "o ARRAY<STRING>,"
                                + "q NUMERIC(6,2),"
                                + "r TIMESTAMP,"
                                + "s SMALLINT,"
                                + "t VARCHAR,"
                                + "u VARCHAR"
                                + ") WITH ("
                                + "'connector'='hologres',"
                                + "'sink.write-mode'='STAGE',"
                                + "'sink.on-conflict-action'='insertOrUpdate',"
                                + "'endpoint'='%s',"
                                + "'dbName'='%s',"
                                + "'tableName'='%s',"
                                + "'userName'='%s',"
                                + "'password'='%s'"
                                + ")",
                        endpoint, database, stageTable, username, password));

        tEnv.executeSql(String.format(INSERT_STATEMENT, "stageAllTypes")).await();

        checkResultWithTimeout(
                expectedRowsToString(EXPECTED),
                "select * from " + stageTable,
                FIELD_NAMES,
                JDBCUtils.getDbUrl(endpoint, database),
                username,
                password,
                30000);
    }

    @Test
    public void testStageSinkWithNullValues() throws Exception {
        // Test that null values are handled correctly
        String nullTable = "test_stage_null_" + randomSuffix;
        executeSql(
                "CREATE TABLE "
                        + nullTable
                        + " (a integer NOT NULL, b text, c double precision, d boolean, "
                        + "e bigint, f smallint, PRIMARY KEY (a))",
                false);

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE stageNullSink ("
                                + "a INT NOT NULL,"
                                + "b STRING,"
                                + "c DOUBLE,"
                                + "d BOOLEAN,"
                                + "e BIGINT,"
                                + "f SMALLINT"
                                + ") WITH ("
                                + "'connector'='hologres',"
                                + "'sink.write-mode'='STAGE',"
                                + "'sink.on-conflict-action'='insertOrUpdate',"
                                + "'endpoint'='%s',"
                                + "'dbName'='%s',"
                                + "'tableName'='%s',"
                                + "'userName'='%s',"
                                + "'password'='%s'"
                                + ")",
                        endpoint, database, nullTable, username, password));

        tEnv.executeSql(
                        "INSERT INTO stageNullSink (a,b,c,d,e,f) VALUES "
                                + "(1,cast(null as STRING),cast(null as DOUBLE),cast(null as BOOLEAN),cast(null as BIGINT),cast(null as SMALLINT)),"
                                + "(2,'hello',3.14,true,100,cast(5 as SMALLINT))")
                .await();

        Object[][] expected =
                new Object[][] {
                    new Object[] {1, null, null, null, null, null},
                    new Object[] {2, "hello", 3.14, true, 100L, 5},
                };

        checkResultWithTimeout(
                expectedRowsToString(expected),
                "select * from " + nullTable,
                new String[] {"a", "b", "c", "d", "e", "f"},
                JDBCUtils.getDbUrl(endpoint, database),
                username,
                password,
                30000);

        dropTable(nullTable);
    }
}
