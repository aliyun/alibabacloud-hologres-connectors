package com.alibaba.hologres.connector.flink.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.testutils.FlinkMatchers;

import com.alibaba.hologres.connector.flink.HologresTestBase;
import com.alibaba.hologres.connector.flink.HologresTestUtils;
import com.alibaba.hologres.connector.flink.config.DirtyDataStrategy;
import com.alibaba.hologres.connector.flink.utils.JDBCUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.hologres.connector.flink.HologresTestUtils.expectedRowsToString;
import static org.hamcrest.MatcherAssert.assertThat;

/** Test for {@link DirtyDataStrategy} of jdbc sink. */
@RunWith(Parameterized.class)
public class HologresSinkDirtyDataStrategyTest extends HologresTestBase {
    boolean sinkV2;
    DirtyDataStrategy dirtyDataStrategy;

    @Parameterized.Parameters(name = "sinkV2, dirtyDataStrategy = {0}")
    public static List<Tuple2<Boolean, String>> parameters() {
        return Arrays.asList(
                Tuple2.of(false, "EXCEPTION"),
                Tuple2.of(false, "SKIP"),
                Tuple2.of(true, "EXCEPTION"),
                Tuple2.of(true, "SKIP"));
    }

    public HologresSinkDirtyDataStrategyTest(Tuple2<Boolean, String> tuple) throws IOException {
        this.sinkV2 = tuple.f0;
        this.dirtyDataStrategy = DirtyDataStrategy.valueOf(tuple.f1);
    }

    String createTableSql =
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
                    + "p time,\n"
                    + "q numeric(6, 2),\n"
                    + "r timestamp without time zone,\n"
                    + "s smallint,\n"
                    + "t json,\n"
                    + "u jsonb,\n"
                    + "PRIMARY KEY (a)\n"
                    + ");\n";

    public static final Object[][] EXPECTED =
            new Object[][] {
                new Object[] {
                    1,
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
                    new java.sql.Time(123456789L), // "18:17:36.789"
                    new BigDecimal("8119.21"),
                    Timestamp.valueOf("2020-07-10 16:28:07.737"),
                    2,
                    "{\"a\":\"bbbb\", \"c\":\"dddd\"}",
                    "{\"a\": \"bbbb\", \"c\": \"dddd\"}"
                },
            };
    public static final String[] FIELD_NAMES = {
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r",
        "s", "t", "u"
    };

    @Before
    public void setup() throws Exception {
        this.sinkTable =
                "test_sink_table_all_type_"
                        + dirtyDataStrategy.name().toLowerCase()
                        + "_"
                        + randomSuffix;
        executeSql(createTableSql.replace("TABLE_NAME", sinkTable), false);
    }

    @After
    public void cleanup() {
        dropTable(sinkTable);
    }

    @Test
    public void testSinkTableNoDirtyData() throws Exception {
        tEnv.executeSql(
                "create table sinkTable"
                        + "(\n"
                        + "a int not null,\n"
                        + "b STRING not null,\n"
                        + "c double,\n"
                        + "d boolean,\n"
                        + "e bigint,\n"
                        + "f date,\n"
                        + "g varchar,\n"
                        + "h TIMESTAMP,\n"
                        + "i float,\n"
                        + "j array<int> not null,\n"
                        + "k array<bigint> not null,\n"
                        + "l array<float>,\n"
                        + "m array<double>,\n"
                        + "n array<boolean>,\n"
                        + "o array<STRING>,\n"
                        + "p time,\n"
                        + "q numeric(6,2),\n"
                        + "r timestamp,\n"
                        + "s smallint,\n"
                        + "t varchar,\n"
                        + "u varchar\n"
                        + ") with ("
                        + "'connector'='hologres',\n"
                        + "'sink.write-mode'='INSERT',\n"
                        + "'sink.on-conflict-action'='INSERT_OR_UPDATE',\n"
                        + "'sink.remove-u0000-in-text.enabled'='true',\n"
                        + "'sink.api-version-v2.enabled'='"
                        + sinkV2
                        + "',\n"
                        + "'sink.insert.dirty-data-strategy'='"
                        + dirtyDataStrategy.name()
                        + "',\n"
                        + "'endpoint'='"
                        + endpoint
                        + "',\n"
                        + "'dbname'='"
                        + database
                        + "',\n"
                        + "'tablename'='"
                        + sinkTable
                        + "',\n"
                        + "'username'='"
                        + username
                        + "',\n"
                        + "'password'='"
                        + password
                        + "'\n"
                        + ")");
        String insertStatement =
                "INSERT INTO %s "
                        + " (a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u) values ("
                        + "1,'dim',cast(20.2007 as double),false,652482,cast('2020-07-08' as date),'source_\u0000test\u0000',cast('2020-07-10 16:28:07.737' as timestamp),"
                        + "cast(8.58965 as float),cast(ARRAY [464,98661,32489] as array<int>),cast(ARRAY [8589934592,8589934593,8589934594] as array<bigint>),"
                        + "ARRAY[cast(8.58967 as float),cast(96.4667 as float),cast(9345.16 as float)], ARRAY [cast(587897.4646746 as double),cast(792343.646446 as double),cast(76.46464 as double)],"
                        + "cast(ARRAY [true,true,false,true] as array<boolean>),cast(ARRAY ['monday','saturday','sunday'] as array<STRING>),cast('18:17:36.789' as time),cast(8119.21 as numeric(6,2)), "
                        + "cast('2020-07-10 16:28:07.737' as timestamp), cast(2 as smallint), cast('{\"a\":\"bb\u0000bb\", \"c\":\"dddd\"}' as varchar), cast('{\"a\":\"bb\u0000bb\", \"c\":\"dddd\"}' as varchar)"
                        + ")";
        tEnv.executeSql(String.format(insertStatement, "sinkTable")).await();
        HologresTestUtils.checkResultWithTimeout(
                expectedRowsToString(EXPECTED),
                "select * from " + sinkTable,
                FIELD_NAMES,
                JDBCUtils.getDbUrl(endpoint, database),
                username,
                password,
                10000);
    }

    @Test
    public void testSinkTableDirtyDataU0000() throws Exception {
        tEnv.executeSql(
                "create table sinkTable"
                        + "(\n"
                        + "a int not null,\n"
                        + "b STRING not null,\n"
                        + "c double,\n"
                        + "d boolean,\n"
                        + "e bigint,\n"
                        + "f date,\n"
                        + "g varchar,\n"
                        + "h TIMESTAMP,\n"
                        + "i float,\n"
                        + "j array<int> not null,\n"
                        + "k array<bigint> not null,\n"
                        + "l array<float>,\n"
                        + "m array<double>,\n"
                        + "n array<boolean>,\n"
                        + "o array<STRING>,\n"
                        + "p time,\n"
                        + "q numeric(6,2),\n"
                        + "r timestamp,\n"
                        + "s smallint,\n"
                        + "t varchar,\n"
                        + "u varchar\n"
                        + ") with ("
                        + "'connector'='hologres',\n"
                        + "'sink.write-mode'='INSERT',\n"
                        + "'sink.on-conflict-action'='INSERT_OR_UPDATE',\n"
                        + "'sink.remove-u0000-in-text.enabled'='false',\n"
                        + "'sink.dirty-data-check.enabled'='false',\n"
                        + "'sink.api-version-v2.enabled'='"
                        + sinkV2
                        + "',\n"
                        + "'sink.insert.dirty-data-strategy'='"
                        + dirtyDataStrategy.name()
                        + "',\n"
                        + "'endpoint'='"
                        + endpoint
                        + "',\n"
                        + "'dbname'='"
                        + database
                        + "',\n"
                        + "'tablename'='"
                        + sinkTable
                        + "',\n"
                        + "'username'='"
                        + username
                        + "',\n"
                        + "'password'='"
                        + password
                        + "'\n"
                        + ")");
        String insertStatement =
                "INSERT INTO %s "
                        + " (a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u) values ("
                        + "1,'dim',cast(20.2007 as double),false,652482,cast('2020-07-08' as date),'source_test',cast('2020-07-10 16:28:07.737' as timestamp),"
                        + "cast(8.58965 as float),cast(ARRAY [464,98661,32489] as array<int>),cast(ARRAY [8589934592,8589934593,8589934594] as array<bigint>),"
                        + "ARRAY[cast(8.58967 as float),cast(96.4667 as float),cast(9345.16 as float)], ARRAY [cast(587897.4646746 as double),cast(792343.646446 as double),cast(76.46464 as double)],"
                        + "cast(ARRAY [true,true,false,true] as array<boolean>),cast(ARRAY ['monday','saturday','sunday'] as array<STRING>),cast('18:17:36.789' as time),cast(8119.21 as numeric(6,2)), "
                        + "cast('2020-07-10 16:28:07.737' as timestamp), cast(2 as smallint), cast('{\"a\":\"bbbb\", \"c\":\"dddd\"}' as varchar), cast('{\"a\":\"bbbb\", \"c\":\"dddd\"}' as varchar)"
                        + "), (2,'dim',cast(20.2007 as double),false,652482,cast('2020-07-08' as date),'source_test',cast('2020-07-10 16:28:07.737' as timestamp),"
                        + "cast(8.58965 as float),cast(ARRAY [464,98661,32489] as array<int>),cast(ARRAY [8589934592,8589934593,8589934594] as array<bigint>),"
                        + "ARRAY[cast(8.58967 as float),cast(96.4667 as float),cast(9345.16 as float)], ARRAY [cast(587897.4646746 as double),cast(792343.646446 as double),cast(76.46464 as double)],"
                        + "cast(ARRAY [true,true,false,true] as array<boolean>),cast(ARRAY ['monday','saturday','sunday'] as array<STRING>),cast('18:17:36.789' as time),cast(8119.21 as numeric(6,2)), "
                        + "cast('2020-07-10 16:28:07.737' as timestamp), cast(2 as smallint), cast('{\"a\":\"bb\u0000bb\", \"c\":\"dddd\"}' as varchar), cast('{\"a\":\"bb\u0000bb\", \"c\":\"dddd\"}' as varchar)"
                        + ")";
        if (dirtyDataStrategy.equals(DirtyDataStrategy.EXCEPTION)) {
            try {
                tEnv.executeSql(String.format(insertStatement, "sinkTable")).await();
            } catch (Exception e) {
                // Different versions of holo may have different error message, which is one of the
                // following:
                // invalid byte sequence for encoding \"SQL_ASCII\": 0x00
                // invalid byte sequence for encoding \"UTF-8\": 0x00
                assertThat(e, FlinkMatchers.containsMessage("invalid byte sequence for encoding"));
            }
        } else {
            // Skip dirty data, and write other data normally
            tEnv.executeSql(String.format(insertStatement, "sinkTable")).await();
        }
        HologresTestUtils.checkResultWithTimeout(
                expectedRowsToString(EXPECTED),
                "select * from " + sinkTable,
                FIELD_NAMES,
                JDBCUtils.getDbUrl(endpoint, database),
                username,
                password,
                10000);
    }
}
