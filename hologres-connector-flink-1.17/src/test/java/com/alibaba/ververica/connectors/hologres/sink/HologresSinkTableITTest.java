package com.alibaba.ververica.connectors.hologres.sink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.alibaba.ververica.connectors.hologres.HologresTestBase;
import com.alibaba.ververica.connectors.hologres.JDBCTestUtils;
import com.alibaba.ververica.connectors.hologres.utils.JDBCUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.stream.Collectors;

/** Tests for Sink. */
public class HologresSinkTableITTest extends HologresTestBase {

    protected EnvironmentSettings streamSettings;

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;

    public HologresSinkTableITTest() throws IOException {}

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
                    true,
                    new BigDecimal("8119.21"),
                },
            };

    public static String[] expectRows =
            Arrays.stream(EXPECTED)
                    .map(
                            row -> {
                                return Arrays.stream(row)
                                        .map(Object::toString)
                                        .collect(Collectors.joining(","));
                            })
                    .toArray(String[]::new);

    public static String[] fieldNames = {
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q"
    };

    @Before
    public void prepare() throws Exception {
        EnvironmentSettings.Builder streamBuilder =
                EnvironmentSettings.newInstance().inStreamingMode();
        this.streamSettings = streamBuilder.build();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env, streamSettings);
    }

    @Test
    public void testJDBCSinkTable() throws Exception {
        this.cleanTable(sinkTable);
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
                        + "p boolean,\n"
                        + "q numeric(6,2)\n"
                        + ") with ("
                        + "'connector'='hologres',\n"
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

        tEnv.executeSql(
                "INSERT INTO sinkTable "
                        + " (a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q) values ("
                        + "1,'dim',cast(20.2007 as double),false,652482,cast('2020-07-08' as date),'source_test',cast('2020-07-10 16:28:07.737' as timestamp),"
                        + "cast(8.58965 as float),cast(ARRAY [464,98661,32489] as array<int>),cast(ARRAY [8589934592,8589934593,8589934594] as array<bigint>),"
                        + "ARRAY[cast(8.58967 as float),cast(96.4667 as float),cast(9345.16 as float)], ARRAY [cast(587897.4646746 as double),cast(792343.646446 as double),cast(76.46464 as double)],"
                        + "cast(ARRAY [true,true,false,true] as array<boolean>),cast(ARRAY ['monday','saturday','sunday'] as array<STRING>),true,cast(8119.21 as numeric(6,2))"
                        + ")");

        JDBCTestUtils.checkResultWithTimeout(
                expectRows,
                sinkTable,
                fieldNames,
                JDBCUtils.getDbUrl(endpoint, database),
                username,
                password,
                10000);
    }

    @Test
    public void testJDBCSinkTableWithSchema() throws Exception {
        this.cleanTable("test." + sinkTable);
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
                        + "p boolean,\n"
                        + "q numeric(6,2)\n"
                        + ") with ("
                        + "'connector'='hologres',\n"
                        + "'endpoint'='"
                        + endpoint
                        + "',\n"
                        + "'dbname'='"
                        + database
                        + "',\n"
                        + "'tablename'='test."
                        + sinkTable
                        + "',\n"
                        + "'username'='"
                        + username
                        + "',\n"
                        + "'password'='"
                        + password
                        + "'\n"
                        + ")");

        tEnv.executeSql(
                "INSERT INTO sinkTable "
                        + " (a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q) values ("
                        + "1,'dim',cast(20.2007 as double),false,652482,cast('2020-07-08' as date),'source_test',cast('2020-07-10 16:28:07.737' as timestamp),"
                        + "cast(8.58965 as float),cast(ARRAY [464,98661,32489] as array<int>),cast(ARRAY [8589934592,8589934593,8589934594] as array<bigint>),"
                        + "ARRAY[cast(8.58967 as float),cast(96.4667 as float),cast(9345.16 as float)], ARRAY [cast(587897.4646746 as double),cast(792343.646446 as double),cast(76.46464 as double)],"
                        + "cast(ARRAY [true,true,false,true] as array<boolean>),cast(ARRAY ['monday','saturday','sunday'] as array<STRING>),true,cast(8119.21 as numeric(6,2))"
                        + ")");

        JDBCTestUtils.checkResultWithTimeout(
                expectRows,
                "test." + sinkTable,
                fieldNames,
                JDBCUtils.getDbUrl(endpoint, database),
                username,
                password,
                10000);
    }
}
