package com.alibaba.ververica.connectors.hologres.dim;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;

import com.alibaba.ververica.connectors.hologres.HologresTestBase;
import org.apache.commons.compress.utils.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

/** Tests for Dim Table. */
public class HologresJDBCDimTableITTest extends HologresTestBase {

    protected EnvironmentSettings streamSettings;

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;

    public HologresJDBCDimTableITTest() throws IOException {}

    @Before
    public void prepare() {
        EnvironmentSettings.Builder streamBuilder =
                EnvironmentSettings.newInstance().inStreamingMode();

        this.streamSettings = streamBuilder.useBlinkPlanner().build();

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env, streamSettings);

        List<Row> data = new ArrayList<>();
        data.add(Row.of(1, "Hi"));
        data.add(Row.of(2, "Hello"));
        data.add(Row.of(5, "Hello word"));
        data.add(Row.of(6, "Narotu"));
        data.add(Row.of(7, "N/A"));
        data.add(Row.of(1, "Hi2"));

        createScanTable(data);
    }

    private void createScanTable(List<Row> data) {
        String dataId = TestValuesTableFactory.registerData(data);
        tEnv.executeSql(
                "create table MyTable (\n"
                        + "  name int,\n"
                        + "  ip STRING,\n"
                        + "  proctime as PROCTIME()\n"
                        + ") with (\n"
                        + "  'connector'='values',\n"
                        + "  'data-id'='"
                        + dataId
                        + "'\n"
                        + ")");
    }

    @Test
    public void testDimTable() {
        tEnv.executeSql(
                "create table dim"
                        + "(\n"
                        + "b STRING not null,\n"
                        + "a int not null,\n"
                        + "c double not null,\n"
                        + "d boolean,\n"
                        + "e bigint,\n"
                        + "f date,\n"
                        + "g varchar,\n"
                        + "h TIMESTAMP,\n"
                        + "i float,\n"
                        + "j array<bigint>,\n"
                        + "l array<float>,\n"
                        + "m array<double>,\n"
                        + "n array<STRING>,\n"
                        + "o array<int>,\n"
                        + "p array<boolean>,\n"
                        + "r Decimal(6,2),\n"
                        + "s boolean\n"
                        + ") with ("
                        + "'connector'='hologres',\n"
                        + "'endpoint'='"
                        + endpoint
                        + "',\n"
                        + "'dbname'='"
                        + database
                        + "',\n"
                        + "'tablename'='"
                        + dimTable
                        + "',\n"
                        + "'username'='"
                        + username
                        + "',\n"
                        + "'password'='"
                        + password
                        + "'\n"
                        + ")");

        TableResult result =
                tEnv.executeSql(
                        "SELECT T.ip, e, f, g, h, j, n, o, p, r, s, a, b, d FROM MyTable AS T JOIN dim "
                                + " FOR SYSTEM_TIME AS OF T.proctime "
                                + "AS H ON T.name = H.a");
        Long[] j = {8589934592L, 8589934593L, 8589934594L};
        Float[] l = {8.58967f, 96.4667f, 9345.16f};
        Double[] m = {587897.4646746, 792343.646446, 76.46464};
        String[] n = {"monday", "saturday", "sunday"};
        Integer[] o = {464, 98661, 32489};
        Boolean[] p = {true, true, false, true};

        Object[] actual = Lists.newArrayList(result.collect()).toArray();
        Object[] expected =
                new Object[] {
                    Row.of(
                            "Hi",
                            652482L,
                            LocalDate.of(2020, 7, 8),
                            "source_test",
                            LocalDateTime.of(
                                    LocalDate.of(2020, 07, 10), LocalTime.of(16, 28, 7, 737000000)),
                            j,
                            n,
                            o,
                            p,
                            BigDecimal.valueOf(811923, 2),
                            true,
                            1,
                            "dim",
                            false),
                    Row.of(
                            "Hi2",
                            652482L,
                            LocalDate.of(2020, 7, 8),
                            "source_test",
                            LocalDateTime.of(
                                    LocalDate.of(2020, 07, 10), LocalTime.of(16, 28, 7, 737000000)),
                            j,
                            n,
                            o,
                            p,
                            BigDecimal.valueOf(811923, 2),
                            true,
                            1,
                            "dim",
                            false),
                };
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testDimTableAsync() {
        tEnv.executeSql(
                "create table dim"
                        + "(\n"
                        + "b STRING not null,\n"
                        + "a int not null,\n"
                        + "c double not null,\n"
                        + "d boolean,\n"
                        + "e bigint,\n"
                        + "f date,\n"
                        + "g varchar,\n"
                        + "h TIMESTAMP,\n"
                        + "i float,\n"
                        + "j array<bigint> not null,\n"
                        + "l array<float> not null,\n"
                        + "m array<double>,\n"
                        + "n array<STRING>,\n"
                        + "o array<int>,\n"
                        + "p array<boolean>,\n"
                        + "r Decimal(6,2),\n"
                        + "s boolean\n"
                        + ") with ("
                        + "'connector'='hologres',\n"
                        + "'async'='true',\n"
                        + "'endpoint'='"
                        + endpoint
                        + "',\n"
                        + "'dbname'='"
                        + database
                        + "',\n"
                        + "'tablename'='"
                        + dimTable
                        + "',\n"
                        + "'username'='"
                        + username
                        + "',\n"
                        + "'password'='"
                        + password
                        + "'\n"
                        + ")");

        TableResult result =
                tEnv.executeSql(
                        "SELECT T.ip, e, f, g, h, j, n, o, p, r, s, a, b, d FROM MyTable AS T JOIN dim "
                                + " FOR SYSTEM_TIME AS OF T.proctime "
                                + "AS H ON T.name = H.a");
        Long[] j = {8589934592L, 8589934593L, 8589934594L};
        Float[] l = {8.58967f, 96.4667f, 9345.16f};
        Double[] m = {587897.4646746, 792343.646446, 76.46464};
        String[] n = {"monday", "saturday", "sunday"};
        Integer[] o = {464, 98661, 32489};
        Boolean[] p = {true, true, false, true};

        Object[] actual = Lists.newArrayList(result.collect()).toArray();
        Object[] expected =
                new Object[] {
                    Row.of(
                            "Hi",
                            652482L,
                            LocalDate.of(2020, 7, 8),
                            "source_test",
                            LocalDateTime.of(
                                    LocalDate.of(2020, 07, 10), LocalTime.of(16, 28, 7, 737000000)),
                            j,
                            n,
                            o,
                            p,
                            BigDecimal.valueOf(811923, 2),
                            true,
                            1,
                            "dim",
                            false),
                    Row.of(
                            "Hi2",
                            652482L,
                            LocalDate.of(2020, 7, 8),
                            "source_test",
                            LocalDateTime.of(
                                    LocalDate.of(2020, 07, 10), LocalTime.of(16, 28, 7, 737000000)),
                            j,
                            n,
                            o,
                            p,
                            BigDecimal.valueOf(811923, 2),
                            true,
                            1,
                            "dim",
                            false),
                };
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testDimTableWithPrimaryKey() {
        tEnv.executeSql(
                "create table dim"
                        + "(\n"
                        + "b STRING not null,\n"
                        + "a int not null,\n"
                        + "c double not null,\n"
                        + "d boolean,\n"
                        + "e bigint,\n"
                        + "f date,\n"
                        + "g varchar,\n"
                        + "h TIMESTAMP,\n"
                        + "i float,\n"
                        + "j array<bigint>,\n"
                        + "l array<float>,\n"
                        + "m array<double>,\n"
                        + "n array<STRING>,\n"
                        + "o array<int>,\n"
                        + "p array<boolean>,\n"
                        + "r Decimal(6,2),\n"
                        + "s boolean, \n"
                        + "primary key(a) not enforced\n"
                        + ") with ("
                        + "'connector'='hologres',\n"
                        + "'endpoint'='"
                        + endpoint
                        + "',\n"
                        + "'dbname'='"
                        + database
                        + "',\n"
                        + "'tablename'='"
                        + dimTable
                        + "',\n"
                        + "'username'='"
                        + username
                        + "',\n"
                        + "'password'='"
                        + password
                        + "'\n"
                        + ")");

        TableResult result =
                tEnv.executeSql(
                        "SELECT T.ip, e, f, g, h, j, n, o, p, r, s, a, b, d FROM MyTable AS T JOIN dim "
                                + " FOR SYSTEM_TIME AS OF T.proctime "
                                + "AS H ON T.name = H.a");
        Long[] j = {8589934592L, 8589934593L, 8589934594L};
        Float[] l = {8.58967f, 96.4667f, 9345.16f};
        Double[] m = {587897.4646746, 792343.646446, 76.46464};
        String[] n = {"monday", "saturday", "sunday"};
        Integer[] o = {464, 98661, 32489};
        Boolean[] p = {true, true, false, true};

        Object[] actual = Lists.newArrayList(result.collect()).toArray();
        Object[] expected =
                new Object[] {
                    Row.of(
                            "Hi",
                            652482L,
                            LocalDate.of(2020, 7, 8),
                            "source_test",
                            LocalDateTime.of(
                                    LocalDate.of(2020, 07, 10), LocalTime.of(16, 28, 7, 737000000)),
                            j,
                            n,
                            o,
                            p,
                            BigDecimal.valueOf(811923, 2),
                            true,
                            1,
                            "dim",
                            false),
                    Row.of(
                            "Hi2",
                            652482L,
                            LocalDate.of(2020, 7, 8),
                            "source_test",
                            LocalDateTime.of(
                                    LocalDate.of(2020, 07, 10), LocalTime.of(16, 28, 7, 737000000)),
                            j,
                            n,
                            o,
                            p,
                            BigDecimal.valueOf(811923, 2),
                            true,
                            1,
                            "dim",
                            false),
                };
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testOne2ManyDimTable() {
        tEnv.executeSql(
                "create table dim"
                        + "(\n"
                        + "b int not null,\n"
                        + "a int not null,\n"
                        + "c varchar\n"
                        + ") with ("
                        + "'connector'='hologres',\n"
                        + "'endpoint'='"
                        + endpoint
                        + "',\n"
                        + "'dbname'='"
                        + database
                        + "',\n"
                        + "'tablename'='"
                        + one2ManyTable
                        + "',\n"
                        + "'username'='"
                        + username
                        + "',\n"
                        + "'password'='"
                        + password
                        + "'\n"
                        + ")");

        TableResult result =
                tEnv.executeSql(
                        "SELECT T.ip, a, b, c FROM MyTable AS T JOIN dim "
                                + " FOR SYSTEM_TIME AS OF T.proctime "
                                + "AS H ON T.name = H.a");

        Object[] actual = Lists.newArrayList(result.collect()).toArray();
        Object[] expected =
                new Object[] {
                    Row.of("Hi", 1, 1, "first row"),
                    Row.of("Hi", 1, 2, "Second row"),
                    Row.of("Hi", 1, 3, "Third row"),
                    Row.of("Hi2", 1, 1, "first row"),
                    Row.of("Hi2", 1, 2, "Second row"),
                    Row.of("Hi2", 1, 3, "Third row"),
                };
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testInsertIfNotExists() throws SQLException {
        this.cleanTable(serialDimTable);
        tEnv.executeSql(
                "create table dim(\n"
                        + "user_id int not null,\n"
                        + "user_name STRING not null,\n"
                        + "primary key(user_name) not enforced\n"
                        + ") with ("
                        + "'connector'='hologres',\n"
                        + "'endpoint'='"
                        + endpoint
                        + "',\n"
                        + "'dbname'='"
                        + database
                        + "',\n"
                        + "'async'='true',\n"
                        + "'insertIfNotExists'='true',\n"
                        + "'tablename'='"
                        + serialDimTable
                        + "',\n"
                        + "'username'='"
                        + username
                        + "',\n"
                        + "'password'='"
                        + password
                        + "'\n"
                        + ")");

        TableResult result =
                tEnv.executeSql(
                        "SELECT H.user_name, H.user_id FROM MyTable AS T JOIN "
                                + " dim FOR SYSTEM_TIME AS OF T.proctime "
                                + "AS H ON T.ip = H.user_name");
        Object[] actual = Lists.newArrayList(result.collect()).toArray();
        Assert.assertEquals(6, actual.length);
        for (Object r : actual) {
            Row row = (Row) r;
            Assert.assertEquals(2, row.getArity());
            Assert.assertNotNull(row.getField(1));
        }
    }
}
