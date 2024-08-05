package com.alibaba.ververica.connectors.hologres.dim;

import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.ververica.connectors.hologres.HologresTestBase;
import com.alibaba.ververica.connectors.hologres.HologresTestUtils;
import org.apache.commons.compress.utils.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertThat;

/** Tests for jdbc Dim, contains jdbc and jdbc_fixed. */
public class HologresDimTableITTest extends HologresTestBase {

    private final Boolean fixedConnectionMode = false;
    private String dimTableWithSchema;
    private String serialDimTable;
    private String one2ManyTable;
    private String partitionTable;

    private final Long[] j = {8589934592L, 8589934593L, 8589934594L};
    private final String[] n = {"monday", "saturday", "sunday"};
    private final Integer[] o = {464, 98661, 32489};
    private final Boolean[] p = {true, true, false, true};

    String prepareCreateTableSql =
            "BEGIN;\n"
                    + "CREATE TABLE IF NOT EXISTS TABLE_NAME (\n"
                    + "    a integer NOT NULL,\n"
                    + "    b text,\n"
                    + "    c double precision,\n"
                    + "    d boolean,\n"
                    + "    e bigint,\n"
                    + "    f date,\n"
                    + "    g character varying,\n"
                    + "    h timestamp with time zone,\n"
                    + "    i real,\n"
                    + "    j bigint[],\n"
                    + "    l real[],\n"
                    + "    m double precision[],\n"
                    + "    n text[],\n"
                    + "    o integer[],\n"
                    + "    p boolean[],\n"
                    + "    r numeric(6, 2),\n"
                    + "    s timestamp without time zone,\n"
                    + "    t timestamp with time zone,\n"
                    + "    u json,\n"
                    + "    v jsonb,\n"
                    + "    PRIMARY KEY (a)\n"
                    + ");\n"
                    + "CALL set_table_property ('TABLE_NAME', 'orientation', 'row');\n"
                    + "END;";
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
                        1,
                        "dim",
                        false,
                        "{\"a\":\"bbbb\", \"c\":\"dddd\"}",
                        "{\"a\": \"bbbb\", \"c\": \"dddd\"}"),
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
                        1,
                        "dim",
                        false,
                        "{\"a\":\"bbbb\", \"c\":\"dddd\"}",
                        "{\"a\": \"bbbb\", \"c\": \"dddd\"}")
            };

    private static final Object[][] prepareInsertTableValues =
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
                    new long[] {8589934592L, 8589934593L, 8589934594L},
                    new float[] {8.58967018F, 96.4666977F, 9345.16016F},
                    new double[] {587897.464674600051, 792343.64644599997, 76.4646400000000028},
                    new String[] {"monday", "saturday", "sunday"},
                    new int[] {464, 98661, 32489},
                    new boolean[] {true, true, false, true},
                    new BigDecimal("8119.23"),
                    Timestamp.valueOf("2020-07-10 16:28:07.737"),
                    Timestamp.valueOf("2020-07-10 16:28:07.737"),
                    "{\"a\":\"bbbb\", \"c\":\"dddd\"}",
                    "{\"a\": \"bbbb\", \"c\": \"dddd\"}"
                },
            };

    public HologresDimTableITTest() throws IOException {}

    @Before
    public void setUp() throws Exception {
        List<Row> data = new ArrayList<>();
        data.add(Row.of(1, "Hi"));
        data.add(Row.of(2, "Hello"));
        data.add(Row.of(5, "Hello word"));
        data.add(Row.of(6, "Narotu"));
        data.add(Row.of(7, "N/A"));
        data.add(Row.of(1, "Hi2"));
        createScanTable(data);

        this.dimTable = "test_dim_table_" + fixedConnectionMode + "_" + randomSuffix;
        executeSql(prepareCreateTableSql.replace("TABLE_NAME", this.dimTable), false);
        try (HoloClient client = getHoloClient()) {
            HologresTestUtils.insertValues(client, this.dimTable, prepareInsertTableValues);
        } catch (HoloClientException e) {
            throw new IllegalArgumentException(e);
        }

        executeSql("create schema if not exists test", false);
        this.dimTableWithSchema = "test." + dimTable;
        executeSql(prepareCreateTableSql.replace("TABLE_NAME", this.dimTableWithSchema), false);
        try (HoloClient client = getHoloClient()) {
            HologresTestUtils.insertValues(
                    client, this.dimTableWithSchema, prepareInsertTableValues);
        } catch (HoloClientException e) {
            throw new IllegalArgumentException(e);
        }
        this.one2ManyTable = "test_dim_one2many_" + fixedConnectionMode + "_" + randomSuffix;
        this.serialDimTable = "test_serial_dim_" + fixedConnectionMode + "_" + randomSuffix;
        this.partitionTable = "test_partition_dim_" + fixedConnectionMode + "_" + randomSuffix;
    }

    @After
    public void cleanUp() {
        dropTable(this.dimTable);
        dropTable(this.dimTableWithSchema);
        dropTable(this.one2ManyTable);
        dropTable(this.serialDimTable);
        dropTable(this.partitionTable);
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
                        + "u varchar,\n" // json
                        + "v varchar\n" // jsonb
                        + ") with ("
                        + "'connector'='hologres',\n"
                        + "'fixedConnectionMode'='"
                        + fixedConnectionMode
                        + "',\n"
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

        runTest();
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
                        + "u varchar,\n" // json
                        + "v varchar\n" // jsonb
                        + ") with ("
                        + "'connector'='hologres',\n"
                        + "'fixedConnectionMode'='"
                        + fixedConnectionMode
                        + "',\n"
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
        runTest();
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
                        + "u varchar,\n" // json
                        + "v varchar,\n" // jsonb
                        + "primary key(a) not enforced\n"
                        + ") with ("
                        + "'connector'='hologres',\n"
                        + "'fixedConnectionMode'='"
                        + fixedConnectionMode
                        + "',\n"
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

        runTest();
    }

    @Test
    public void testOne2ManyDimTable() {
        setUpOne2ManyTable();
        tEnv.executeSql(
                "create table dim"
                        + "(\n"
                        + "b int not null,\n"
                        + "a int not null,\n"
                        + "c varchar\n"
                        + ") with ("
                        + "'connector'='hologres',\n"
                        + "'fixedConnectionMode'='"
                        + fixedConnectionMode
                        + "',\n"
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
        try {
            runOne2ManyTest();
        } catch (Exception e) {
            if (fixedConnectionMode.equals("jdbc_fixed")) {
                assertThat(
                        e,
                        FlinkMatchers.containsMessage(
                                "Just hologres jdbc connection mode dimension table support one to many join."));
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void testDimTableWithPartitionTable() {
        String createSql =
                "BEGIN;\n"
                        + "CREATE TABLE TABLE_NAME(\n"
                        + "a integer NOT NULL,\n"
                        + "b text NOT NULL,\n"
                        + "ds text\n"
                        + ",PRIMARY KEY(a,ds)\n"
                        + ")\n"
                        + "PARTITION BY LIST(ds);"
                        + "CALL set_table_property('TABLE_NAME', 'orientation', 'row');\n"
                        + "CREATE TABLE TABLE_NAME_child1 PARTITION OF TABLE_NAME FOR VALUES IN('Hi');\n"
                        + "CREATE TABLE TABLE_NAME_child2 PARTITION OF TABLE_NAME FOR VALUES IN('Hello');"
                        + "END;";
        executeSql(createSql.replace("TABLE_NAME", partitionTable), false);
        final Object[][] prepareInsertTableValues =
                new Object[][] {
                    new Object[] {1, "child table 1", "Hi"},
                    new Object[] {
                        2, "child table 2", "Hello",
                    }
                };

        try (HoloClient client = getHoloClient()) {
            HologresTestUtils.insertValues(client, this.partitionTable, prepareInsertTableValues);
        } catch (HoloClientException e) {
            throw new IllegalArgumentException(e);
        }
        tEnv.executeSql(
                "create table dim"
                        + "(\n"
                        + "a int not null,\n"
                        + "b varchar not null,\n"
                        + "ds varchar\n"
                        + ") with ("
                        + "'connector'='hologres',\n"
                        + "'fixedConnectionMode'='"
                        + fixedConnectionMode
                        + "',\n"
                        + "'endpoint'='"
                        + endpoint
                        + "',\n"
                        + "'dbname'='"
                        + database
                        + "',\n"
                        + "'tablename'='"
                        + partitionTable
                        + "',\n"
                        + "'username'='"
                        + username
                        + "',\n"
                        + "'password'='"
                        + password
                        + "'\n"
                        + ")");
        try {
            TableResult result =
                    tEnv.executeSql(
                            String.format(
                                    "SELECT T.ip, a, b FROM MyTable AS T JOIN dim /*+ OPTIONS('fixedConnectionMode'='%s') */"
                                            + " FOR SYSTEM_TIME AS OF T.proctime "
                                            + "AS H ON T.name = H.a and T.ip = H.ds",
                                    fixedConnectionMode));

            Object[] actual = Lists.newArrayList(result.collect()).toArray();
            Object[] expected =
                    new Object[] {
                        Row.of("Hi", 1, "child table 1"), Row.of("Hello", 2, "child table 2"),
                    };
            Assert.assertArrayEquals(expected, actual);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testInsertIfNotExists() {
        setUpSerialDimTable();
        tEnv.executeSql(
                "create table dim(\n"
                        + "user_id int not null,\n"
                        + "user_name STRING not null,\n"
                        + "primary key(user_name) not enforced\n"
                        + ") with ("
                        + "'connector'='hologres',\n"
                        + "'fixedConnectionMode'='"
                        + fixedConnectionMode
                        + "',\n"
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
        runTestInsertIfNotExists();
    }

    @Test
    public void testDimTableWithLruCache() {
        testDimTableWithCache("lru");
    }

    private void runTest() {
        runTest("dim");
    }

    private void runTest(String tableName) {
        TableResult result =
                tEnv.executeSql(
                        String.format(
                                "SELECT T.ip, e, f, g, h, j, n, o, p, r, a, b, d, u, v FROM default_catalog.default_database.MyTable AS T JOIN %s /*+ OPTIONS('fixedConnectionMode'='%s') */"
                                        + " FOR SYSTEM_TIME AS OF T.proctime "
                                        + "AS H ON T.name = H.a",
                                tableName, fixedConnectionMode));

        Object[] actual = Lists.newArrayList(result.collect()).toArray();

        Assert.assertArrayEquals(expected, actual);
    }

    private void setUpOne2ManyTable() {
        String createSql =
                "BEGIN;\n"
                        + "CREATE TABLE TABLE_NAME(\n"
                        + "a integer NOT NULL,\n"
                        + "b integer NOT NULL,\n"
                        + "c text\n"
                        + ",PRIMARY KEY(a,b)\n"
                        + ");\n"
                        + "CALL set_table_property('TABLE_NAME', 'distribution_key', 'a');\n"
                        + "END;";
        executeSql(createSql.replace("TABLE_NAME", one2ManyTable), false);

        final Object[][] prepareInsertTableValues =
                new Object[][] {
                    new Object[] {
                        1, 1, "first row",
                    },
                    new Object[] {
                        1, 2, "Second row",
                    },
                    new Object[] {
                        1, 3, "Third row",
                    },
                };

        try (HoloClient client = getHoloClient()) {
            HologresTestUtils.insertValues(client, this.one2ManyTable, prepareInsertTableValues);
        } catch (HoloClientException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private void runOne2ManyTest() {
        try {
            TableResult result =
                    tEnv.executeSql(
                            String.format(
                                    "SELECT T.ip, a, b, c FROM MyTable AS T JOIN dim /*+ OPTIONS('fixedConnectionMode'='%s') */"
                                            + " FOR SYSTEM_TIME AS OF T.proctime "
                                            + "AS H ON T.name = H.a",
                                    fixedConnectionMode));

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
        } catch (Exception e) {
            if (fixedConnectionMode.equals("jdbc_fixed")) {
                assertThat(
                        e,
                        FlinkMatchers.containsMessage(
                                "Just hologres jdbc connection mode dimension table support one to many join."));
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private void setUpSerialDimTable() {
        String createSql =
                "BEGIN;\n"
                        + "CREATE TABLE TABLE_NAME(\n"
                        + "user_name text NOT NULL,\n"
                        + "user_id serial NOT NULL,\n"
                        + "PRIMARY KEY(user_name)\n"
                        + ");\n"
                        + "END;";
        executeSql(createSql.replace("TABLE_NAME", serialDimTable), false);
    }

    private void runTestInsertIfNotExists() {
        try {
            TableResult result =
                    tEnv.executeSql(
                            String.format(
                                    "SELECT H.user_name, H.user_id FROM default_catalog.default_database.MyTable AS T JOIN "
                                            + " dim /*+ OPTIONS('insertIfNotExists'='true', 'fixedConnectionMode'='%s')*/ FOR SYSTEM_TIME AS OF T.proctime "
                                            + "AS H ON T.ip = H.user_name",
                                    fixedConnectionMode));
            Object[] actual = Lists.newArrayList(result.collect()).toArray();
            Assert.assertEquals(6, actual.length);
            for (Object r : actual) {
                Row row = (Row) r;
                Assert.assertEquals(2, row.getArity());
                Assert.assertNotNull(row.getField(1));
            }
        } catch (Exception e) {
            if (fixedConnectionMode.equals("jdbc_fixed")) {
                assertThat(e, FlinkMatchers.containsMessage("FixedFe does not support."));
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private void testDimTableWithCache(String cacheStrategy) {
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
                        + "u varchar,\n" // json
                        + "v varchar\n" // jsonb
                        + ") with ("
                        + "'connector'='hologres',\n"
                        + "'fixedConnectionMode'='"
                        + fixedConnectionMode
                        + "',\n"
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
                        + "',\n"
                        + "'cache'='"
                        + cacheStrategy
                        + "'\n"
                        + ")");

        runTest();
    }
}
