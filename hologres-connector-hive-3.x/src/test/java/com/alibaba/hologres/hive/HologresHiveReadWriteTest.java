package com.alibaba.hologres.hive;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;

/** HologresHiveReadWriteTest. */
public class HologresHiveReadWriteTest extends HologresHiveTestBase {
    public HologresHiveReadWriteTest() throws IOException {}

    private static final String prepareCreateTableSql =
        "CREATE EXTERNAL TABLE TABLE_NAME\n"
            + "(\n"
            + "    a  int,\n"
            + "    b  bigint,\n"
            + "    c  boolean,\n"
            + "    d  float,\n"
            + "    e  double,\n"
            + "    f  string,\n"
            + "    g  string,\n"
            + "    h  string,\n"
            + "    i  timestamp,\n"
            + "    j  date,\n"
            + "    k  decimal(18,5),\n"
            + "    l  binary,\n "
            + "    m  ARRAY<int>,\n"
            + "    n  ARRAY<bigint>,\n"
            + "    o  ARRAY<float>,\n"
            + "    p  ARRAY<double>,\n"
            + "    q  ARRAY<boolean>,\n"
            + "    r  ARRAY<string>\n"
            + ")\n";

    private static final String prepareInsertValuesSql =
        "insert into TABLE_NAME select 111, 222, 'false', 1.23, 2.34,'ccc','{\"a\":\"b\"}','{\"a\":\"b\"}', '2021-05-21 16:00:45.123', '2021-05-21','85.23', '\\x030405', "
            + "array(1, 2, 3),"
            + "array(1000000000, 2000000000, 3000000000),"
            + "array(cast(1.1 as float), cast(1.2 as float)),"
            + "array(cast(2.1 as double), cast(2.2 as double)),"
            + "array(false, true, false),"
            + "array('a','b','c')\n";

    @Before
    public void setup() throws SQLException {
        try (Connection conn = getHoloConnection()) {
            try (Statement statement = conn.createStatement()) {
                // 在hologres中创建表
                statement.execute(
                    "create table if not exists hive_customer_type(a int, b int8,c bool, d real, "
                        + "e float8, f varchar(5), g json, h jsonb,"
                        + "i timestamptz,j date, k numeric(18,5), l bytea, "
                        + "m int[], n bigint[], o real[], p double precision[], "
                        + "q bool[], r text[])");
                statement.execute("truncate hive_customer_type");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        try (Connection connection =
                 DriverManager.getConnection(hiveJdbcUrl, hiveUsername, hivePassword)) {
            try (Statement statement = connection.createStatement()) {
                // hive中创建本地表并插入数据
                statement.execute(prepareCreateTableSql.replace("TABLE_NAME", "customer_hive_local").replace("EXTERNAL", ""));
                statement.execute(prepareInsertValuesSql.replace("TABLE_NAME", "customer_hive_local"));
                // hive 中创建holo外表
                statement.execute(
                    String.format(
                        prepareCreateTableSql.replace("TABLE_NAME", "customer_to_holo")
                            + "STORED BY 'com.alibaba.hologres.hive.HoloStorageHandler'\n"
                            + "TBLPROPERTIES (\n"
                            + "    \"hive.sql.jdbc.driver\" = \"org.postgresql.Driver\",\n"
                            + "    \"hive.sql.jdbc.url\" = \"%s\",\n"
                            + "    \"hive.sql.username\" = \"%s\",\n"
                            + "    \"hive.sql.password\" = \"%s\",\n"
                            + "    \"hive.sql.table\" = \"hive_customer_type\",\n"
                            + "    \"hive.sql.write_mode\" = \"INSERT_OR_UPDATE\",\n"
                            + "    \"hive.sql.copy_write_mode\" = \"true\",\n"
                            + "    \"hive.sql.copy_write_max_connections_number\" = \"20\",\n"
                            + "    \"hive.sql.dirty_data_check\" = \"true\"\n"
                            + ")",
                        holoJdbcUrl,
                        holoUsername,
                        holoPassword));
            }
        }
    }

    /**
     * insert into customer_to_holo select 111,222 ...
     * 此方式Hive给到HoloSerDe的是XWritable类型
     */
    @Test
    public void holoWriteTest() throws SQLException {
        try (Connection connection =
                 DriverManager.getConnection(hiveJdbcUrl, hiveUsername, hivePassword)) {
            try (Statement statement = connection.createStatement()) {
                System.out.println(
                    prepareInsertValuesSql.replace("TABLE_NAME", "customer_to_holo"));
                statement.execute(prepareInsertValuesSql.replace("TABLE_NAME", "customer_to_holo"));
            }
        }
        checkResult();
    }


    /**
     * insert into customer_to_holo select * from customer_hive_local;
     * 此方式Hive给到HoloSerDe的是LazyX类型
     */
    @Test
    public void holoReadWriteTest() throws SQLException {
        try (Connection connection =
                 DriverManager.getConnection(hiveJdbcUrl, hiveUsername, hivePassword)) {
            try (Statement statement = connection.createStatement()) {
                System.out.println(
                    prepareInsertValuesSql.replace("TABLE_NAME", "customer_to_holo"));
                statement.execute(prepareInsertValuesSql.replace("TABLE_NAME", "customer_to_holo"));
            }
        }
        checkResult();
    }

    private void checkResult() throws SQLException {
        // 检查hologres中插入的数据
        try (Connection conn = getHoloConnection()) {
            try (Statement statement = conn.createStatement()) {
                ResultSet rs = statement.executeQuery("select * from hive_customer_type");
                if (rs.next()) {
                    Assert.assertEquals(111, rs.getInt(1));
                    Assert.assertEquals(222, rs.getLong(2));
                    Assert.assertEquals(false, rs.getBoolean(3));
                    Assert.assertEquals(1.23f, rs.getFloat(4), 0.0001);
                    Assert.assertEquals(2.34d, rs.getDouble(5), 0.0001);
                    Assert.assertEquals("ccc", rs.getString(6));
                    Assert.assertEquals("{\"a\":\"b\"}", rs.getString(7));
                    Assert.assertEquals("{\"a\": \"b\"}", rs.getString(8));
                    Assert.assertEquals(
                        Timestamp.valueOf("2021-05-21 16:00:45.123"), rs.getTimestamp(9));
                    Assert.assertEquals(Date.valueOf("2021-05-21"), rs.getDate(10));
                    Assert.assertEquals(
                        new BigDecimal("85.23000"),
                        rs.getBigDecimal(11));
                    Assert.assertEquals(
                        Arrays.toString((new byte[] {120, 48, 51, 48, 52, 48, 53, 0, 0, 0})),
                        Arrays.toString(rs.getBytes(12)));
                    Assert.assertEquals("{1,2,3}", rs.getObject(13).toString());
                    Assert.assertEquals(
                        "{1000000000,2000000000,3000000000}", rs.getObject(14).toString());
                    Assert.assertEquals("{1.10000002,1.20000005}", rs.getObject(15).toString());
                    Assert.assertEquals("{2.10000000000000009,2.20000000000000018}", rs.getObject(16).toString());
                    Assert.assertEquals("{f,t,f}", rs.getObject(17).toString());
                    Assert.assertEquals("{a,b,c}", rs.getObject(18).toString());
                } else {
                    throw new RuntimeException("have no values");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
