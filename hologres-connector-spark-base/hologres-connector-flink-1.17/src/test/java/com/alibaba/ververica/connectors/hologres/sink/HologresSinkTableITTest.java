/*
 *  Copyright (c) 2021, Alibaba Group;
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.ververica.connectors.hologres.sink;

import org.apache.flink.core.testutils.FlinkAssertions;

import com.alibaba.ververica.connectors.hologres.HologresTestBase;
import com.alibaba.ververica.connectors.hologres.utils.JDBCUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.ververica.connectors.hologres.HologresTestUtils.checkResultWithTimeout;
import static com.alibaba.ververica.connectors.hologres.HologresTestUtils.expectedRowsToString;

/** Tests for Sink. */
@RunWith(Parameterized.class)
public class HologresSinkTableITTest extends HologresTestBase {
    private final String sdkMode;
    private boolean fixedMode = false;
    private boolean copyMode = false;
    private boolean bulkLoad = false;
    private String sinkTableWithSchema;

    @Parameterized.Parameters(name = "use sdkMode = {0}")
    public static List<String> parameters() {
        return Arrays.asList("jdbc", "jdbc_copy", "jdbc_fixed", "bulkload");
    }

    public HologresSinkTableITTest(String sdkMode) throws IOException {
        this.sdkMode = sdkMode;
        switch (sdkMode) {
            case "jdbc_fixed":
                this.fixedMode = true;
                break;
            case "jdbc_copy":
                this.copyMode = true;
                break;
            case "bulkload":
                this.copyMode = true;
                this.bulkLoad = true;
                break;
        }
    }

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
                    + "p boolean,\n"
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
                    true,
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
        this.sinkTable = "test_sink_table_all_type_" + sdkMode + "_" + randomSuffix;
        this.sinkTableWithSchema = "test." + sinkTable;
        executeSql(prepareCreateTableSql.replace("TABLE_NAME", sinkTable), false);
        executeSql(prepareCreateTableSql.replace("TABLE_NAME", sinkTableWithSchema), false);
    }

    @After
    public void cleanup() throws SQLException {
        dropTable(sinkTableWithSchema);
        dropTable(sinkTable);
    }

    private static final String insertStatement =
            "INSERT INTO %s "
                    + " (a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u) values ("
                    + "1,'dim',cast(20.2007 as double),false,652482,cast('2020-07-08' as date),'source_test',cast('2020-07-10 16:28:07.737' as timestamp),"
                    + "cast(8.58965 as float),cast(ARRAY [464,98661,32489] as array<int>),cast(ARRAY [8589934592,8589934593,8589934594] as array<bigint>),"
                    + "ARRAY[cast(8.58967 as float),cast(96.4667 as float),cast(9345.16 as float)], ARRAY [cast(587897.4646746 as double),cast(792343.646446 as double),cast(76.46464 as double)],"
                    + "cast(ARRAY [true,true,false,true] as array<boolean>),cast(ARRAY ['monday','saturday','sunday'] as array<STRING>),true,cast(8119.21 as numeric(6,2)), "
                    + "cast('2020-07-10 16:28:07.737' as timestamp), cast(2 as smallint), cast('{\"a\":\"bbbb\", \"c\":\"dddd\"}' as varchar), cast('{\"a\":\"bbbb\", \"c\":\"dddd\"}' as varchar)"
                    + ")";

    @Test
    public void testSinkTable() throws Exception {
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
                        + "q numeric(6,2),\n"
                        + "r timestamp,\n"
                        + "s smallint,\n"
                        + "t varchar,\n"
                        + "u varchar\n"
                        + ") with ("
                        + "'connector'='hologres',\n"
                        + "'fixedConnectionMode'='"
                        + fixedMode
                        + "',\n"
                        + "'jdbcCopyWriteMode'='"
                        + copyMode
                        + "',\n"
                        + "'bulkLoad'='"
                        + bulkLoad
                        + "',\n"
                        + "'mutatetype'='insertorignore',\n"
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

        tEnv.executeSql(String.format(insertStatement, "sinkTable")).await();

        checkResultWithTimeout(
                expectedRowsToString(EXPECTED),
                "select * from " + sinkTable,
                FIELD_NAMES,
                JDBCUtils.getDbUrl(endpoint, database),
                username,
                password,
                10000);
    }

    @Test
    public void testSinkTableWithSchema() throws Exception {
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
                        + "q numeric(6,2),\n"
                        + "r timestamp,\n"
                        + "s smallint,\n"
                        + "t varchar,\n"
                        + "u varchar\n"
                        + ") with ("
                        + "'connector'='hologres',\n"
                        + "'fixedConnectionMode'='"
                        + fixedMode
                        + "',\n"
                        + "'jdbcCopyWriteMode'='"
                        + copyMode
                        + "',\n"
                        + "'bulkLoad'='"
                        + bulkLoad
                        + "',\n"
                        + "'jdbcCopyWriteFormat'='text',\n"
                        + "'mutatetype'='insertorignore',\n"
                        + "'connectionPoolName'='pool',\n"
                        + "'endpoint'='"
                        + endpoint
                        + "',\n"
                        + "'dbname'='"
                        + database
                        + "',\n"
                        + "'tablename'='"
                        + sinkTableWithSchema
                        + "',\n"
                        + "'username'='"
                        + username
                        + "',\n"
                        + "'password'='"
                        + password
                        + "'\n"
                        + ")");

        tEnv.executeSql(String.format(insertStatement, "sinkTable")).await();

        checkResultWithTimeout(
                expectedRowsToString(EXPECTED),
                "select * from " + sinkTableWithSchema,
                FIELD_NAMES,
                JDBCUtils.getDbUrl(endpoint, database),
                username,
                password,
                10000);
    }

    @Test
    public void testSinkTableWithException() throws Exception {
        if (!sdkMode.equals("jdbc_copy") && !sdkMode.equals("bulkload")) {
            return;
        }
        tEnv.executeSql(
                "create table sinkTable"
                        + "(\n"
                        + "a int,\n"
                        + "b STRING\n"
                        + ") with ("
                        + "'connector'='hologres',\n"
                        + "'fixedConnectionMode'='"
                        + fixedMode
                        + "',\n"
                        + "'jdbcCopyWriteMode'='"
                        + copyMode
                        + "',\n"
                        + "'bulkLoad'='"
                        + bulkLoad
                        + "',\n"
                        + "'mutatetype'='insertorignore',\n"
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

        String insertStatement = "INSERT INTO %s (a) values (1)";
        try {
            tEnv.executeSql(String.format(insertStatement, "sinkTable")).await();
            Assert.fail("Should fail.");
        } catch (Exception ex) {
            FlinkAssertions.anyCauseMatches(
                            IOException.class,
                            "failed to copy because dirty data, the error record is Record")
                    .accept(ex);
        }
    }

    @Test
    public void testSinkTableCheckAndPut() throws Exception {
        if (copyMode) {
            return;
        }
        String sinkTableName = "\"TEST_sink_table_for_check_and_put" + randomSuffix + "\"";
        String createTableSql =
                "begin;\n"
                        + "CREATE TABLE TABLE_NAME (\n"
                        + "a integer primary key,\n"
                        + "b text,\n"
                        + "\"C,C\" timestamptz\n"
                        + ");\n"
                        + "CALL set_table_property('TABLE_NAME', 'orientation', 'row');\n"
                        + "CALL set_table_property('TABLE_NAME', 'binlog.level', 'replica');\n"
                        + "commit;";
        executeSql(createTableSql.replace("TABLE_NAME", sinkTableName), false);
        tEnv.executeSql(
                "create table sinkTable"
                        + "(\n"
                        + "a int not null,\n"
                        + "b STRING not null,\n"
                        + "`C,C` timestamp\n"
                        + ") with ("
                        + "'connector'='hologres',\n"
                        + "'check-and-put.column'='C,C',\n"
                        + "'check-and-put.null-as'='2023-10-10 12:00:00',\n"
                        + "'check-and-put.operator'='GREATER_OR_EQUAL',\n"
                        + "'jdbcWriteBatchSize'='1',\n"
                        + "'fixedConnectionMode'='"
                        + fixedMode
                        + "',\n"
                        + "'jdbcCopyWriteMode'='"
                        + copyMode
                        + "',\n"
                        + "'bulkLoad'='"
                        + bulkLoad
                        + "',\n"
                        + "'mutatetype'='insertorupdate',\n"
                        + "'endpoint'='"
                        + endpoint
                        + "',\n"
                        + "'dbname'='"
                        + database
                        + "',\n"
                        + "'tablename'='"
                        + sinkTableName
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
                        + " (a,b,`C,C`) values "
                        + "(1,'dim0',cast(null as timestamp))," // insert
                        + "(1,'dim1',cast('2023-10-10 11:00:00' as timestamp))," // less than (null
                        // as) 2023-10-10
                        // 12:00:00, skip
                        + "(1,'dim2',cast('2023-10-10 12:00:00' as timestamp))," // equal with (null
                        // as) 2023-10-10
                        // 12:00:00, update
                        + "(1,'dim3',cast('2023-10-10 14:00:00' as timestamp))," // greater than
                        // 2023-10-10
                        // 12:00:00, update
                        + "(1,'dim4',cast('2023-10-10 13:00:00' as timestamp))," // less than
                        // 2023-10-10
                        // 14:00:00, skip
                        + "(1,'dim5',cast('2023-10-10 14:00:00' as timestamp))," // equal with
                        // 2023-10-10
                        // 14:00:00, update
                        + "(1,'dim6',cast('2023-10-10 15:00:00' as timestamp))"; // greater than
        // 2023-10-10
        // 14:00:00, update
        tEnv.executeSql(String.format(insertStatement, "sinkTable")).await();
        checkResultWithTimeout(
                new String[] {
                    "5,1,dim0,null",
                    "3,1,dim0,null",
                    "7,1,dim2,2023-10-10 12:00:00.0",
                    "3,1,dim2,2023-10-10 12:00:00.0",
                    "7,1,dim3,2023-10-10 14:00:00.0",
                    "3,1,dim3,2023-10-10 14:00:00.0",
                    "7,1,dim5,2023-10-10 14:00:00.0",
                    "3,1,dim5,2023-10-10 14:00:00.0",
                    "7,1,dim6,2023-10-10 15:00:00.0"
                },
                "select hg_binlog_event_type,* from " + sinkTableName,
                new String[] {"hg_binlog_event_type", "a", "b", "C,C"},
                JDBCUtils.getDbUrl(endpoint, database),
                username,
                password,
                10000);
        dropTable(sinkTableName);
    }

    private void insertValues(String tableName, Object[][] insertValues) throws SQLException {
        try (Connection connection = getConnection();
                PreparedStatement statement =
                        connection.prepareStatement(
                                String.format("insert into %s values (?,?);", tableName))) {
            for (Object[] row : insertValues) {
                statement.setInt(1, (Integer) row[0]);
                statement.setString(2, (String) row[1]);
                statement.executeUpdate();
            }
        }
    }
}
