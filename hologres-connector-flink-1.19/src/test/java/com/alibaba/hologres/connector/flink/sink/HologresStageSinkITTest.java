package com.alibaba.hologres.connector.flink.sink;

import com.alibaba.hologres.connector.flink.HologresTestBase;
import com.alibaba.hologres.connector.flink.utils.JDBCUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.alibaba.hologres.connector.flink.HologresTestUtils.checkResultWithTimeout;
import static com.alibaba.hologres.connector.flink.HologresTestUtils.expectedRowsToString;
import static org.junit.Assert.assertEquals;

/** Integration tests for STAGE mode sink. */
public class HologresStageSinkITTest extends HologresTestBase {

    private String stageTable;

    public HologresStageSinkITTest() throws IOException {}

    String prepareCreateTableSql =
            "CREATE TABLE TABLE_NAME (\n"
                    + "a integer NOT NULL,\n"
                    + "b text NOT NULL,\n"
                    + "c double precision,\n"
                    + "d bigint,\n"
                    + "PRIMARY KEY (a)\n"
                    + ") with (table_group='tg_3');\n";

    public static final Object[][] EXPECTED =
            new Object[][] {
                new Object[] {1, "hello", 1.1, 100L},
                new Object[] {2, "world", 2.2, 200L},
                new Object[] {3, "flink", 3.3, 300L},
            };

    public static final String[] FIELD_NAMES = {"a", "b", "c", "d"};

    @Before
    public void setup() throws Exception {
        this.stageTable = "test_stage_sink_" + randomSuffix;
        executeSql(prepareCreateTableSql.replace("TABLE_NAME", stageTable), false);
    }

    @After
    public void cleanup() throws SQLException {
        dropTable(stageTable);
    }

    @Test
    public void testStageSinkBasic() throws Exception {
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE stageSinkTable ("
                                + "a INT NOT NULL,"
                                + "b STRING NOT NULL,"
                                + "c DOUBLE,"
                                + "d BIGINT"
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

        tEnv.executeSql(
                        "INSERT INTO stageSinkTable (a,b,c,d) VALUES "
                                + "(1,'hello',1.1,100),"
                                + "(2,'world',2.2,200),"
                                + "(3,'flink',3.3,300)")
                .await();

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
    public void testStageSinkWithParallelism() throws Exception {
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE stageSinkParallel ("
                                + "a INT NOT NULL,"
                                + "b STRING NOT NULL,"
                                + "c DOUBLE,"
                                + "d BIGINT"
                                + ") WITH ("
                                + "'connector'='hologres',"
                                + "'sink.write-mode'='STAGE',"
                                + "'sink.on-conflict-action'='insertOrUpdate',"
                                + "'sink.parallelism'='2',"
                                + "'endpoint'='%s',"
                                + "'dbName'='%s',"
                                + "'tableName'='%s',"
                                + "'userName'='%s',"
                                + "'password'='%s'"
                                + ")",
                        endpoint, database, stageTable, username, password));

        tEnv.executeSql(
                        "INSERT INTO stageSinkParallel (a,b,c,d) VALUES "
                                + "(1,'hello',1.1,100),"
                                + "(2,'world',2.2,200),"
                                + "(3,'flink',3.3,300)")
                .await();

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
    public void testStageSinkInsertOrIgnore() throws Exception {
        // Pre-insert a row
        executeSql(
                String.format("INSERT INTO %s (a,b,c,d) VALUES (1,'existing',9.9,999)", stageTable),
                false);

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE stageSinkIgnore ("
                                + "a INT NOT NULL,"
                                + "b STRING NOT NULL,"
                                + "c DOUBLE,"
                                + "d BIGINT"
                                + ") WITH ("
                                + "'connector'='hologres',"
                                + "'sink.write-mode'='STAGE',"
                                + "'sink.on-conflict-action'='insertOrIgnore',"
                                + "'endpoint'='%s',"
                                + "'dbName'='%s',"
                                + "'tableName'='%s',"
                                + "'userName'='%s',"
                                + "'password'='%s'"
                                + ")",
                        endpoint, database, stageTable, username, password));

        tEnv.executeSql(
                        "INSERT INTO stageSinkIgnore (a,b,c,d) VALUES "
                                + "(1,'hello',1.1,100),"
                                + "(2,'world',2.2,200)")
                .await();

        // Row with a=1 should remain 'existing' due to INSERT_OR_IGNORE
        Object[][] expected =
                new Object[][] {
                    new Object[] {1, "existing", 9.9, 999L},
                    new Object[] {2, "world", 2.2, 200L},
                };

        checkResultWithTimeout(
                expectedRowsToString(expected),
                "select * from " + stageTable,
                FIELD_NAMES,
                JDBCUtils.getDbUrl(endpoint, database),
                username,
                password,
                30000);
    }

    @Test
    public void testStageSinkDatagenWithTwentyShardsAndTenWriters() throws Exception {
        String multiShardTable = "test_stage_sink_20_shards_" + randomSuffix;
        try {
            createTable(
                    String.format(
                            "CREATE TABLE %s ("
                                    + "a integer NOT NULL PRIMARY KEY,"
                                    + "b text NOT NULL"
                                    + ") WITH (distribution_key='a', shard_count='20')",
                            multiShardTable));

            tEnv.executeSql(
                    "CREATE TABLE stageDatagenSource ("
                            + "a INT,"
                            + "b STRING"
                            + ") WITH ("
                            + "'connector'='datagen',"
                            + "'number-of-rows'='2000',"
                            + "'fields.a.kind'='sequence',"
                            + "'fields.a.start'='1',"
                            + "'fields.a.end'='2000',"
                            + "'fields.b.length'='32'"
                            + ")");
            tEnv.executeSql(
                    String.format(
                            "CREATE TABLE stageDatagenSink ("
                                    + "a INT NOT NULL,"
                                    + "b STRING NOT NULL"
                                    + ") WITH ("
                                    + "'connector'='hologres',"
                                    + "'sink.write-mode'='STAGE',"
                                    + "'sink.on-conflict-action'='insertOrUpdate',"
                                    + "'sink.parallelism'='10',"
                                    + "'endpoint'='%s',"
                                    + "'dbName'='%s',"
                                    + "'tableName'='%s',"
                                    + "'userName'='%s',"
                                    + "'password'='%s'"
                                    + ")",
                            endpoint, database, multiShardTable, username, password));

            tEnv.executeSql("INSERT INTO stageDatagenSink SELECT a, b FROM stageDatagenSource")
                    .await();

            try (Connection connection = getConnection();
                    Statement statement = connection.createStatement();
                    ResultSet resultSet =
                            statement.executeQuery(
                                    "SELECT count(*), count(distinct hg_shard_id), "
                                            + "min(hg_shard_id), max(hg_shard_id) FROM "
                                            + multiShardTable)) {
                resultSet.next();
                assertEquals(2000, resultSet.getInt(1));
                assertEquals(20, resultSet.getInt(2));
                assertEquals(0, resultSet.getInt(3));
                assertEquals(19, resultSet.getInt(4));
            }
        } finally {
            dropTable(multiShardTable);
        }
    }
}
