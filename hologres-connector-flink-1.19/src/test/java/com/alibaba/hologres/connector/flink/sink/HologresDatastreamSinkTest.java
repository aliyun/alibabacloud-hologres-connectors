// package com.alibaba.hologres.connector.flink.sink;
//
// import com.alibaba.hologres.client.model.TableName;
// import com.alibaba.hologres.connector.flink.HologresTestBase;
// import com.alibaba.hologres.connector.flink.config.HologresConfigs;
// import com.alibaba.hologres.connector.flink.sink.v1.multi.HologresMultiTableSinkFunction;
// import com.alibaba.hologres.connector.flink.utils.JDBCUtils;
// import org.apache.flink.configuration.Configuration;
// import org.apache.flink.streaming.api.datastream.DataStreamSource;
// import org.junit.After;
// import org.junit.Before;
// import org.junit.Test;
//
// import java.io.IOException;
// import java.math.BigDecimal;
// import java.sql.SQLException;
// import java.sql.Timestamp;
// import java.util.HashMap;
// import java.util.Map;
//
// import static com.alibaba.hologres.connector.flink.HologresTestUtils.checkResultWithTimeout;
// import static com.alibaba.hologres.connector.flink.HologresTestUtils.expectedRowsToString;
//
// public class HologresDatastreamSinkTest extends HologresTestBase {
//
//    private String sinkTableWithSchema;
//
//    public HologresDatastreamSinkTest() throws IOException {
//    }
//
//    public static final Object[][] EXPECTED =
//            new Object[][] {
//                    new Object[] {
//                            0,
//                            "dim",
//                            20.2007,
//                            false,
//                            652482,
//                            new java.sql.Date(120, 6, 8),
//                            "source_test",
//                            Timestamp.valueOf("2020-07-10 16:28:07.737"),
//                            8.58965,
//                            "{464,98661,32489}",
//                            "{8589934592,8589934593,8589934594}",
//                            "{8.58967018,96.4666977,9345.16016}",
//                            "{587897.464674600051,792343.64644599997,76.4646400000000028}",
//                            "{t,t,f,t}",
//                            "{monday,saturday,sunday}",
//                            new java.sql.Time(123456789L), // "18:17:36.789"
//                            new BigDecimal("8119.21"),
//                            Timestamp.valueOf("2020-07-10 16:28:07.737"),
//                            2,
//                            "{\"a\":\"bbbb\", \"c\":\"dddd\"}",
//                            "{\"a\": \"bbbb\", \"c\": \"dddd\"}"
//                    },
//                    new Object[] {
//                            2,
//                            "dim",
//                            20.2007,
//                            false,
//                            652482,
//                            new java.sql.Date(120, 6, 8),
//                            "source_test",
//                            Timestamp.valueOf("2020-07-10 16:28:07.737"),
//                            8.58965,
//                            "{464,98661,32489}",
//                            "{8589934592,8589934593,8589934594}",
//                            "{8.58967018,96.4666977,9345.16016}",
//                            "{587897.464674600051,792343.64644599997,76.4646400000000028}",
//                            "{t,t,f,t}",
//                            "{monday,saturday,sunday}",
//                            new java.sql.Time(123456789L), // "18:17:36.789"
//                            new BigDecimal("8119.21"),
//                            Timestamp.valueOf("2020-07-10 16:28:07.737"),
//                            2,
//                            "{\"a\":\"bbbb\", \"c\":\"dddd\"}",
//                            "{\"a\": \"bbbb\", \"c\": \"dddd\"}"
//                    },
//                    new Object[] {
//                            11,
//                            "dim",
//                            20.2007,
//                            false,
//                            652482,
//                            new java.sql.Date(120, 6, 8),
//                            "source_test",
//                            Timestamp.valueOf("2020-07-10 16:28:07.737"),
//                            8.58965,
//                            "{464,98661,32489}",
//                            "{8589934592,8589934593,8589934594}",
//                            "{8.58967018,96.4666977,9345.16016}",
//                            "{587897.464674600051,792343.64644599997,76.4646400000000028}",
//                            "{t,t,f,t}",
//                            "{monday,saturday,sunday}",
//                            new java.sql.Time(123456789L), // "18:17:36.789"
//                            new BigDecimal("8119.21"),
//                            Timestamp.valueOf("2020-07-10 16:28:07.737"),
//                            2,
//                            "{\"a\":\"bbbb\", \"c\":\"dddd\"}",
//                            "{\"a\": \"bbbb\", \"c\": \"dddd\"}"
//                    },
//            };
//    String prepareCreateTableSql =
//            "CREATE TABLE TABLE_NAME (\n"
//                    + "a integer NOT NULL,\n"
//                    + "b text NOT NULL,\n"
//                    + "c double precision,\n"
//                    + "d boolean,\n"
//                    + "e bigint,\n"
//                    + "f date,\n"
//                    + "g character varying,\n"
//                    + "h timestamp with time zone,\n"
//                    + "i real,\n"
//                    + "j integer[],\n"
//                    + "k bigint[],\n"
//                    + "l real[],\n"
//                    + "m double precision[],\n"
//                    + "n boolean[],\n"
//                    + "o text[],\n"
//                    + "p TIME,\n"
//                    + "q numeric(6, 2),\n"
//                    + "r timestamp without time zone,\n"
//                    + "s smallint,\n"
//                    + "t json,\n"
//                    + "u jsonb,\n"
//                    + "PRIMARY KEY (a)\n"
//                    + ") with (table_group='tg_3');\n";
//
//    @Before
//    public void setup() throws Exception {
//        this.sinkTable = "test_sink_table_all_type_" + "_" + randomSuffix;
//        this.sinkTableWithSchema = "test." + sinkTable;
//        executeSql(prepareCreateTableSql.replace("TABLE_NAME", sinkTable), false);
//        executeSql(prepareCreateTableSql.replace("TABLE_NAME", sinkTableWithSchema), false);
//    }
//
//    @After
//    public void cleanup() throws SQLException {
//        dropTable(sinkTableWithSchema);
//        dropTable(sinkTable);
//    }
//
//    @Test
//    public void testSinkTable() throws Exception {
//
//        env.enableCheckpointing(10000);
//        env.setParallelism(1);
//
//        DataStreamSource<Object[]> source = env.fromData(EXPECTED.);
//
//        Configuration configuration = new Configuration();
//        configuration.set(HologresConfigs.ENDPOINT, endpoint);
//        configuration.set(HologresConfigs.DATABASE, database);
//        configuration.set(HologresConfigs.USERNAME, username);
//        configuration.set(HologresConfigs.PASSWORD, password);
//        configuration.set(HologresConfigs.CONNECTION_POOL_SIZE, 10);
//        configuration.set(HologresConfigs.CONNECTION_POOL_NAME, "common-pool");
//
//        source.addSink(new HologresMultiTableSinkFunction<>(configuration, new
// RecordConverter()));
//
//        env.execute("Insert");
//
//        checkResultWithTimeout(
//                expectedRowsToString(EXPECTED),
//                "select * from " + sinkTable,
//                FIELD_NAMES,
//                JDBCUtils.getDbUrl(endpoint, database),
//                username,
//                password,
//                10000);
//    }
//
// }
