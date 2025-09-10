package com.alibaba.hologres.connector.flink.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.math.BigDecimal;
import java.sql.Timestamp;

import static org.apache.flink.table.api.Expressions.$;

/**
 * A Flink data stream example and SQL sinking data to Hologres.
 */
public class FlinkDSAndSQLToHoloExample {

    /**
     * Hologres DDL. create table sink_table(user_id bigint, user_name text, price decimal(38,
     * 2),sale_timestamp timestamptz);
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        SettingHelper settingHelper = new SettingHelper(args, "setting.properties");
        String endpoint = settingHelper.getEndpoint();
        String username = settingHelper.getUsername();
        String password = settingHelper.getPassword();
        String database = settingHelper.getDatabase();
        String tableName = "sink_table";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // use BATCH or STREAMING mode
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env);

        DataStream<SourceItem> source =
                env.fromElements(
                        new SourceItem(
                                123L,
                                "Adam",
                                new BigDecimal("123.11"),
                                new Timestamp(System.currentTimeMillis())
                        ),
                        new SourceItem(
                                234,
                                "Bob",
                                new BigDecimal("000.11"),
                                new Timestamp(System.currentTimeMillis())
                        ));

        Table table = tEnv.fromDataStream(source);
        table.printSchema();

        String createHologresTable =
                String.format(
                        "create table sink("
                                + "  user_id bigint,"
                                + "  user_name string,"
                                + "  price decimal(38,2),"
                                + "  sale_timestamp timestamp"
                                + ") with ("
                                + "  'connector'='hologres',"
                                + "  'dbname' = '%s',"
                                + "  'tablename' = '%s',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s',"
                                + "  'endpoint' = '%s'"
                                + ")",
                        database, tableName, username, password, endpoint);
        tEnv.executeSql(createHologresTable);

        tEnv.executeSql("insert into sink select userId,userName,price,saleTimestamp from " + table);
    }
}
