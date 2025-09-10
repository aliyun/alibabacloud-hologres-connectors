package com.alibaba.hologres.connector.flink.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * A Flink data stream example and SQL sinking data to Hologres.
 */
public class FlinkSQLSourceAndSinkExample {

    /**
     * Hologres DDL. create table source_table(user_id bigint, user_name text, price decimal(38,
     * 2),sale_timestamp timestamptz);
     * insert into source_table values(123,'Adam',123.11,'2022-05-19 14:33:05.418+08');
     * insert into source_table values(456,'Bob',123.45,'2022-05-19 14:33:05.418+08');
     * <p>
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
        String sourceTableName = "source_table";
        String sinkTableName = "sink_table";

        EnvironmentSettings.Builder streamBuilder =
                EnvironmentSettings.newInstance().inStreamingMode();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, streamBuilder.build());

        String createHologresSourceTable =
                String.format(
                        "create table source("
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
                        database, sourceTableName, username, password, endpoint);
        tEnv.executeSql(createHologresSourceTable);

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
                                + "  'sink.write-mode' = 'COPY_STREAM',"
                                + "  'endpoint' = '%s'"
                                + ")",
                        database, sinkTableName, username, password, endpoint);
        tEnv.executeSql(createHologresTable);

        tEnv.executeSql("insert into sink select * from source");
    }

}
