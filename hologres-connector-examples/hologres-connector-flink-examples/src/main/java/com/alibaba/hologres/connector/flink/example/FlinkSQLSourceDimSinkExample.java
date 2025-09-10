package com.alibaba.hologres.connector.flink.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.InputStream;
import java.util.Properties;


/**
 * A Flink data stream example and SQL sinking data to Hologres.
 */
public class FlinkSQLSourceDimSinkExample {

    /**
     * Hologres SQL.
     * <p>create table source_order_table(order_id bigint primary key, price decimal(38,2), sale_timestamp timestamptz);
     * <p>insert into source_order_table select a, a * 1.1, now() from generate_series(0, 1000) as a;
     * <p>
     * <p>create table dim_order_table(order_id bigint primary key, order_name text) with (orientation = 'row');
     * <p>insert into dim_order_table select a, 'user_' || a from generate_series(0, 10) as a;
     * <p>
     * <p>create table sink_order_table(order_id bigint, order_name text, price decimal(38,2),sale_timestamp timestamptz);
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

        String sourceTableName = "source_order_table";
        String dimTableName = "dim_table";
        String sinkTableName = "sink_order_table";
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8088");

        EnvironmentSettings.Builder streamBuilder =
                EnvironmentSettings.newInstance().inStreamingMode();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, streamBuilder.build());

        String createDatagenSourceTable =
                String.format(
                        "create table source("
                                + "  order_id bigint,"
                                + "  price decimal(38,2),"
                                + "  sale_timestamp timestamp,"
                                + "  proctime AS PROCTIME ()"
                                + ") with ("
                                + "  'connector'='hologres',"
                                + "  'dbname' = '%s',"
                                + "  'tablename' = '%s',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s',"
                                + "  'endpoint' = '%s'"
                                + ")",
                        database, sourceTableName, username, password, endpoint);
        tEnv.executeSql(createDatagenSourceTable);

        String createHologresDimTable =
                String.format(
                        "create table dim("
                                + "  order_id bigint,"
                                + "  order_name string"
                                + ") with ("
                                + "  'connector'='hologres',"
                                + "  'dbname' = '%s',"
                                + "  'tablename' = '%s',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s',"
                                + "  'endpoint' = '%s'"
                                + ")",
                        database, dimTableName, username, password, endpoint);
        tEnv.executeSql(createHologresDimTable);

        String createHologresTable =
                String.format(
                        "create table sink("
                                + "  order_id bigint,"
                                + "  order_name string,"
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
                        database, sinkTableName, username, password, endpoint);
        tEnv.executeSql(createHologresTable);

        tEnv.executeSql("insert into sink\n" +
                "select T.order_id, H.order_name, T.price, T.sale_timestamp\n" +
                "FROM\n" +
                "  source AS T\n" +
                "  JOIN dim FOR SYSTEM_TIME AS OF T.proctime AS H\n" +
                "  ON T.order_id % 10 = H.order_id;");
    }

}
