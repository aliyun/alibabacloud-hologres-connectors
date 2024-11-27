package com.alibaba.ververica.connectors.hologres.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Flink data stream demo sink data to hologres and do custom partition.
 */
public class FlinkSQLToHoloRePartitionExample {
    private static final transient Logger LOG =
            LoggerFactory.getLogger(FlinkSQLToHoloRePartitionExample.class);

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("e", "endpoint", true, "Hologres endpoint");
        options.addOption("u", "username", true, "Username");
        options.addOption("p", "password", true, "Password");
        options.addOption("d", "database", true, "Database");
        options.addOption("t", "tablename", true, "Table name");

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);
        String endPoint = commandLine.getOptionValue("endpoint");
        String userName = commandLine.getOptionValue("username");
        String password = commandLine.getOptionValue("password");
        String database = commandLine.getOptionValue("database");
        String tableName = commandLine.getOptionValue("tablename");

        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8081");

        EnvironmentSettings.Builder streamBuilder =
                EnvironmentSettings.newInstance().inStreamingMode();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);
        env.setParallelism(5);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, streamBuilder.build());

        String sourceDDL =
                "CREATE TEMPORARY TABLE source_table"
                        + "("
                        + "    c_custkey     BIGINT"
                        + "    ,c_name       STRING"
                        + "    ,c_address    STRING"
                        + "    ,c_nationkey  INTEGER"
                        + "    ,c_phone      STRING"
                        + "    ,c_acctbal    NUMERIC(15, 2)"
                        + "    ,c_mktsegment STRING"
                        + "    ,c_comment    STRING"
                        + ")"
                        + "WITH ("
                        + "    'connector' = 'datagen'"
                        + "    ,'rows-per-second' = '1000'"
                        + "    ,'number-of-rows' = '300000'"
                        + ");";
        String sinkDDL =
                String.format(
                        "CREATE TEMPORARY TABLE sink_table"
                                + "("
                                + "    c_custkey     BIGINT"
                                + "    ,c_name       STRING"
                                + "    ,c_address    STRING"
                                + "    ,c_nationkey  INTEGER"
                                + "    ,c_phone      STRING"
                                + "    ,c_acctbal    NUMERIC(15, 2)"
                                + "    ,c_mktsegment STRING"
                                + "    ,c_comment    STRING"
                                + "    ,`date`       DATE"
                                + ")"
                                + "with ("
                                + "  'connector' = 'hologres',"
                                + "  'dbname' = '%s',"
                                + "  'tablename' = '%s',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s',"
                                + "  'endpoint' = '%s',"
                                + "  'jdbccopywritemode' = 'BULK_LOAD',"
                                + "  'reshuffle-by-holo-distribution-key.enabled'='true'"
                                + ");",
                        database, tableName, userName, password, endPoint);

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        tEnv.executeSql("insert into sink_table select *, cast('2024-04-21' as DATE) from source_table");
    }
}
