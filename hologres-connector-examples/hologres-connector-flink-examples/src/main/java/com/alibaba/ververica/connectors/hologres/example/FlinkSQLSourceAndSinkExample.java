package com.alibaba.ververica.connectors.hologres.example;

import java.math.BigDecimal;
import java.sql.Timestamp;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/** A Flink data stream example and SQL sinking data to Hologres. */
public class FlinkSQLSourceAndSinkExample {

    /**
     * Hologres DDL. create table source_table(user_id bigint, user_name text, price decimal(38,
     * 2),sale_timestamp timestamptz);
     * insert into source_table values(123,'Adam',123.11,'2022-05-19 14:33:05.418+08');
     * insert into source_table values(456,'Bob',123.45,'2022-05-19 14:33:05.418+08');
     *
     * Hologres DDL. create table sink_table(user_id bigint, user_name text, price decimal(38,
     * 2),sale_timestamp timestamptz);
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("e", "endpoint", true, "Hologres endpoint");
        options.addOption("u", "username", true, "Username");
        options.addOption("p", "password", true, "Password");
        options.addOption("d", "database", true, "Database");
        options.addOption("source", "sourceTableName", true, "Source table name");
        options.addOption("sink", "sinkTableName", true, "Sink table name");

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);
        String endPoint = commandLine.getOptionValue("endpoint");
        String userName = commandLine.getOptionValue("username");
        String password = commandLine.getOptionValue("password");
        String database = commandLine.getOptionValue("database");
        String sourceTableName = commandLine.getOptionValue("sourceTableName");
        String sinkTableName = commandLine.getOptionValue("sinkTableName");

        EnvironmentSettings.Builder streamBuilder =
            EnvironmentSettings.newInstance().inStreamingMode();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(env, streamBuilder.useBlinkPlanner().build());

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
                database, sourceTableName, userName, password, endPoint);
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
                    + "  'jdbcRetryCount'='20',"
                    + "  'dbname' = '%s',"
                    + "  'tablename' = '%s',"
                    + "  'username' = '%s',"
                    + "  'password' = '%s',"
                    + "  'endpoint' = '%s'"
                    + ")",
                database, sinkTableName, userName, password, endPoint);
        tEnv.executeSql(createHologresTable);

        tEnv.executeSql("insert into sink select * from source");
    }

}
