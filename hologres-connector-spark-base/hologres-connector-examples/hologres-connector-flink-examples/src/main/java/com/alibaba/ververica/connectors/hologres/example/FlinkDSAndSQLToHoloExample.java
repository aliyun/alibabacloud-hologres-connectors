package com.alibaba.ververica.connectors.hologres.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import java.math.BigDecimal;
import java.sql.Timestamp;

import static org.apache.flink.table.api.Expressions.$;

/** A Flink data stream example and SQL sinking data to Hologres. */
public class FlinkDSAndSQLToHoloExample {

    /**
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
        options.addOption("t", "tablename", true, "Table name");

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);
        String endPoint = commandLine.getOptionValue("endpoint");
        String userName = commandLine.getOptionValue("username");
        String password = commandLine.getOptionValue("password");
        String database = commandLine.getOptionValue("database");
        String tableName = commandLine.getOptionValue("tablename");

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
                        database, tableName, userName, password, endPoint);
        tEnv.executeSql(createHologresTable);

        tEnv.executeSql("insert into sink select userId,userName,price,saleTimestamp from " + table);
    }
}
