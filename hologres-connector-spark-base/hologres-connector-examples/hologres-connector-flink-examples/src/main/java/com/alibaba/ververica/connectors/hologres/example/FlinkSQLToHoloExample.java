package com.alibaba.ververica.connectors.hologres.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

/** A Flink sql example sinking data to Hologres. */
public class FlinkSQLToHoloExample {

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

        EnvironmentSettings.Builder streamBuilder =
                EnvironmentSettings.newInstance().inStreamingMode();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, streamBuilder.build());

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
                                + "  'jdbcCopyWriteMode' = 'true',"
                                + "  'tablename' = '%s',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s',"
                                + "  'endpoint' = '%s'"
                                + ")",
                        database, tableName, userName, password, endPoint);
        tEnv.executeSql(createHologresTable);

        createScanTable(tEnv);

        tEnv.executeSql("insert into sink select * from source");
    }

    private static void createScanTable(TableEnvironment tableEnvironment) {
        List<Row> data = new ArrayList<>();
        data.add(
                Row.of(
                        1L,
                        "Hi",
                        new BigDecimal("123.12"),
                        LocalDateTime.of(
                                LocalDate.of(2020, 07, 10), LocalTime.of(16, 28, 7, 737000000))));
        data.add(
                Row.of(
                        2L,
                        "Hello",
                        new BigDecimal("234.24"),
                        LocalDateTime.of(
                                LocalDate.of(2020, 07, 10), LocalTime.of(16, 28, 7, 737000000))));
        data.add(
                Row.of(
                        3L,
                        "Hello word",
                        new BigDecimal("123.12"),
                        LocalDateTime.of(
                                LocalDate.of(2020, 07, 10), LocalTime.of(16, 28, 7, 737000000))));

        String dataId = TestValuesTableFactory.registerData(data);
        tableEnvironment.executeSql(
                "create table source (\n"
                        + "  user_id bigint,"
                        + "  user_name string,"
                        + "  price decimal(38,2),"
                        + "  sale_timestamp timestamp"
                        + ") with (\n"
                        + "  'connector'='values',\n"
                        + "  'data-id'='"
                        + dataId
                        + "'\n"
                        + ")");
    }
}
