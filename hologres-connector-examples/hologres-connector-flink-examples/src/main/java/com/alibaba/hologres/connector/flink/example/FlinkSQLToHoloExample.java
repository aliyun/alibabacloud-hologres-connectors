package com.alibaba.hologres.connector.flink.example;

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

/**
 * A Flink sql example sinking data to Hologres.
 */
public class FlinkSQLToHoloExample {

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
                                + "  'sink.write-mode' = 'COPY_STREAM',"
                                + "  'tablename' = '%s',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s',"
                                + "  'endpoint' = '%s'"
                                + ")",
                        database, tableName, username, password, endpoint);
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
