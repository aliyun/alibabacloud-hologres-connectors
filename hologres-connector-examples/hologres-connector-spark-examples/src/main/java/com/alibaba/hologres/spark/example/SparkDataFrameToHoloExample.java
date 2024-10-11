package com.alibaba.hologres.spark.example;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

/** A Spark DataFrame example write to Hologres. */
public class SparkDataFrameToHoloExample {

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

        SparkSession sparkSession =
                SparkSession.builder()
                        .master("local")
                        .appName("SparkDataFrameToHoloExample")
                        .config("spark.default.parallelism", 1)
                        .getOrCreate();

        List<Row> data =
                Arrays.asList(
                        RowFactory.create(
                                123L,
                                "Adam",
                                new BigDecimal("123.11"),
                                new Timestamp(System.currentTimeMillis())),
                        RowFactory.create(
                                234L,
                                "Bob",
                                new BigDecimal("000.11"),
                                new Timestamp(System.currentTimeMillis())));

        List<StructField> asList =
                Arrays.asList(
                        DataTypes.createStructField("user_id", DataTypes.LongType, true),
                        DataTypes.createStructField("user_name", DataTypes.StringType, true),
                        DataTypes.createStructField("price", new DecimalType(38, 2), true),
                        DataTypes.createStructField(
                                "sale_timestamp", DataTypes.TimestampType, true));

        StructType schema = DataTypes.createStructType(asList);

        Dataset<Row> df = sparkSession.createDataFrame(data, schema);

        df.write()
            .format("hologres")
            .option("username", userName)
            .option("password", password)
            .option("endpoint", endPoint)
            .option("database", database)
            .option("table", tableName)
            .mode(SaveMode.Append)
            .save();

        sparkSession.stop();
    }
}
