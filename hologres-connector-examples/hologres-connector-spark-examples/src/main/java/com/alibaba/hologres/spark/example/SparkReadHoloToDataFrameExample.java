package com.alibaba.hologres.spark.example;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * A Spark DataFrame example read from Hologres.
 */
public class SparkReadHoloToDataFrameExample {

    /**
     * Hologres DDL.
     * <p> create table source_table(user_id bigint, "USER_NAME" text, price decimal(38,
     * 2),sale_timestamp timestamptz);
     * <p>
     * insert into source_table select generate_series(1,20), 'abcd', 123.45, now();
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        InputStream inputStream = SparkHoloTableCatalogExample.class.getClassLoader().getResourceAsStream("setting.properties");
        prop.load(inputStream);
        String username = prop.getProperty("USERNAME");
        String password = prop.getProperty("PASSWORD");
        String url = prop.getProperty("JDBCURL");

        SparkSession sparkSession =
                SparkSession.builder()
                        .appName("SparkToHoloRepartitionExample")
                        .master("local[*]")
                        .appName("SparkDataFrameToHoloExample")
                        .config("spark.default.parallelism", 20)
                        .getOrCreate();
        sparkSession.sparkContext().setLogLevel("INFO");
        List<StructField> asList =
                Arrays.asList(
                        DataTypes.createStructField("user_id", DataTypes.LongType, true),
                        DataTypes.createStructField("USER_NAME", DataTypes.StringType, true),
                        DataTypes.createStructField("price", new DecimalType(38, 2), true),
                        DataTypes.createStructField(
                                "sale_timestamp", DataTypes.TimestampType, true));

        StructType schema = DataTypes.createStructType(asList);

        Dataset<Row> df = sparkSession.read()
                // .schema(schema) // 可选,不设置schema时,会自动推断
                .format("hologres")
                .option("username", username)
                .option("password", password)
                .option("jdbcurl", url)
                // .option("table", "source_table")
                .option("read.mode", "bulk_read")
                .option("read.query", "select * from source_table") // 1.5.0版本支持通过query读取
                .load();

        df.show();
        df.count();

        Thread.sleep(10000000);
        sparkSession.stop();
    }
}
