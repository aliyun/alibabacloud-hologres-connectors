package com.alibaba.hologres.spark.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.InputStream;
import java.util.Properties;

/**
 * A Spark DataFrame example read from Hologres.
 */
public class SparkReadHoloSplitExample {

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

        Dataset<Row> df = sparkSession.read()
                .format("hologres")
                .option("username", username)
                .option("password", password)
                .option("jdbcurl", url)
                .option("table", "public.sales_data_view")
//                .option("read.split.strategy", "shard")
//                .option("read.split.strategy", "partition")
                .option("read.split.strategy", "range")
                .option("read.split.column", "user_id")
                .option("read.split.lower_bound", "1")
                .option("read.split.upper_bound", "20")
                .option("read.split.num", "4")
                .load();

        df.show();
        df.count();

        Thread.sleep(10000000);
        sparkSession.stop();
    }
}
