package com.alibaba.hologres.spark.example;

import org.apache.spark.sql.SparkSession;

import java.io.InputStream;
import java.util.Properties;

/**
 * A Spark DataFrame example read from Hologres.
 */
public class SparkHoloTableCatalogExample {

    /**
     * Hologres DDL.
     * <p>create database test_db1;
     * <p>create database test_db2;
     * <p>\c test_db1
     * <p>create table test_table1(user_id bigint, user_name text, price decimal(38,
     * 2),sale_timestamp timestamptz);
     * <p>insert into test_table1 values(1, 'test', 1.0, '2021-01-01 00:00:00');
     *
     * <p>\c test_db2
     * <p>create table test_table2(user_id bigint, user_name text, price decimal(38,
     * 2),sale_timestamp timestamptz);
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
                        .appName("SparkHoloToDataFrameExample")
                        .master("local[*]")
                        .config("spark.yarn.max.executor.failures", 3)
                        .config("spark.stage.maxConsecutiveAttempts", 3)
                        .config("spark.task.maxFailures", 3)
                        .config("spark.default.parallelism", 24)
                        .config("spark.sql.catalog.hologres_external", "com.alibaba.hologres.spark3.HoloTableCatalog")
                        // 此处参数都是connector本身支持的参数
                        .config("spark.sql.catalog.hologres_external.username", username)
                        .config("spark.sql.catalog.hologres_external.password", password)
                        .config("spark.sql.catalog.hologres_external.jdbcurl", url)
                        .config("spark.sql.catalog.hologres_external.bulk_read", true)
                        .getOrCreate();

        // 切换到hologres_external catalog
        sparkSession.sql("use hologres_external");
        // 查看所有namespace, 即hologres中所有的database
        sparkSession.sql("show namespaces;").show();

        // 查看test_db1中的所有表
        sparkSession.sql("show tables in test_db1;").show();
        sparkSession.sql("select * from test_db1.`public.test_table1`;").show();
        // 切换到test_db1这个namespace下,查表时可以省略namespace
        sparkSession.sql("use test_db1;");
        // hologres中的schema和table名,必须使用``括起来
        sparkSession.sql("select * from `public.test_table1`;").show();

        sparkSession.sql("show tables in test_db2;").show();
        sparkSession.sql("select * from test_db2.`public.test_table2`;").show();
        // 支持读写, 相应的参数在创建catalog时指定, 无法单独为某个table指定
        sparkSession.sql("insert into test_db2.`public.test_table2` select * from test_db1.`public.test_table1`;");
        sparkSession.sql("use test_db2;");
        sparkSession.sql("select * from `public.test_table2`;").show();
        sparkSession.sql("select count(1) from `public.test_table2`;").show();

        sparkSession.stop();
    }
}
