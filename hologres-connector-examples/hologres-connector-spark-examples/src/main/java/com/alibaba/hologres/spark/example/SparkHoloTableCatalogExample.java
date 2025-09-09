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
     * <p>create database test_db;
     * <p>\c test_db
     * <p>create schema "Test_Schema";
     * <P>create table test_table1(user_id bigint, "USER_NAME" text, price decimal(38,
     * 2),sale_timestamp timestamptz);
     * <P>create table test_table2(user_id bigint, "USER_NAME" text, price decimal(38,
     * 2),sale_timestamp timestamptz);
     * <p>create table "Test_Schema".test_table3(user_id bigint, "USER_NAME" text, price decimal(38,
     * 2),sale_timestamp timestamptz);
     * <p>
     * <p>insert into test_table1 values(1, 'test', 1.0, '2021-01-01 00:00:00');
     * <p>insert into test_table2 values(2, 'test', 2.0, '2022-01-01 00:00:00');
     * <p>insert into "Test_Schema".test_table3 values(3, 'test', 3.0, '2023-01-01 00:00:00');
     *
     * <p> CREATE TABLE customer_holo_table
     * (
     * C_CUSTKEY BIGINT ,
     * C_NAME TEXT ,
     * C_ADDRESS TEXT ,
     * C_NATIONKEY INT ,
     * C_PHONE TEXT ,
     * C_ACCTBAL DECIMAL(15,2) ,
     * C_MKTSEGMENT TEXT ,
     * C_COMMENT TEXT
     * , primary key(C_CUSTKEY)
     * );</p>
     */
    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        InputStream inputStream = SparkHoloTableCatalogExample.class.getClassLoader().getResourceAsStream("setting.properties");
        prop.load(inputStream);
        String username = prop.getProperty("USERNAME");
        String password = prop.getProperty("PASSWORD");
        String url = prop.getProperty("JDBCURL");
        String table = prop.getProperty("TABLE");
        String filepath = prop.getProperty("FILEPATH");
        SparkSession sparkSession =
                SparkSession.builder()
                        .appName("SparkHoloToDataFrameExample")
                        .master("local[*]")
                        .config("spark.yarn.max.executor.failures", 3)
                        .config("spark.stage.maxConsecutiveAttempts", 3)
                        .config("spark.task.maxFailures", 3)
                        .config("spark.default.parallelism", 24)
                        .config("spark.sql.catalog.hologres_external_test_db", "com.alibaba.hologres.spark3.HoloTableCatalog")
                        // 此处参数都是connector本身支持的参数
                        .config("spark.sql.catalog.hologres_external_test_db.username", username)
                        .config("spark.sql.catalog.hologres_external_test_db.password", password)
                        .config("spark.sql.catalog.hologres_external_test_db.jdbcurl", url)
                        .getOrCreate();

        // 切换到hologres_external catalog, 对应hologres的test_db
        sparkSession.sql("use hologres_external_test_db");
        // 查看所有namespace, 即hologres test_db中所有的schema
        sparkSession.sql("show namespaces;").show();
        // 默认的namespace是public, 查询public下的所有表
        sparkSession.sql("show tables;").show();
        sparkSession.sql("select * from test_table1;").show();
        // 查询Test_Schema下的所有表
        sparkSession.sql("show tables in Test_Schema;").show();
        sparkSession.sql("select * from Test_Schema.test_table3;").show();
        // 通过use切换默认的namespace
        sparkSession.sql("use Test_Schema");
        sparkSession.sql("select * from test_table3;").show();

        // 支持读写, 相应的参数在创建catalog时指定, 无法单独为某个table指定
        sparkSession.sql("insert into Test_Schema.test_table3 select * from public.test_table1;");
        sparkSession.sql("select * from Test_Schema.test_table3;").show();
        sparkSession.sql("select count(1) from Test_Schema.test_table3;").show();

        sparkSession.sql("CREATE TEMPORARY VIEW csvTable (\n" +
                "    c_custkey bigint,\n" +
                "    c_name string,\n" +
                "    c_address string,\n" +
                "    c_nationkey int,\n" +
                "    c_phone string,\n" +
                "    c_acctbal decimal(15, 2),\n" +
                "    c_mktsegment string,\n" +
                "    c_comment string)\n" +
                "USING csv OPTIONS (\n" +
                "    path \"" + filepath + "\", sep \"|\"\n" +
                ");").show();
        sparkSession.sql("insert into public." + table + " select * from csvTable;\n").show();
        Thread.sleep(1000000);
        sparkSession.stop();
    }
}
