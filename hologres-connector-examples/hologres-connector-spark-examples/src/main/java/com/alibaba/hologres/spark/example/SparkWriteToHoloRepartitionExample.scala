package com.alibaba.hologres.spark.example

import com.alibaba.hologres.client.HoloConfig
import com.alibaba.hologres.spark.utils.RepartitionUtil
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties


/**
 * CREATE TABLE customer_holo_table
 * <p>(
 * <p>C_CUSTKEY    BIGINT ,
 * <p>C_NAME       TEXT   ,
 * <p>C_ADDRESS    TEXT   ,
 * <p>C_NATIONKEY  INT    ,
 * <p>C_PHONE      TEXT   ,
 * <p>C_ACCTBAL    DECIMAL(15,2) ,
 * <p>C_MKTSEGMENT TEXT   ,
 * <p>C_COMMENT    TEXT
 * <p>, primary key(C_CUSTKEY)
 * <p>);
 *
 */
object SparkWriteToHoloRepartitionExample {

  def main(args: Array[String]): Unit = {

    val prop = new Properties()
    // 参数配置在 hologres-connector-spark-examples/src/main/resources/setting.properties
    val inputStream = SparkWriteToHoloRepartitionExample.getClass.getClassLoader.getResourceAsStream("setting.properties")
    prop.load(inputStream)
    val username = prop.getProperty("USERNAME")
    val password = prop.getProperty("PASSWORD")
    val url = prop.getProperty("JDBCURL")
    val table = prop.getProperty("TABLE")
    val filepath = prop.getProperty("FILEPATH")

    val spark = SparkSession
      .builder
      .appName("SparkToHoloRepartitionExample")
      .master("local[*]")
      .config("spark.yarn.max.executor.failures", 3)
      .config("spark.stage.maxConsecutiveAttempts", 3)
      .config("spark.task.maxFailures", 3)
      .config("spark.default.parallelism", 24)
      .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    val schema = StructType(Array(
      StructField("c_custkey", LongType),
      StructField("c_name", StringType),
      StructField("c_address", StringType),
      StructField("c_nationkey", IntegerType),
      StructField("c_phone", StringType),
      StructField("c_acctbal", DecimalType(15, 2)),
      StructField("c_mktsegment", StringType),
      StructField("c_comment", StringType)
    ))

    val csvDf = spark.read
      .format("csv")
      .schema(schema)
      .option("sep", "|")
      .load(filepath)

    // 传入连接配置和表名, Util方法自行计算shardCount和分布键
    RepartitionUtil.reShuffleThenWrite(csvDf, username, password, url, tableName = table, copyWriteMode = "bulk_load_on_conflict", writeMode = "insertOrReplace", saveMode = SaveMode.Append)

    spark.stop()
  }
}