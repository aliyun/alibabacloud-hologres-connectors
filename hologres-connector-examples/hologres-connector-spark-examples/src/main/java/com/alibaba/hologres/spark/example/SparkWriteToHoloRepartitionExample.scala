package com.alibaba.hologres.spark.example

import com.alibaba.hologres.client.HoloConfig
import com.alibaba.hologres.spark.utils.RepartitionUtil
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.{SparkSession, functions => F}

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

    // Number of records to generate
    val numRecords = 10000

    // Creating a DataFrame with a sequence of numbers
    val df = spark.range(0, numRecords).toDF("id")

    // Defining UDFs for generating random data
    val randomStringUDF = F.udf(() => scala.util.Random.alphanumeric.take(10).mkString)
    val randomIntUDF = F.udf(() => scala.util.Random.nextInt(100))
    val randomDoubleUDF = F.udf(() => scala.util.Random.nextDouble())

    // Adding multiple random fields
    val randomDf = df
      .withColumn("randomInt", randomIntUDF())
      .withColumn("randomString", randomStringUDF())
      .withColumn("randomDouble", randomDoubleUDF())

    // Show a few rows of the generated DataFrame
    randomDf.show(10)
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
    // RepartitionUtil.reShuffleThenWrite(csvDf, username, password, url, tableName = table, copyWriteMode = "bulk_load_on_conflict", saveMode = SaveMode.Append)

    // 1.5.1版本起, reshuffle逻辑被放入了write中,用户不需要特别配置
    // 传入连接配置和表名,
    csvDf.write
      .format("hologres")
      .option("username", username)
      .option("password", password)
      .option("jdbcurl", url)
      .option("table", table)
      .option("write.mode", "auto")
      .option("write.on_conflict_action", "insertorReplace")
      .option("enable_serverless_computing", "true")
      .mode(SaveMode.Append)
      .save()
    Thread.sleep(1000000)
    spark.stop()
  }
}