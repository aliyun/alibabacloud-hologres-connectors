package com.alibaba.hologres.spark.example

import com.alibaba.hologres.client.impl.util.ShardUtil
import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.client.{Command, HoloClient, HoloConfig}
import com.alibaba.hologres.org.postgresql.jdbc.TimestampUtil
import org.apache.spark.Partitioner
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import java.sql.{Date, Timestamp}
import java.util.Properties
import java.util.concurrent.{ConcurrentSkipListMap, ThreadLocalRandom}


object SparkToHoloRepartitionExample {

  def main(args: Array[String]): Unit = {

    val prop = new Properties()
    val inputStream = SparkToHoloRepartitionExample.getClass.getClassLoader.getResourceAsStream("setting.properties")
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

    var schema = StructType(Array(
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

    // 新增一列当做分区字段
    schema = schema.add("c_date", DateType)
    val df = csvDf.withColumn("c_date", lit("2024-05-27").cast(DateType))
    df.show(10)

    // 获取shardCount和TableSchema(distribution key)
    val holoConf = new HoloConfig
    holoConf.setUsername(username)
    holoConf.setPassword(password)
    holoConf.setJdbcUrl(url)
    val client = new HoloClient(holoConf)
    val holoSchema = client.getTableSchema(table)
    val shardCount = Command.getShardCount(client, holoSchema)

    val keySelector = new HoloKeySelector(shardCount, schema, holoSchema)
    val partitioner = new CustomerPartition(shardCount)

    val rdd = {
      // keySelector 根据 distribution key字段的值计算shard
      df.rdd.map(row => {(keySelector.getKey(row), row)})
      // 数据repartition为shardCount个分区
      .partitionBy(partitioner)
      .map(_._2)
    }

    // repartition 之后的rdd重新使用DataFrame的write api写入holo
    spark.createDataFrame(rdd, schema)
      .write.format("hologres")
      .option("username", prop.getProperty("USERNAME"))
      .option("password", prop.getProperty("PASSWORD"))
      .option("jdbcurl", prop.getProperty("JDBCURL"))
      .option("table", prop.getProperty("TABLE"))
      .option("write_mode", "insert_or_update")
      .option("copy_mode", "bulk_load")
      .option("reshuffle_by_holo_distribution_key", "true")
      .mode(SaveMode.Append)
      .save()

    spark.stop()
  }

  private object RowShardUtil{
    // hologres仅支持int等整数类型,字符串类型,Date类型做distribution key,大多数类型toString的字面值与holo是一致的,因此不存在计算错误的情况
    // 但date类型在holo中存储为int,如果遇到Date类型,则转换为int计算
    // 类似的,目前timestamptz类型也可以当作distribution key(timestamp类型不可以),因此计算shard使用其对应的long值.
    def hash(row: Row, indexes: Array[Int]): Int = {
      var hash = 0
      var first = true
      if (indexes == null || indexes.length == 0) {
        val rand = ThreadLocalRandom.current
        hash = rand.nextInt
      }
      else for (i <- indexes) {
        var o = row.get(i)
        o match {
          case date: Date => o = date.toLocalDate.toEpochDay
          case timestamp : Timestamp => o = TimestampUtil.timestampToMillisecond(timestamp, "timestamptz")
          case _ =>
        }
        if (first) hash = ShardUtil.hash(o)
        else hash ^= ShardUtil.hash(o)
        first = false
      }
      hash
    }
  }


  private class HoloKeySelector(shardCount: Int, sparkSchema: StructType, holoSchema: TableSchema) extends Serializable {
    final private val splitRange = new ConcurrentSkipListMap[Integer, Integer]
    val range: Array[Array[Int]] = ShardUtil.split(shardCount)
    for (i <- 0 until shardCount) {
      splitRange.put(range(i)(0), i)
    }

    val keyIndexInHolo: Array[Int] = holoSchema.getDistributionKeyIndex
    val keyIndexInSpark = new Array[Int](keyIndexInHolo.length)
    for (i <- keyIndexInHolo.indices) {
      val columnName = holoSchema.getColumn(keyIndexInHolo(i)).getName
      try {
        keyIndexInSpark(i) = sparkSchema.fieldIndex(columnName)
      } catch {
        case e: IllegalArgumentException =>
          throw new IllegalArgumentException(String.format("repartition need write all distribution keys %s, missing column info: %s", holoSchema.getDistributionKeys, e.getMessage))
      }
    }

    /**
     * 根据Row中的distribution key字段的信息，计算写入holo时所处的shard
     */
    def getKey(value: Row): Integer = {
      val raw = RowShardUtil.hash(value, keyIndexInSpark)
      val hash = Integer.remainderUnsigned(raw, ShardUtil.RANGE_END)
      splitRange.floorEntry(hash).getValue
    }
  }

  private class CustomerPartition(partitions: Int) extends Partitioner {
    def numPartitions: Int = partitions

    def getPartition(key: Any): Int = {
      Integer.valueOf(key.toString) % numPartitions
    }
  }


}