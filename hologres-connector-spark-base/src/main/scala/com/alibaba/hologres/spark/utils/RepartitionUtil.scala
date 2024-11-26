/*
 *  Copyright (c) 2021, Alibaba Group;
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.hologres.spark.utils

import com.alibaba.hologres.client.impl.util.ShardUtil
import com.alibaba.hologres.client.{Command, HoloClient, HoloConfig}
import com.alibaba.hologres.org.postgresql.jdbc.TimestampUtil
import org.apache.spark.Partitioner
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}
import java.util.concurrent.{ConcurrentSkipListMap, ThreadLocalRandom}

object RepartitionUtil {
  def reShuffleThenWrite(inputDf: DataFrame, username: String, password: String, url: String, tableName: String,
                         copyWriteMode: String = "bulk_load", writeMode: String = "insertOrReplace", saveMode: SaveMode = SaveMode.Append): Unit = {
    val reShuffledDf = reShuffleByHoloDistributionKey(inputDf, username, password, url, tableName)
    // 将shuffle之后的DataFrame写入到Hologres中
    reShuffledDf.write
      .format("hologres")
      .option("username", username)
      .option("password", password)
      .option("jdbcurl", url)
      .option("table", tableName)
      .option("copy_write_mode", copyWriteMode)
      .option("write_mode", writeMode)
      .option("reshuffle_by_holo_distribution_key", "true")
      .mode(saveMode)
      .save()
  }

  /**
   * 传入holo的配置,从而自行计算表的shardCount和分布键信息
   */
  private def reShuffleByHoloDistributionKey(inputDf: DataFrame, username: String, password: String, url: String, tableName: String): DataFrame = {
    val holoConf = new HoloConfig
    holoConf.setUsername(username)
    holoConf.setPassword(password)
    holoConf.setJdbcUrl(url)
    val client = new HoloClient(holoConf)
    val holoSchema = client.getTableSchema(tableName)
    val shardCount = Command.getShardCount(client, holoSchema)
    val sparkSession = inputDf.sparkSession
    val inputSchema = inputDf.schema

    val keySelector = new HoloKeySelector(shardCount, inputSchema,  holoSchema.getDistributionKeys)
    val partitioner = new CustomerPartition(shardCount)
    val rdd = {
      // keySelector 根据 distribution key字段的值计算shard
      inputDf.rdd.map(row => {
          (keySelector.getKey(row), row)
        })
        // 数据repartition为shardCount个分区
        .partitionBy(partitioner)
        .map(_._2)
    }
    sparkSession.createDataFrame(rdd, inputSchema)
  }

  private object RowShardUtil {
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
          case timestamp: Timestamp => o = TimestampUtil.timestampToMillisecond(timestamp, "timestamptz")
          case _ =>
        }
        if (first) hash = ShardUtil.hash(o)
        else hash ^= ShardUtil.hash(o)
        first = false
      }
      hash
    }
  }

  private class HoloKeySelector(shardCount: Int, sparkSchema: StructType, distributionKeys: Array[String]) extends Serializable {
    final private val splitRange = new ConcurrentSkipListMap[Integer, Integer]
    val range: Array[Array[Int]] = ShardUtil.split(shardCount)
    for (i <- 0 until shardCount) {
      splitRange.put(range(i)(0), i)
    }

    val keyIndexInSpark = new Array[Int](distributionKeys.length)
    var i: Int = 0
    for (columnName <- distributionKeys) {
      try {
        keyIndexInSpark(i) = sparkSchema.fieldIndex(columnName)
        i = i + 1
      } catch {
        case e: IllegalArgumentException =>
          throw new IllegalArgumentException(String.format("repartition need all distribution keys %s, " +
            "but missing column in input DataFrame's schema: %s", distributionKeys, sparkSchema), e)
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
