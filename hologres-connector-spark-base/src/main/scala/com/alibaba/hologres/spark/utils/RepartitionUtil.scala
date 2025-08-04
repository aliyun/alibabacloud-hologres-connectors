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
import com.alibaba.hologres.spark.config.HologresConfigs
import org.apache.spark.Partitioner
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import java.sql.{Date, Timestamp}
import java.util.concurrent.{ConcurrentSkipListMap, ThreadLocalRandom}

object RepartitionUtil {

  private val logger = new LoggerWrapper(getClass)

  def reShuffleThenWrite(inputDf: DataFrame, hologresConfigs: HologresConfigs, saveMode: SaveMode): Unit = {
    logger.setSparkAppName(hologresConfigs.sparkAppName)
    logger.setSparkAppId(hologresConfigs.sparkAppId)
    logger.setHoloTableName(hologresConfigs.table)

    val reShuffledDf = reShuffleByHoloDistributionKey(inputDf,
      hologresConfigs.username, hologresConfigs.password, hologresConfigs.jdbcUrl, hologresConfigs.table)
    // 将shuffle之后的DataFrame写入到Hologres中, 原样传入所有写入和通用参数
    reShuffledDf.write
      .format("hologres")
      .option("username", hologresConfigs.username)
      .option("password", hologresConfigs.password)
      .option("jdbcurl", hologresConfigs.jdbcUrl)
      .option("table", hologresConfigs.table)
      .option("enable_serverless_computing", hologresConfigs.enableServerlessComputing)
      .option("serverless_computing_query_priority", hologresConfigs.serverlessComputingQueryPriority)
      .option("statement_timeout_seconds", hologresConfigs.statementTimeout)
      .option("retry_count", hologresConfigs.holoConfig.getRetryCount)
      .option("retry_sleep_init_ms", hologresConfigs.holoConfig.getRetrySleepInitMs)
      .option("retry_sleep_step_ms", hologresConfigs.holoConfig.getRetrySleepStepMs)
      .option("connection_max_idle_ms", hologresConfigs.holoConfig.getConnectionMaxIdleMs)
      .option("fixed_connection_mode", hologresConfigs.holoConfig.isUseFixedFe)
      .option("direct_connect", hologresConfigs.directConnect)
      .option("retry_count", hologresConfigs.holoConfig.getRetryCount)
      .option("write.mode", hologresConfigs.writeMode.toString)
      .option("write.on_conflict_action", hologresConfigs.onConflictAction.name())
      .option("write.copy.max_buffer_size", hologresConfigs.writeCopyMaxBufferSize)
      .option("write.copy.format", hologresConfigs.writeCopyFormat)
      .option("write.copy.disable_right_join", hologresConfigs.disableRightJoinInCopy)
      .option("write.copy.dirty_data_check", hologresConfigs.writeCopyDirtyDataCheck)
      .option("write.strict_datatype_check", hologresConfigs.writeStrictDataTypeCheck)
      .option("write.remove_u0000", hologresConfigs.writeRemoveU0000)
      .option("write.reshuffle_by_holo_distribution_key", "true")
      .mode(saveMode)
      .save()
  }

  @deprecated()
  def reShuffleThenWrite(inputDf: DataFrame, username: String, password: String, url: String, tableName: String,
                         writeMode: String = "bulk_load", onConflictAction: String = "insertOrReplace",
                         maxBufferSize: Int = 50 * 1024 * 1024, saveMode: SaveMode = SaveMode.Append,
                         directConnect: Boolean = false, enableServerlessComputing: Boolean = false): Unit = {
    val hologresConfigs:HologresConfigs = new HologresConfigs(Map(
      "username" -> username,
      "password" -> password,
      "jdbcurl" -> url,
      "table" -> tableName,
      "write.mode" -> writeMode,
      "write.on_conflict_action" -> onConflictAction,
      "write.copy.max_buffer_size" -> maxBufferSize.toString,
      "direct_connect" -> directConnect.toString,
      "enable_serverless_computing" -> enableServerlessComputing.toString
    ))
    logger.setSparkAppName(hologresConfigs.sparkAppName)
    logger.setSparkAppId(hologresConfigs.sparkAppId)
    logger.setHoloTableName(hologresConfigs.table)

    reShuffleThenWrite(inputDf, hologresConfigs, saveMode)
  }

  /**
   * 传入holo的配置,从而自行计算表的shardCount和分布键信息
   */
  def reShuffleByHoloDistributionKey(inputDf: DataFrame, username: String, password: String, url: String, tableName: String,
                                     enableAkv4: Boolean = false, akv4Region: String = ""): DataFrame = {
    val holoConf = new HoloConfig
    holoConf.setUsername(username)
    holoConf.setPassword(password)
    holoConf.setJdbcUrl(url)
    holoConf.setUseAKv4(enableAkv4)
    if (enableAkv4 && akv4Region != "") {
      holoConf.setRegion(akv4Region)
    }
    val client = new HoloClient(holoConf)
    try {
      val holoSchema = client.getTableSchema(tableName)
      val shardCount = Command.getShardCount(client, holoSchema)
      val sparkSession = inputDf.sparkSession
      val inputSchema = inputDf.schema
      logger.info(s"start repartition by holo distribution key, shardCount: $shardCount")
      val keySelector = new HoloKeySelector(shardCount, inputSchema, holoSchema.getDistributionKeys)
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
      logger.info(s"repartition by holo distribution key finished")
      sparkSession.createDataFrame(rdd, inputSchema)
    } finally {
      client.close()
    }
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
