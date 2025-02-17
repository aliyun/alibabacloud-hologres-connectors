package com.alibaba.hologres.spark3

import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.spark.BaseSourceProvider
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.utils.{RepartitionUtil, SparkHoloUtil}
import com.alibaba.hologres.spark3.sink.HologresRelation
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter

/** SourceProvider Register. */
class SourceProvider extends DataSourceRegister
  with TableProvider with CreatableRelationProvider {

  private var sparkSchema: StructType = _
  private var inferredSchema: Boolean = false
  // 如果通过query查询holo, mock一个holo schema一路传下去, 直接查询holo不需要使用
  private var mockHoloSchemaForQuery: TableSchema = _

  override def shortName(): String = "hologres"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val hologresConfigs: HologresConfigs = new HologresConfigs(options.asScala.toMap)
    inferredSchema = true
    if (hologresConfigs.isTableSource) {
      sparkSchema = SparkHoloUtil.inferSparkTableSchema(hologresConfigs)
    } else {
      mockHoloSchemaForQuery = SparkHoloUtil.mockHoloSchemaForQuery(hologresConfigs)
      sparkSchema = SparkHoloUtil.inferSparkTableSchema(hologresConfigs, mockHoloSchemaForQuery)
    }
    sparkSchema
  }

  override def getTable(sparkSchema: StructType, transforms: Array[Transform], properties: util.Map[String, String]): Table = {
    this.sparkSchema = sparkSchema
    val opts = properties.asScala.toMap
    val hologresConfigs = new HologresConfigs(opts)
    if (hologresConfigs.isTableSource) {
      if (!inferredSchema) {
        SparkHoloUtil.checkSparkTableSchema(hologresConfigs, sparkSchema)
      }
      new HoloTable(sparkSchema, hologresConfigs)
    } else {
      mockHoloSchemaForQuery = SparkHoloUtil.mockHoloSchemaForQuery(hologresConfigs)
      if (!inferredSchema) {
        SparkHoloUtil.checkSparkTableSchema(hologresConfigs, sparkSchema, mockHoloSchemaForQuery)
      }
      new HoloTable(sparkSchema, hologresConfigs, mockHoloSchemaForQuery)
    }
  }

  override def supportsExternalMetadata = true

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val hologresConfigs = new HologresConfigs(parameters)
    RepartitionUtil.reShuffleThenWrite(data, hologresConfigs.username, hologresConfigs.password, hologresConfigs.jdbcUrl,
      hologresConfigs.table, writeMode = hologresConfigs.writeMode.toString, onConflictAction = hologresConfigs.onConflictAction.name(),
      hologresConfigs.writeCopyMaxBufferSize, saveMode = mode)
    new HologresRelation(hologresConfigs, data.schema, mode == SaveMode.Overwrite)(sqlContext.sparkSession)
  }
}

object SourceProvider extends BaseSourceProvider {
  @deprecated("not need", "1.3.2")
  val INPUT_DATA_SCHEMA_DDL = "input_data_schema_ddl"
}
