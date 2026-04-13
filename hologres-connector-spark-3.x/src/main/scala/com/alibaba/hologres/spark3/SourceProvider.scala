package com.alibaba.hologres.spark3

import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.spark.BaseSourceProvider
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.utils.{RepartitionUtil, SparkHoloUtil}
import com.alibaba.hologres.spark3.sink.HologresRelation
import org.apache.spark.SparkContext
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter

/** SourceProvider Register. */
class SourceProvider extends DataSourceRegister
  with TableProvider with CreatableRelationProvider {

  private var sparkSchema: StructType = _
  private var inferredSchema: Boolean = false
  // holo表的schema,如果是通过query或者view查询holo, 也需要mock一个holo schema
  private var holoSchema: TableSchema = _
  private var sourceType: String = "TABLE"
  var sparkAppName: String = SparkContext.getOrCreate().appName
  if (sparkAppName == null || "".eq(sparkAppName)) {
    sparkAppName = "default"
  }
  var sparkAppId: String = SparkContext.getOrCreate().applicationId
  if (sparkAppId == null) {
    sparkAppId = ""
  }

  override def shortName(): String = "hologres"

  /**
   * 用户不指定spark schema, 通过holo表的schema推断
   */
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val hologresConfigs: HologresConfigs = new HologresConfigs(options.asScala.toMap, sparkAppName, sparkAppId)
    inferredSchema = true
    val tuple2 = SparkHoloUtil.getHoloSchema(hologresConfigs)
    holoSchema = tuple2._1
    sourceType = tuple2._2
    sparkSchema = SparkHoloUtil.inferSparkTableSchema(holoSchema)
    sparkSchema
  }

  override def getTable(sparkSchema: StructType, transforms: Array[Transform], properties: util.Map[String, String]): Table = {
    this.sparkSchema = sparkSchema
    val opts = properties.asScala.toMap
    val hologresConfigs = new HologresConfigs(opts, sparkAppName, sparkAppId)
    if (holoSchema == null) {
      val tuple2 = SparkHoloUtil.getHoloSchema(hologresConfigs)
      holoSchema = tuple2._1
      sourceType = tuple2._2
    }
    hologresConfigs.sourceType = sourceType
    if (!inferredSchema) {
      SparkHoloUtil.checkSparkTableSchema(hologresConfigs, sparkSchema, holoSchema)
    }
    new HoloTable(sparkSchema, hologresConfigs, holoSchema)
  }

  override def supportsExternalMetadata = true

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val hologresConfigs = new HologresConfigs(parameters)
    RepartitionUtil.v1Write(data, hologresConfigs, saveMode = mode)
    new HologresRelation(hologresConfigs, data.schema, mode == SaveMode.Overwrite)(sqlContext.sparkSession)
  }
}

object SourceProvider extends BaseSourceProvider {
  @deprecated("not need", "1.3.2")
  val INPUT_DATA_SCHEMA_DDL = "input_data_schema_ddl"
}
