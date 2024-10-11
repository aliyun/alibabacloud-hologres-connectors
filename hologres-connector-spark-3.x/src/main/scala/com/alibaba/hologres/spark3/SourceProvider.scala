package com.alibaba.hologres.spark3

import com.alibaba.hologres.spark.BaseSourceProvider
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.utils.SparkHoloUtil
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter

/** SourceProvider Register. */
class SourceProvider extends DataSourceRegister
  with TableProvider {

  override def shortName(): String = "hologres"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val hologresConfigs: HologresConfigs = new HologresConfigs(options.asScala.toMap)
    SparkHoloUtil.getSparkTableSchema(hologresConfigs)
  }

  override def getTable(schema: StructType, transforms: Array[Transform], properties: util.Map[String, String]): Table = {
    val opts = properties.asScala.toMap
    val hologresConfigs = new HologresConfigs(opts)
    SparkHoloUtil.checkSparkTableSchema(hologresConfigs, schema)
    new HoloTable(schema, opts)
  }

  override def supportsExternalMetadata = true
}

object SourceProvider extends BaseSourceProvider {
  @deprecated("not need", "1.3.2")
  val INPUT_DATA_SCHEMA_DDL = "input_data_schema_ddl"
}
