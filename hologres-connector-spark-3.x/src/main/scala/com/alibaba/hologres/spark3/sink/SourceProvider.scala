package com.alibaba.hologres.spark3.sink

import java.util

import com.alibaba.hologres.spark.sink.BaseSourceProvider
import org.apache.commons.cli.MissingArgumentException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters.mapAsScalaMapConverter

/** SourceProvider Register. */
class SourceProvider extends DataSourceRegister
  with TableProvider
  with Logging {
  override def shortName(): String = "hologres"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    StructType.fromDDL(options.get(SourceProvider.INPUT_DATA_SCHEMA_DDL))
  }

  override def getTable(schema: StructType, transforms: Array[Transform], properties: util.Map[String, String]): Table = {
    val opts = properties.asScala.toMap
    val table = opts.getOrElse("table",
      throw new MissingArgumentException("Missing necessary parameter 'table'."))
    new HoloTable(table, schema, opts)
  }
}

object SourceProvider extends BaseSourceProvider {
  val INPUT_DATA_SCHEMA_DDL = "input_data_schema_ddl"
}
