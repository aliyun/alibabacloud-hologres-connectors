package com.alibaba.hologres.spark3.sink

import org.apache.spark.sql.connector.catalog.{SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/** HoloTable with SupportsWrite . */
class HoloTable(
                 table: String,
                 sparkSchema: StructType,
                 sourceOptions: Map[String, String]) extends SupportsWrite {
  override def name(): String = this.getClass.toString

  override def schema(): StructType = sparkSchema

  override def capabilities(): java.util.Set[TableCapability] = Set(
    TableCapability.BATCH_WRITE,
    TableCapability.ACCEPT_ANY_SCHEMA,
    TableCapability.OVERWRITE_BY_FILTER,
    TableCapability.OVERWRITE_DYNAMIC
  ).asJava

  override def newWriteBuilder(info: LogicalWriteInfo): HoloWriterBuilder = {
    new HoloWriterBuilder(table, sourceOptions, Some(sparkSchema))
  }
}
