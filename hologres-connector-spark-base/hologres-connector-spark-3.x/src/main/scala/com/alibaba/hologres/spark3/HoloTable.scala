package com.alibaba.hologres.spark3

import com.alibaba.hologres.spark3.sink.HoloWriterBuilder
import com.alibaba.hologres.spark3.source.HoloScanBuilder
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/** HoloTable with SupportsWrite . */
class HoloTable(
                 sparkSchema: StructType,
                 sourceOptions: Map[String, String]) extends SupportsWrite with SupportsRead {
  override def name(): String = this.getClass.toString

  override def schema(): StructType = sparkSchema

  override def capabilities(): java.util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
    TableCapability.ACCEPT_ANY_SCHEMA,
    TableCapability.OVERWRITE_BY_FILTER,
    TableCapability.OVERWRITE_DYNAMIC
  ).asJava

  override def newWriteBuilder(info: LogicalWriteInfo): HoloWriterBuilder = {
    new HoloWriterBuilder(sourceOptions, sparkSchema)
  }

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder = {
    new HoloScanBuilder(sourceOptions, sparkSchema)
  }
}
