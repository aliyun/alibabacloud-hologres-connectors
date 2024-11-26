package com.alibaba.hologres.spark3

import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark3.sink.HoloWriterBuilder
import com.alibaba.hologres.spark3.source.HoloScanBuilder
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/** HoloTable with SupportsWrite, SupportsRead. */
class HoloTable(
                 sparkSchema: StructType,
                 hologresConfigs: HologresConfigs,
                 mockHoloSchemaForQuery: TableSchema = null) extends SupportsWrite with SupportsRead {
  override def name(): String = this.getClass.toString

  object HoloTableType extends Enumeration {
    val TABLE, QUERY = Value
  }

  def tableType(): HoloTableType.Value = {
    if (mockHoloSchemaForQuery != null) {
      HoloTableType.QUERY
    } else {
      HoloTableType.TABLE
    }
  }

  override def schema(): StructType = sparkSchema

  override def capabilities(): java.util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
    TableCapability.ACCEPT_ANY_SCHEMA,
    TableCapability.OVERWRITE_BY_FILTER,
    TableCapability.OVERWRITE_DYNAMIC
  ).asJava

  override def newWriteBuilder(info: LogicalWriteInfo): HoloWriterBuilder = {
    new HoloWriterBuilder(hologresConfigs, sparkSchema)
  }

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder = {
    if (tableType() == HoloTableType.TABLE) {
      new HoloScanBuilder(hologresConfigs, sparkSchema)
    } else {
      new HoloScanBuilder(hologresConfigs, sparkSchema, mockHoloSchemaForQuery)
    }
  }
}
