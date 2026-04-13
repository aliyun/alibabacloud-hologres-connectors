package com.alibaba.hologres.spark3

import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.utils.{LoggerWrapper, SparkHoloUtil}
import com.alibaba.hologres.spark3.sink.{HoloWriterBuilder, HoloWriterBuilderV1}
import com.alibaba.hologres.spark3.source.HoloScanBuilder
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/** HoloTable with SupportsWrite, SupportsRead. */
class HoloTable(
                 sparkSchema: StructType,
                 hologresConfigs: HologresConfigs,
                 holoSchema: TableSchema) extends SupportsWrite with SupportsRead {
  private val logger = new LoggerWrapper(getClass)
  logger.setSparkAppName(hologresConfigs.sparkAppName)
  logger.setSparkAppId(hologresConfigs.sparkAppId)
  logger.setHoloTableName(hologresConfigs.table)
  private var optimizeConfigs: HologresConfigs = hologresConfigs

  logger.info(s"Initial ${name()}")

  override def name(): String = {
    if (tableType() == HoloTableType.QUERY) {
      "HoloTableQuery(" + optimizeConfigs.jdbcUrl + ", " + optimizeConfigs.query + ")"
    } else if (tableType() == HoloTableType.TABLE_V1) {
      "HoloTableV1(" + optimizeConfigs.jdbcUrl + ", " + optimizeConfigs.table + ")"
    } else {
      "HoloTableV2(" + optimizeConfigs.jdbcUrl + ", " + optimizeConfigs.table + ")"
    }
  }


  object HoloTableType extends Enumeration {
    val TABLE_V1, TABLE_V2, QUERY, VIEW = Value
  }

  def tableType(): HoloTableType.Value = {
    optimizeConfigs = SparkHoloUtil.chooseBestMode(sparkSchema, holoSchema, hologresConfigs)
    if (hologresConfigs.sourceType.equals("QUERY")) {
      HoloTableType.QUERY
    } else if (hologresConfigs.sourceType.equals("VIEW")) {
      HoloTableType.VIEW
    } else {
      if (optimizeConfigs.needReshuffle || optimizeConfigs.useV1Write) {
        HoloTableType.TABLE_V1
      } else {
        HoloTableType.TABLE_V2
      }
    }
  }

  override def schema(): StructType = sparkSchema

  override def capabilities(): java.util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    if (tableType().equals(HoloTableType.TABLE_V1)) TableCapability.V1_BATCH_WRITE else TableCapability.BATCH_WRITE,
    TableCapability.ACCEPT_ANY_SCHEMA,
    TableCapability.OVERWRITE_BY_FILTER,
    TableCapability.OVERWRITE_DYNAMIC
  ).asJava

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    if (tableType() == HoloTableType.TABLE_V1) {
      new HoloWriterBuilderV1(optimizeConfigs, sparkSchema)
    } else {
      // 对于Table V2检查plan中的schema是否与sparkSchema一致, plan中的schema只需要检查字段数量和类型
      SparkHoloUtil.checkSparkTableSchema(optimizeConfigs, sparkSchema, info.schema())
      new HoloWriterBuilder(optimizeConfigs, sparkSchema)
    }
  }

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder = {
    new HoloScanBuilder(optimizeConfigs, sparkSchema, holoSchema)
  }
}
