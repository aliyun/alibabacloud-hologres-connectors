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
  private var cachedTableType: Option[HoloTableType.Value] = None

  object HoloTableType extends Enumeration {
    val TABLE_V1, TABLE_V2, QUERY, VIEW = Value
  }

  optimizeConfigs = SparkHoloUtil.chooseBestMode(sparkSchema, holoSchema, hologresConfigs)
  cachedTableType = Some(computeTableType())

  logger.info(s"Initial ${name()}")

  override def name(): String = {
    tableType() match {
      case HoloTableType.QUERY =>
        "HoloTableQuery(" + optimizeConfigs.jdbcUrl + ", " + optimizeConfigs.query + ")"
      case HoloTableType.TABLE_V1 =>
        "HoloTableV1(" + optimizeConfigs.jdbcUrl + ", " + optimizeConfigs.table + ")"
      case _ =>
        "HoloTableV2(" + optimizeConfigs.jdbcUrl + ", " + optimizeConfigs.table + ")"
    }
  }

  private def computeTableType(): HoloTableType.Value = {
    if (hologresConfigs.sourceType.equals("QUERY")) {
      HoloTableType.QUERY
    } else if (hologresConfigs.sourceType.equals("VIEW")) {
      HoloTableType.VIEW
    } else {
      if (optimizeConfigs.needReshuffle) {
        HoloTableType.TABLE_V1
      } else {
        HoloTableType.TABLE_V2
      }
    }
  }

  def tableType(): HoloTableType.Value = {
    cachedTableType.getOrElse {
      optimizeConfigs = SparkHoloUtil.chooseBestMode(sparkSchema, holoSchema, hologresConfigs)
      val t = computeTableType()
      cachedTableType = Some(t)
      t
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
    // 写入时过滤掉生成列，生成列由Hologres自动计算，不需要也不能由用户写入
    val writeSparkSchema = StructType(sparkSchema.fields.filter { field =>
      val colIndex = holoSchema.getColumnIndex(field.name)
      colIndex == null || !java.lang.Boolean.TRUE.equals(holoSchema.getColumn(colIndex).isGeneratedColumn)
    })
    // 检查plan中的schema是否与writeSparkSchema一致, plan中的schema只需要检查字段数量和类型
    SparkHoloUtil.checkSparkTableSchema(optimizeConfigs, writeSparkSchema, info.schema())
    if (tableType() == HoloTableType.TABLE_V1) {
      new HoloWriterBuilderV1(optimizeConfigs, writeSparkSchema)
    } else {
      new HoloWriterBuilder(optimizeConfigs, writeSparkSchema)
    }
  }

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder = {
    new HoloScanBuilder(optimizeConfigs, sparkSchema, holoSchema)
  }
}
