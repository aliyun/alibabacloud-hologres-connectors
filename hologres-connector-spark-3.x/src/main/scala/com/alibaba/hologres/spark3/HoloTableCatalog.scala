package com.alibaba.hologres.spark3

import com.alibaba.hologres.client.model.TableName
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.utils.{JDBCUtil, SparkHoloUtil}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{Identifier, NamespaceChange, SupportsNamespaces, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter

/**
 * HoloTableCatalog.
 * 每个catalog 对应一个 hologres database
 * namespace 对应 hologres 的 schema
 * table 对应 hologres 的 table
 * 目前不支持执行DDL, 只支持对已有表的查询和写入
 */
class HoloTableCatalog extends TableCatalog with SupportsNamespaces with Logging {
  private val logger = LoggerFactory.getLogger(getClass)

  private var catalogName: String = _
  private var hologresConfigs: HologresConfigs = _
  private var currentHoloSchema: String = "public"

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    assert(catalogName == null, "The Holo table catalog is already initialed")
    catalogName = name
    hologresConfigs = new HologresConfigs(options.asScala.toMap)
  }

  override def name(): String = {
    require(catalogName != null, "The Holo table catalog is not initialed")
    catalogName
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    checkNamespace(namespace)
    logger.info("listTables namespace: " + namespace.mkString(","))
    var tables = Array.empty[String]
    namespace match {
      case Array() =>
        tables = JDBCUtil.listTables(hologresConfigs, currentHoloSchema)
        tables.map(table => Identifier.of(Array(currentHoloSchema), s"$table"))
      case Array(schema) =>
        tables = JDBCUtil.listTables(hologresConfigs, schema)
        tables.map(table => Identifier.of(namespace, s"$table"))
    }
  }

  override def loadTable(ident: Identifier): Table = {
    logger.info(s"loadTable namespace: ${ident.namespace().mkString(",")}, name: ${ident.name()}")
    checkNamespace(ident.namespace())
    val specialTableConfigs: HologresConfigs = hologresConfigs.clone()
    ident.namespace() match {
      case Array(schema) =>
        specialTableConfigs.table = TableName.quoteValueOf(schema, ident.name()).getFullName
      case _ =>
        specialTableConfigs.table = TableName.quoteValueOf(currentHoloSchema, ident.name()).getFullName
    }
    val sparkSchema = SparkHoloUtil.inferSparkTableSchema(specialTableConfigs)
    new HoloTable(sparkSchema, specialTableConfigs)
  }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
    throw new UnsupportedOperationException("create table is not supported now.")
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("alter table is not supported now.")
  }

  override def dropTable(ident: Identifier): Boolean = {
    throw new UnsupportedOperationException("drop table is not supported now.")
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("rename table is not supported now.")
  }

  private def checkNamespace(namespace: Array[String]): Unit = {
    if (namespace.length > 1) {
      throw new IllegalArgumentException(
        s"""The namespace in holo catalog only supports single-layer, corresponding to the schema in hologres.
           But current namespace is: ${namespace.mkString(",")}""")
    }
  }

  /**
   * 列出所有namespace: 列出所有hologres当前database下的schema
   */
  override def listNamespaces(): Array[Array[String]] = {
    val databases = JDBCUtil.listSchemas(hologresConfigs)
    databases.map(Array(_))
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    throw new UnsupportedOperationException("list nested namespaces is not supported now.")
  }

  /**
   * 切换默认的namespace: 重置配置中jdbcUrl的默认database
   */
  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    checkNamespace(namespace)
    namespace match {
      case Array(schema) =>
        currentHoloSchema = schema
    }
    null
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = {
    throw new UnsupportedOperationException("create namespace is not supported now.")
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    throw new UnsupportedOperationException("alter namespace is not supported now.")
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    throw new UnsupportedOperationException("drop namespace is not supported now.")
  }
}


