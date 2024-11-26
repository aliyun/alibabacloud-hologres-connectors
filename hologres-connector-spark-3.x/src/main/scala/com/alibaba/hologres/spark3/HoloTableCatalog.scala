package com.alibaba.hologres.spark3

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
 * namespace 对应 hologres 的 database
 * table 对应 hologres 的 table, 可以包含schema,但必须使用``包起来
 * 目前不支持执行DDL, 只支持对已有表的查询和写入
 */
class HoloTableCatalog extends TableCatalog with SupportsNamespaces with Logging {
  private val logger = LoggerFactory.getLogger(getClass)

  private var catalogName: String = _
  private var hologresConfigs: HologresConfigs = _

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
    if (namespace.isEmpty) {
      tables = JDBCUtil.listTables(hologresConfigs)
    } else {
      tables = JDBCUtil.listTables(hologresConfigs, namespace.head)
    }
    tables.map(table => Identifier.of(namespace, s"`$table`"))
  }

  override def loadTable(ident: Identifier): Table = {
    logger.info(s"loadTable namespace: ${ident.namespace().mkString(",")}, name: ${ident.name()}")
    checkNamespace(ident.namespace())
    val specialTableConfigs: HologresConfigs = hologresConfigs.clone()
    if (ident.namespace().nonEmpty) {
      // 表名是 <database>.<table> 格式
      specialTableConfigs.resetJdbcUrl(JDBCUtil.replaceUrlDatabase(hologresConfigs.jdbcUrl, ident.namespace().head))
    }
    specialTableConfigs.table = ident.name()
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
        s"""a namespace in Holo Table catalog corresponds to a hologres database, " +
        "do not use <database>.<schema> bug only <database> as the namespace, " +
        "if want read or write a table with schema, use <database>.`<schema>.<table>`, but now is ${namespace.mkString(",")}""")
    }
  }

  /**
   * 列出所有namespace: 列出所有hologres数据库
   */
  override def listNamespaces(): Array[Array[String]] = {
    val databases = JDBCUtil.listDatabases(hologresConfigs)
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
    hologresConfigs.resetJdbcUrl(JDBCUtil.replaceUrlDatabase(hologresConfigs.jdbcUrl, namespace.head))
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


