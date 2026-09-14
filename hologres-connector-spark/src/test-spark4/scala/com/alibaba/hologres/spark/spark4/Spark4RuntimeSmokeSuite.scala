package com.alibaba.hologres.spark4

import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.QueryTest
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

/**
 * 冒烟测试: 不依赖 Hologres 实例, 验证 Spark 4 运行时与 DSv2 注册链路是否连通.
 * 本模块其余测试均为集成测试, 需要真实 Hologres; 这个套件是唯一可以在 CI 中裸跑的.
 */
@RunWith(classOf[JUnitRunner])
class Spark4RuntimeSmokeSuite extends QueryTest with SharedSparkSession {

  test("spark 4 session works") {
    assert(spark.version.startsWith("4."))
    assert(spark.range(0, 10).count() === 10)
  }

  test("hologres short name resolves to neutral SourceProvider") {
    val clz = DataSource.lookupDataSource("hologres", spark.sessionState.conf)
    assert(clz.getName === "com.alibaba.hologres.spark.SourceProvider")
  }

  test("HoloTableCatalog is loadable via neutral and spark3 legacy paths") {
    // Spark 4 尚未发布过版本，仅需验证中性路径和已发布的 Spark 3 历史路径。
    val paths = Seq(
      "com.alibaba.hologres.spark.HoloTableCatalog",
      "com.alibaba.hologres.spark3.HoloTableCatalog")
    paths.foreach { path =>
      val clz = Class.forName(path)
      assert(classOf[org.apache.spark.sql.connector.catalog.TableCatalog].isAssignableFrom(clz), path)
    }
  }

  test("Apache Arrow MemoryUtil initializes on Java 17+") {
    Class.forName("org.apache.arrow.memory.util.MemoryUtil")
  }

  test("missing config fails with connector error, not linkage error") {
    val e = intercept[Exception] {
      spark.read.format("hologres").load().schema
    }
    val msg = String.valueOf(e.getMessage) + String.valueOf(e.getCause)
    assert(!msg.contains("UnsupportedClassVersionError"), msg)
    assert(!msg.contains("NoSuchMethodError"), msg)
    assert(!msg.contains("NoClassDefFoundError"), msg)
    // 期望是连接/参数校验类错误
    println("expected connector-level failure: " + e.getClass.getName + ": " + e.getMessage)
  }
}
