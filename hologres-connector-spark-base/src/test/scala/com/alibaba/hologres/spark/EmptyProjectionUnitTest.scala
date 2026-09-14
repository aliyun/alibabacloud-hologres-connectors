package com.alibaba.hologres.spark

import com.alibaba.hologres.spark.utils.JDBCUtil
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

/**
 * 空投影退化为常量投影的单测.
 *
 * Spark 列裁剪后 required schema 可能为空(如 select count(*)), 直接拼成 "select  from t"
 * 会让服务端返回的列数与请求对不上, 因此退化为常量投影.
 */
@RunWith(classOf[JUnitRunner])
class EmptyProjectionUnitTest extends AnyFunSuite {

  test("empty select fields on table degrade to constant projection") {
    val sql = JDBCUtil.getSimpleSelectFromTable("\"public\".\"t\"", Array.empty[String])
    assert(sql == "select 1 from \"public\".\"t\"")
    // 关键: 不能出现 select 与 from 之间为空的情况
    assert(!sql.contains("select  from"))
  }

  test("empty select fields on query degrade to constant projection") {
    val sql = JDBCUtil.getSimpleSelectFromQuery("select * from t", Array.empty[String])
    assert(sql == "select 1 from (select * from t) t")
    assert(!sql.contains("select  from"))
  }

  test("non empty select fields are quoted and unchanged") {
    assert(JDBCUtil.getSimpleSelectFromTable("t", Array("pk")) == "select \"pk\" from t")
    assert(JDBCUtil.getSimpleSelectFromTable("t", Array("pk", "name")) ==
      "select \"pk\", \"name\" from t")
    assert(JDBCUtil.getSimpleSelectFromQuery("select * from s", Array("pk", "name")) ==
      "select \"pk\", \"name\" from (select * from s) t")
  }
}
