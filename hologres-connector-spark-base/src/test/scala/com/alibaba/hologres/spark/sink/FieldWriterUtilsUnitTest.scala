package com.alibaba.hologres.spark.sink

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner
import org.scalatest.matchers.should.Matchers

@RunWith(classOf[JUnitRunner])
class FieldWriterUtilsUnitTest extends AnyFunSuite with Matchers {

  test("long array field writer turns null elements into default value") {
    val row = new GenericInternalRow(Array[Any](
      new GenericArrayData(Array[AnyRef](java.lang.Long.valueOf(1L), null, java.lang.Long.valueOf(3L)))
    ))
    val writer = FieldWriterUtils.createFieldWriter(
      StructField("ids", ArrayType(LongType, containsNull = true), nullable = true)
    )

    val value = writer.writeValue(row, 0).asInstanceOf[Array[Long]]
    value shouldEqual Array(1L, 0L, 3L)
  }

  test("string array field writer turns null elements into empty string") {
    val row = new GenericInternalRow(Array[Any](new GenericArrayData(Array[AnyRef](
      UTF8String.fromString("a"), null, UTF8String.fromString("c")
    ))))
    val writer = FieldWriterUtils.createFieldWriter(
      StructField("ids", ArrayType(StringType, containsNull = true), nullable = true)
    )

    val value = writer.writeValue(row, 0).asInstanceOf[Array[String]]
    value shouldEqual Array("a", "", "c")
  }
}
