package com.alibaba.hologres.spark

import com.alibaba.hologres.client.model.{Column, TableName, TableSchema}
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.utils.SparkHoloUtil
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner
import org.scalatest.matchers.should.Matchers

import java.sql.Types

@RunWith(classOf[JUnitRunner])
class SparkHoloSchemaCheckUnitTest extends AnyFunSuite with Matchers {

  private def defaultConfig(byName: Boolean = false, strict: Boolean = false): HologresConfigs = {
    new HologresConfigs(Map(
      "jdbcurl" -> "jdbc:postgresql://localhost:5432/testdb",
      "username" -> "testuser",
      "password" -> "testpass",
      "table" -> "test_table",
      "write.catalog.by_name" -> byName.toString,
      "write.strict_datatype_check" -> strict.toString
    ))
  }

  private def column(
      name: String,
      sqlType: Int,
      typeName: String,
      primaryKey: Boolean = false,
      generated: Boolean = false): Column = {
    val column = new Column()
    column.setName(name)
    column.setType(sqlType)
    column.setTypeName(typeName)
    column.setAllowNull(!primaryKey)
    column.setPrimaryKey(primaryKey)
    column.setGeneratedColumn(generated)
    column
  }

  private def holoSchema(partitionColumn: Option[String] = None): TableSchema = {
    val builder = new TableSchema.Builder()
      .setTableName(TableName.valueOf("public.test_table"))
      .addColumn(column("id", Types.BIGINT, "int8", primaryKey = true))
      .addColumn(column("name", Types.VARCHAR, "text"))
      .addColumn(column("amount", Types.INTEGER, "int4"))
      .addColumn(column("generated_value", Types.BIGINT, "int8", generated = true))
    partitionColumn.foreach(builder.setPartitionColumnName)
    val schema = builder.build()
    schema.calculateProperties()
    schema
  }

  private val writableSchema = StructType(Seq(
    StructField("id", LongType),
    StructField("name", StringType),
    StructField("amount", IntegerType)
  ))

  test("schema check ignores array containsNull difference") {
    val sparkSchema = StructType(Seq(
      StructField("id", LongType, nullable = true),
      StructField("ids", ArrayType(LongType, containsNull = false), nullable = false)
    ))
    val planSchema = StructType(Seq(
      StructField("id", LongType, nullable = true),
      StructField("ids", ArrayType(LongType, containsNull = true), nullable = true)
    ))

    noException should be thrownBy {
      SparkHoloUtil.checkSparkTableSchema(defaultConfig(), sparkSchema, planSchema)
    }
  }

  test("schema check still rejects array element type mismatch") {
    val sparkSchema = StructType(Seq(
      StructField("ids", ArrayType(LongType, containsNull = true), nullable = true)
    ))
    val planSchema = StructType(Seq(
      StructField("ids", ArrayType(IntegerType, containsNull = true), nullable = true)
    ))

    val ex = intercept[IllegalArgumentException] {
      SparkHoloUtil.checkSparkTableSchema(defaultConfig(), sparkSchema, planSchema)
    }
    ex.getMessage should include("schema not match in field 0")
  }

  test("catalog by-name write is disabled by default") {
    defaultConfig().writeCatalogByName shouldBe false
    val partialSchema = StructType(Seq(StructField("id", LongType)))
    val ex = intercept[IllegalArgumentException] {
      SparkHoloUtil.resolveCatalogWriteSchema(defaultConfig(), writableSchema, partialSchema, holoSchema())
    }
    ex.getMessage should include("schema length not match")
  }

  test("catalog by-name write resolves partial columns in input order") {
    val planSchema = StructType(Seq(
      StructField("amount", IntegerType, nullable = false),
      StructField("id", LongType, nullable = false)
    ))
    val resolved = SparkHoloUtil.resolveCatalogWriteSchema(
      defaultConfig(byName = true), writableSchema, planSchema, holoSchema())

    resolved.fieldNames should contain theSameElementsInOrderAs Array("amount", "id")
    resolved.fields.map(_.dataType) should contain theSameElementsInOrderAs Array(IntegerType, LongType)
    resolved.fields.foreach(_.nullable shouldBe false)
  }

  test("catalog by-name write normalizes a unique case-insensitive match") {
    val planSchema = StructType(Seq(
      StructField("ID", LongType),
      StructField("NAME", StringType)
    ))
    val resolved = SparkHoloUtil.resolveCatalogWriteSchema(
      defaultConfig(byName = true), writableSchema, planSchema, holoSchema())

    resolved.fieldNames should contain theSameElementsInOrderAs Array("id", "name")
  }

  test("catalog by-name write rejects unknown duplicate and generated columns") {
    val config = defaultConfig(byName = true)
    val schema = holoSchema()

    val unknown = intercept[IllegalArgumentException] {
      SparkHoloUtil.resolveCatalogWriteSchema(config, writableSchema,
        StructType(Seq(StructField("id", LongType), StructField("missing", StringType))), schema)
    }
    unknown.getMessage should include("does not exist")

    val duplicate = intercept[IllegalArgumentException] {
      SparkHoloUtil.resolveCatalogWriteSchema(config, writableSchema,
        StructType(Seq(StructField("id", LongType), StructField("ID", LongType))), schema)
    }
    duplicate.getMessage should include("specified more than once")

    val generated = intercept[IllegalArgumentException] {
      SparkHoloUtil.resolveCatalogWriteSchema(config, writableSchema,
        StructType(Seq(StructField("id", LongType), StructField("generated_value", LongType))), schema)
    }
    generated.getMessage should include("generated column")

    val ambiguousBuilder = new TableSchema.Builder()
      .setTableName(TableName.valueOf("public.ambiguous_table"))
      .addColumn(column("id", Types.BIGINT, "int8", primaryKey = true))
      .addColumn(column("Name", Types.VARCHAR, "text"))
      .addColumn(column("name", Types.VARCHAR, "text"))
    val ambiguousSchema = ambiguousBuilder.build()
    ambiguousSchema.calculateProperties()
    val ambiguous = intercept[IllegalArgumentException] {
      SparkHoloUtil.resolveCatalogWriteSchema(config, writableSchema,
        StructType(Seq(StructField("id", LongType), StructField("NAME", StringType))), ambiguousSchema)
    }
    ambiguous.getMessage should include("ambiguous")
  }

  test("catalog by-name write requires primary and physical partition columns") {
    val config = defaultConfig(byName = true)

    val missingPrimaryKey = intercept[IllegalArgumentException] {
      SparkHoloUtil.resolveCatalogWriteSchema(config, writableSchema,
        StructType(Seq(StructField("name", StringType))), holoSchema())
    }
    missingPrimaryKey.getMessage should include("id")

    val missingPartitionKey = intercept[IllegalArgumentException] {
      SparkHoloUtil.resolveCatalogWriteSchema(config, writableSchema,
        StructType(Seq(StructField("id", LongType), StructField("name", StringType))), holoSchema(Some("amount")))
    }
    missingPartitionKey.getMessage should include("amount")
  }

  test("catalog by-name write applies strict and implicit type checks") {
    val planSchema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType)
    ))
    val resolved = SparkHoloUtil.resolveCatalogWriteSchema(
      defaultConfig(byName = true), writableSchema, planSchema, holoSchema())
    resolved("id").dataType shouldBe IntegerType

    val strictError = intercept[IllegalArgumentException] {
      SparkHoloUtil.resolveCatalogWriteSchema(
        defaultConfig(byName = true, strict = true), writableSchema, planSchema, holoSchema())
    }
    strictError.getMessage should include("schema not match for column id")
  }
}
