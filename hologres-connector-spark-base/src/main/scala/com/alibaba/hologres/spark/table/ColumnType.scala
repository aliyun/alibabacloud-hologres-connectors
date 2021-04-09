package com.alibaba.hologres.spark.table

/** ColumnType of a schema. */
object ColumnType extends Enumeration {
  type ColumnType = Value
  val INT, BIGINT, FLOAT, DOUBLE, BOOLEAN, TIMESTAMP, TEXT, DECIMAL, DATE, BYTEA, INTA, BIGINTA, FLOATA, DOUBLEA, BOOLEANA, TEXTA = Value
}
