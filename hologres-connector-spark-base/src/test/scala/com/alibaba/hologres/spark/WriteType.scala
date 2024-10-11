package com.alibaba.hologres.spark

object WriteType extends Enumeration {
  type Color = Value
  val
  DISABLE, // insert
  STREAM,  // fixed_copy
  BULK_LOAD // bulk_load
  = Value
}
