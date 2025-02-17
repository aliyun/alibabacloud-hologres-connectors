package com.alibaba.hologres.spark

object WriteType extends Enumeration {
  type Color = Value
  val
  AUTO,
  INSERT, // insert
  STREAM, // fixed_copy
  BULK_LOAD // bulk_load
  = Value
}

object ReadType extends Enumeration {
  type Color = Value
  val
  AUTO,
  SELECT, // select
  BULK_READ,  // copy out arrow format
  BULK_READ_COMPRESSED  // copy out arrow format
  = Value
}
