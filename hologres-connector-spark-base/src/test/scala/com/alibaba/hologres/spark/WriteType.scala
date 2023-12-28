package com.alibaba.hologres.spark

object WriteType extends Enumeration {
  type Color = Value
  val INSERT, FIXED_COPY, BULK_LOAD = Value
}
