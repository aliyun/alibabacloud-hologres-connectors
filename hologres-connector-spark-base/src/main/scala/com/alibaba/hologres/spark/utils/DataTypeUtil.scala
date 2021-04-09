package com.alibaba.hologres.spark.utils

import com.alibaba.hologres.spark.exception.SparkHoloException
import org.slf4j.LoggerFactory

/** DataType utils. */
object DataTypeUtil {
  private val logger = LoggerFactory.getLogger(getClass)

  def getDecimalParameter(typeName: String): Array[Int] = {
    val res = Array(38, 18)
    if (typeName.startsWith("DecimalType")) {
      val numPatter = "[0-9]+".r
      var idx = 0
      numPatter.findAllIn(typeName).foreach(a => {
        if (idx == 0) {
          res(0) = a.toInt
          idx = idx + 1
        } else res(1) = a.toInt
      })
      res
    } else {
      val e = s"typeName $typeName Not a DecimalType"
      logger.error(e)
      throw new SparkHoloException(e)
    }
  }

  def getArrayType(typeName: String): String = {
    if (typeName.startsWith("ArrayType")) {
      if (typeName.contains("IntegerType")) {
        "IntegerType"
      } else if (typeName.contains("LongType")) {
        "LongType"
      } else if (typeName.contains("FloatType")) {
        "FloatType"
      } else if (typeName.contains("DoubleType")) {
        "DoubleType"
      } else if (typeName.contains("BooleanType")) {
        "BooleanType"
      } else {
        "StringType"
      }
    } else {
      val e = s"typeName $typeName Not a ArrayType"
      logger.error(e)
      throw new SparkHoloException(e)
    }
  }
}
