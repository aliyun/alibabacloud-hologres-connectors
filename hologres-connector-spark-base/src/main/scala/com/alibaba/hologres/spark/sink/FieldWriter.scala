package com.alibaba.hologres.spark.sink

import com.alibaba.hologres.client.model.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp, Types}
import java.time.LocalDate

trait FieldWriter {
  def writeValue(row: InternalRow, idx: Int): Any
}

class ShortFieldWriter extends FieldWriter {
  override def writeValue(row: InternalRow, idx: Int): Short = {
    row.getShort(idx)
  }
}

class IntFieldWriter extends FieldWriter {
  override def writeValue(row: InternalRow, idx: Int): Int = {
    row.getInt(idx)
  }
}

class LongFieldWriter extends FieldWriter {
  override def writeValue(row: InternalRow, idx: Int): Long = {
    row.getLong(idx)
  }
}

class FloatFieldWriter extends FieldWriter {
  override def writeValue(row: InternalRow, idx: Int): Float = {
    row.getFloat(idx)
  }
}

class DoubleFieldWriter extends FieldWriter {
  override def writeValue(row: InternalRow, idx: Int): Double = {
    row.getDouble(idx)
  }
}

class DecimalFieldWriter(precision: Int, scale: Int) extends FieldWriter {
  override def writeValue(row: InternalRow, idx: Int): java.math.BigDecimal = {
    row.getDecimal(idx, precision, scale).toJavaBigDecimal
  }
}

class BooleanFieldWriter extends FieldWriter {
  override def writeValue(row: InternalRow, idx: Int): Boolean = {
    row.getBoolean(idx)
  }
}

class StringFieldWriter(removeU0000: Boolean) extends FieldWriter {
  override def writeValue(row: InternalRow, idx: Int): String = {
    if (removeU0000) {
      row.getString(idx).replaceAll("\u0000", "")
    } else {
      row.getString(idx)
    }
  }
}

class DateFieldWriter extends FieldWriter {
  override def writeValue(row: InternalRow, idx: Int): Date = {
    Date.valueOf(LocalDate.ofEpochDay(row.getLong(idx)))
  }
}

class TimestampFieldWriter extends FieldWriter {
  override def writeValue(row: InternalRow, idx: Int): Timestamp = {
    new Timestamp(row.getLong(idx) / 1000)
  }
}

class BinaryFieldWriter extends FieldWriter {
  override def writeValue(row: InternalRow, idx: Int): Array[Byte] = {
    row.getBinary(idx)
  }
}

class IntArrayFieldWriter extends FieldWriter {
  override def writeValue(row: InternalRow, idx: Int): Array[Int] = {
    row.getArray(idx).toIntArray()
  }
}

class LongArrayFieldWriter extends FieldWriter {
  override def writeValue(row: InternalRow, idx: Int): Array[Long] = {
    row.getArray(idx).toLongArray()
  }
}

class FloatArrayFieldWriter extends FieldWriter {
  override def writeValue(row: InternalRow, idx: Int): Array[Float] = {
    row.getArray(idx).toFloatArray()
  }
}

class DoubleArrayFieldWriter extends FieldWriter {
  override def writeValue(row: InternalRow, idx: Int): Array[Double] = {
    row.getArray(idx).toDoubleArray()
  }
}

class BooleanArrayFieldWriter extends FieldWriter {
  override def writeValue(row: InternalRow, idx: Int): Array[Boolean] = {
    row.getArray(idx).toBooleanArray()
  }
}

class StringArrayFieldWriter extends FieldWriter {
  override def writeValue(row: InternalRow, idx: Int): Array[String] = {
    row.getArray(idx).toObjectArray(StringType).map(e => {
      // 与InternalRow get array 表现一致，当数组元素有null值时，使用默认值空字符串""
      if (e == null) {
        ""
      } else {
        e.toString
      }
    })
  }
}

/**
 * 目前仅支持低精度到高精度的类型转换，例如int -> long，或者long -> string
 * 不支持高精度到低精度的类型转换，例如long -> int， 以及string -> long
 */
trait Caster {
  def castValue(value: Any): Any
}

class ToOriginCaster extends Caster {
  override def castValue(value: Any): Any = {
    value
  }
}

class ToStringCaster extends Caster {
  override def castValue(value: Any): Any = {
    value.toString
  }
}

class ToIntCaster extends Caster {
  override def castValue(value: Any): Any = {
    value match {
      case value: Short => value.toInt
      case _ => value
    }
  }
}

class ToLongCaster extends Caster {
  override def castValue(value: Any): Any = {
    value match {
      case value: Short => value.toLong
      case value: Int => value.toLong
      case _ => value
    }
  }
}

class ToDoubleCaster extends Caster {
  override def castValue(value: Any): Any = {
    value match {
      case value: Float => value.toDouble
      case _ => value
    }
  }
}


object FieldWriterUtils {
  def createFieldWriter(sparkColumn: StructField, removeU0000: Boolean = false): FieldWriter = {
    sparkColumn.dataType match {
      case DataTypes.ShortType =>
        new ShortFieldWriter
      case DataTypes.IntegerType =>
        new IntFieldWriter
      case DataTypes.LongType =>
        new LongFieldWriter
      case DataTypes.FloatType =>
        new FloatFieldWriter
      case DataTypes.DoubleType =>
        new DoubleFieldWriter
      case DecimalType() =>
        val decimalType = sparkColumn.dataType.asInstanceOf[DecimalType]
        new DecimalFieldWriter(decimalType.precision, decimalType.scale)
      case DataTypes.BooleanType =>
        new BooleanFieldWriter
      case DataTypes.StringType =>
        new StringFieldWriter(removeU0000)
      case DataTypes.DateType => new DateFieldWriter
      case DataTypes.TimestampType => new TimestampFieldWriter
      case DataTypes.BinaryType => new BinaryFieldWriter
      case ArrayType(DataTypes.IntegerType, _) => new IntArrayFieldWriter
      case ArrayType(DataTypes.LongType, _) => new LongArrayFieldWriter
      case ArrayType(DataTypes.FloatType, _) => new FloatArrayFieldWriter
      case ArrayType(DataTypes.DoubleType, _) => new DoubleArrayFieldWriter
      case ArrayType(DataTypes.BooleanType, _) => new BooleanArrayFieldWriter
      case ArrayType(DataTypes.StringType, _) => new StringArrayFieldWriter
      case _ =>
        throw new IllegalArgumentException(String.format("Hologres sink does not support data type %s for now", sparkColumn.dataType.typeName))
    }
  }


  def createCaster(holoColumn: Column): Caster = {
    holoColumn.getType match {
      case Types.INTEGER =>
        new ToIntCaster
      case Types.BIGINT =>
        new ToLongCaster
      case Types.DOUBLE =>
        new ToDoubleCaster
      case Types.VARCHAR | Types.CHAR | Types.LONGVARCHAR | Types.LONGNVARCHAR =>
        new ToStringCaster
      case _ =>
        new ToOriginCaster
    }
  }
}
