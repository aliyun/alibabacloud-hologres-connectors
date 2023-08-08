package com.alibaba.hologres.spark.sink

import com.alibaba.hologres.client.model.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StringType

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

class StringFieldWriter extends FieldWriter {
  override def writeValue(row: InternalRow, idx: Int): String = {
    row.getString(idx)
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

object FieldWriterUtils {
  def createFieldWriter(holoColumn: Column): FieldWriter = {
    holoColumn.getType match {
      case Types.TINYINT | Types.SMALLINT =>
        new ShortFieldWriter
      case Types.INTEGER =>
        new IntFieldWriter
      case Types.BIGINT =>
        new LongFieldWriter
      case Types.REAL | Types.FLOAT =>
        new FloatFieldWriter
      case Types.DOUBLE =>
        new DoubleFieldWriter
      case Types.NUMERIC | Types.DECIMAL =>
        new DecimalFieldWriter(holoColumn.getPrecision, holoColumn.getScale)
      case Types.BOOLEAN | Types.BIT =>
        new BooleanFieldWriter
      case Types.CHAR | Types.VARCHAR | Types.LONGVARCHAR =>
        new StringFieldWriter
      case Types.DATE => new DateFieldWriter
      case Types.TIMESTAMP => new TimestampFieldWriter
      case Types.BINARY | Types.VARBINARY => new BinaryFieldWriter
      case Types.OTHER =>
        holoColumn.getTypeName match {
          case "json" | "jsonb" => new StringFieldWriter
          case "roaringbitmap" => new BinaryFieldWriter
        }
      case Types.ARRAY =>
        holoColumn.getTypeName match {
          case "_int4" => new IntArrayFieldWriter
          case "_int8" => new LongArrayFieldWriter
          case "_float4" => new FloatArrayFieldWriter
          case "_float8" => new DoubleArrayFieldWriter
          case "_bool" => new BooleanArrayFieldWriter
          case "_varchar" | "_text" => new StringArrayFieldWriter
        }
      case _ =>
        throw new IllegalArgumentException(String.format("Hologres sink does not support data type %s for now", holoColumn.getTypeName))
    }
  }

}
