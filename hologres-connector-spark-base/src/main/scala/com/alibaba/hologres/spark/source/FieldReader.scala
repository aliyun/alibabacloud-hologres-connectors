package com.alibaba.hologres.spark.source

import com.alibaba.hologres.org.postgresql.util.PGobject
import com.alibaba.hologres.spark.exception.SparkHoloException
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

import java.sql.{ResultSet, Types}

trait FieldReader {
  def readValue(resultSet: ResultSet, idx: Int): Any
}

class ShortFieldReader extends FieldReader {
  override def readValue(resultSet: ResultSet, idx: Int): Short = {
    resultSet.getShort(idx)
  }
}

class IntFieldReader extends FieldReader {
  override def readValue(resultSet: ResultSet, idx: Int): Int = {
    resultSet.getInt(idx)
  }
}

class LongFieldReader extends FieldReader {
  override def readValue(resultSet: ResultSet, idx: Int): Long = {
    resultSet.getLong(idx)
  }
}

class FloatFieldReader extends FieldReader {
  override def readValue(resultSet: ResultSet, idx: Int): Float = {
    resultSet.getFloat(idx)
  }
}

class DoubleFieldReader extends FieldReader {
  override def readValue(resultSet: ResultSet, idx: Int): Double = {
    resultSet.getDouble(idx)
  }
}

class DecimalFieldReader extends FieldReader {
  override def readValue(resultSet: ResultSet, idx: Int): Decimal = {
    Decimal.apply(resultSet.getBigDecimal(idx))
  }
}

class BooleanFieldReader extends FieldReader {
  override def readValue(resultSet: ResultSet, idx: Int): Boolean = {
    resultSet.getBoolean(idx)
  }
}

class StringFieldReader extends FieldReader {
  override def readValue(resultSet: ResultSet, idx: Int): UTF8String = {
    UTF8String.fromString(resultSet.getString(idx))
  }
}

class DateFieldReader extends FieldReader {
  override def readValue(resultSet: ResultSet, idx: Int): Int = {
    resultSet.getDate(idx).toLocalDate.toEpochDay.toInt
  }
}

class TimestampFieldReader extends FieldReader {
  override def readValue(resultSet: ResultSet, idx: Int): Long = {
    resultSet.getTimestamp(idx).getTime * 1000
  }
}

class BinaryFieldReader extends FieldReader {
  override def readValue(resultSet: ResultSet, idx: Int): Array[Byte] = {
    resultSet.getBytes(idx)
  }
}

class RoaringBitmapFieldReader extends FieldReader {
  override def readValue(resultSet: ResultSet, idx: Int): Array[Byte] = {
    val obj = resultSet.getObject(idx)
    obj match {
      case rbObject: PGobject =>
        var value = rbObject.getValue
        if (value.startsWith("\\x")) value = value.toLowerCase
        val bytes = new Array[Byte]((value.length - 2) >> 1)
        var i = 2
        while (i < value.length) {
          val highDit = (Character.digit(value.charAt(i), 16) & 0xFF).toByte
          val lowDit = (Character.digit(value.charAt(i + 1), 16) & 0xFF).toByte
          bytes(i / 2 - 1) = (highDit << 4 | lowDit).toByte
          i += 2
        }
        bytes
      case _ => throw new SparkHoloException("not support read roaringbitmap from " + obj.getClass);
    }
  }
}

class IntArrayFieldReader extends FieldReader {
  override def readValue(resultSet: ResultSet, idx: Int): ArrayData = {
    ArrayData.toArrayData(resultSet.getArray(idx).getArray.asInstanceOf[Array[java.lang.Integer]])
  }
}

class LongArrayFieldReader extends FieldReader {
  override def readValue(resultSet: ResultSet, idx: Int): ArrayData = {
    ArrayData.toArrayData(resultSet.getArray(idx).getArray.asInstanceOf[Array[java.lang.Long]])
  }
}

class FloatArrayFieldReader extends FieldReader {
  override def readValue(resultSet: ResultSet, idx: Int): ArrayData = {
    ArrayData.toArrayData(resultSet.getArray(idx).getArray.asInstanceOf[Array[java.lang.Float]])
  }
}

class DoubleArrayFieldReader extends FieldReader {
  override def readValue(resultSet: ResultSet, idx: Int): ArrayData = {
    ArrayData.toArrayData(resultSet.getArray(idx).getArray.asInstanceOf[Array[java.lang.Double]])
  }
}

class BooleanArrayFieldReader extends FieldReader {
  override def readValue(resultSet: ResultSet, idx: Int): ArrayData = {
    ArrayData.toArrayData(resultSet.getArray(idx).getArray.asInstanceOf[Array[java.lang.Boolean]])
  }
}

class StringArrayFieldReader extends FieldReader {
  override def readValue(resultSet: ResultSet, idx: Int): ArrayData = {
    ArrayData.toArrayData(resultSet.getArray(idx).getArray.asInstanceOf[Array[String]].map(UTF8String.fromString))
  }
}

object FieldReaderUtils {
  def createFieldReader(hologresType: Int, hologresTypeName: String): FieldReader = {
    hologresType match {
      case Types.TINYINT | Types.SMALLINT =>
        new ShortFieldReader
      case Types.INTEGER =>
        new IntFieldReader
      case Types.BIGINT =>
        new LongFieldReader
      case Types.REAL | Types.FLOAT =>
        new FloatFieldReader
      case Types.DOUBLE =>
        new DoubleFieldReader
      case Types.NUMERIC | Types.DECIMAL =>
        new DecimalFieldReader
      case Types.BOOLEAN | Types.BIT =>
        new BooleanFieldReader
      case Types.CHAR | Types.VARCHAR | Types.LONGVARCHAR =>
        new StringFieldReader
      case Types.DATE => new DateFieldReader
      case Types.TIMESTAMP => new TimestampFieldReader
      case Types.BINARY | Types.VARBINARY => new BinaryFieldReader
      case Types.OTHER =>
        hologresTypeName match {
          case "json" | "jsonb" => new StringFieldReader
          case "roaringbitmap" => new RoaringBitmapFieldReader
        }
      case Types.ARRAY =>
        hologresTypeName match {
          case "_int4" => new IntArrayFieldReader
          case "_int8" => new LongArrayFieldReader
          case "_float4" => new FloatArrayFieldReader
          case "_float8" => new DoubleArrayFieldReader
          case "_bool" => new BooleanArrayFieldReader
          case "_varchar" | "_text" => new StringArrayFieldReader
        }
      case _ =>
        throw new IllegalArgumentException(String.format("Hologres source does not support data type %s for now", hologresTypeName))
    }
  }

}
