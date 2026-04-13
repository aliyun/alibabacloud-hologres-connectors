package com.alibaba.hologres.client.copy.in.arrow;

import com.alibaba.hologres.client.copy.in.arrow.creator.*;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.TableSchema;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** HoloArrowVectorCreatorUtil. */
public class ArrowVectorCreatorUtil {

    public static Schema createArrowSchema(TableSchema schema, List<String> columns) {
        if (schema == null || columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Schema and columns cannot be null or empty.");
        }
        List<Field> fields = new ArrayList<>();
        for (String columnName : columns) {
            int index = schema.getColumnIndex(columnName);
            Column column = schema.getColumn(index);
            Field field;
            switch (column.getType()) {
                case Types.INTEGER:
                    field =
                            new Field(
                                    columnName,
                                    FieldType.nullable(new ArrowType.Int(32, true)),
                                    null);
                    break;
                case Types.BIGINT:
                    field =
                            new Field(
                                    columnName,
                                    FieldType.nullable(new ArrowType.Int(64, true)),
                                    null);
                    break;
                case Types.SMALLINT:
                    field =
                            new Field(
                                    columnName,
                                    FieldType.nullable(new ArrowType.Int(16, true)),
                                    null);
                    break;
                case Types.FLOAT:
                case Types.REAL:
                    field =
                            new Field(
                                    columnName,
                                    FieldType.nullable(
                                            new ArrowType.FloatingPoint(
                                                    FloatingPointPrecision.SINGLE)),
                                    null);
                    break;
                case Types.DOUBLE:
                    field =
                            new Field(
                                    columnName,
                                    FieldType.nullable(
                                            new ArrowType.FloatingPoint(
                                                    FloatingPointPrecision.DOUBLE)),
                                    null);
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                    field =
                            new Field(
                                    columnName,
                                    FieldType.nullable(
                                            new ArrowType.Decimal(
                                                    column.getPrecision(), column.getScale())),
                                    null);
                    break;
                case Types.BOOLEAN:
                case Types.BIT:
                    field = new Field(columnName, FieldType.nullable(new ArrowType.Bool()), null);
                    break;
                case Types.CHAR:
                    field = new Field(columnName, FieldType.nullable(new ArrowType.Utf8()), null);
                    break;
                case Types.VARCHAR:
                    field = new Field(columnName, FieldType.nullable(new ArrowType.Utf8()), null);
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                    field = new Field(columnName, FieldType.nullable(new ArrowType.Binary()), null);
                    break;
                case Types.TIMESTAMP:
                case Types.TIME_WITH_TIMEZONE:
                    if (column.getTypeName().equals("timestamptz")) {
                        field =
                                new Field(
                                        columnName,
                                        FieldType.nullable(
                                                new ArrowType.Date(DateUnit.MILLISECOND)),
                                        null);
                    } else {
                        field =
                                new Field(
                                        columnName,
                                        FieldType.nullable(
                                                new ArrowType.Timestamp(
                                                        TimeUnit.MICROSECOND, null)),
                                        null);
                    }
                    break;
                case Types.TIME:
                    if (column.getTypeName().equals("timetz")) {
                        field =
                                new Field(
                                        columnName,
                                        FieldType.nullable(new ArrowType.FixedSizeBinary(16)),
                                        null);
                    } else {
                        field =
                                new Field(
                                        columnName,
                                        FieldType.nullable(
                                                new ArrowType.Time(TimeUnit.MICROSECOND, 64)),
                                        null);
                    }
                    break;
                case Types.DATE:
                    field =
                            new Field(
                                    columnName,
                                    FieldType.nullable(new ArrowType.Date(DateUnit.DAY)),
                                    null);
                    break;
                case Types.ARRAY:
                    switch (column.getTypeName()) {
                        case "_int4":
                            field =
                                    new Field(
                                            columnName,
                                            FieldType.nullable(ArrowType.List.INSTANCE),
                                            Collections.singletonList(
                                                    new Field(
                                                            "item",
                                                            FieldType.notNullable(
                                                                    new ArrowType.Int(32, true)),
                                                            null)));
                            break;
                        case "_int8":
                            field =
                                    new Field(
                                            columnName,
                                            FieldType.nullable(ArrowType.List.INSTANCE),
                                            Collections.singletonList(
                                                    new Field(
                                                            "item",
                                                            FieldType.notNullable(
                                                                    new ArrowType.Int(64, true)),
                                                            null)));
                            break;
                        case "_float4":
                            field =
                                    new Field(
                                            columnName,
                                            FieldType.nullable(ArrowType.List.INSTANCE),
                                            Collections.singletonList(
                                                    new Field(
                                                            "item",
                                                            FieldType.notNullable(
                                                                    new ArrowType.FloatingPoint(
                                                                            FloatingPointPrecision
                                                                                    .SINGLE)),
                                                            null)));
                            break;
                        case "_float8":
                            field =
                                    new Field(
                                            columnName,
                                            FieldType.nullable(ArrowType.List.INSTANCE),
                                            Collections.singletonList(
                                                    new Field(
                                                            "item",
                                                            FieldType.notNullable(
                                                                    new ArrowType.FloatingPoint(
                                                                            FloatingPointPrecision
                                                                                    .DOUBLE)),
                                                            null)));
                            break;
                        case "_bool":
                            field =
                                    new Field(
                                            columnName,
                                            FieldType.nullable(ArrowType.List.INSTANCE),
                                            Collections.singletonList(
                                                    new Field(
                                                            "item",
                                                            FieldType.notNullable(
                                                                    new ArrowType.Bool()),
                                                            null)));
                            break;
                        case "_text":
                        case "_varchar":
                            field =
                                    new Field(
                                            columnName,
                                            FieldType.nullable(ArrowType.List.INSTANCE),
                                            Collections.singletonList(
                                                    new Field(
                                                            "item",
                                                            FieldType.notNullable(
                                                                    new ArrowType.Utf8()),
                                                            null)));
                            break;
                        default:
                            throw new IllegalArgumentException(
                                    "Unsupported array element type: " + column.getTypeName());
                    }
                    break;
                case Types.OTHER:
                    if (column.getTypeName().equals("roaringbitmap")) {
                        field =
                                new Field(
                                        columnName,
                                        FieldType.nullable(new ArrowType.Binary()),
                                        null);
                    } else {
                        field =
                                new Field(
                                        columnName, FieldType.nullable(new ArrowType.Utf8()), null);
                    }
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported column type: " + column.getTypeName());
            }
            fields.add(field);
        }
        return new Schema(fields);
    }

    public static AbstractArrowVectorCreator createColumnVectorCreator(
            FieldVector vector, Column column) {
        switch (column.getType()) {
            case Types.INTEGER:
                return new BaseArrowIntCreator((IntVector) vector);
            case Types.BIGINT:
                return new BaseArrowBigIntCreator((BigIntVector) vector);
            case Types.SMALLINT:
                return new BaseArrowSmallIntCreator((SmallIntVector) vector);
            case Types.FLOAT:
            case Types.REAL:
                return new BaseArrowFloat4Creator((Float4Vector) vector);
            case Types.DOUBLE:
                return new BaseArrowFloat8Creator((Float8Vector) vector);
            case Types.DECIMAL:
            case Types.NUMERIC:
                return new BaseArrowDecimalCreator(
                        (DecimalVector) vector, column.getPrecision(), column.getScale());
            case Types.BOOLEAN:
            case Types.BIT:
                return new BaseArrowBitCreator((BitVector) vector);
            case Types.CHAR:
                return new BaseArrowVarCharCreator((VarCharVector) vector, column.getPrecision());
            case Types.VARCHAR:
                return new BaseArrowVarCharCreator((VarCharVector) vector);
            case Types.BINARY:
            case Types.VARBINARY:
                return new BaseArrowVarBinaryCreator((VarBinaryVector) vector);
            case Types.TIMESTAMP:
            case Types.TIME_WITH_TIMEZONE:
                if ("timestamptz".equals(column.getTypeName())) {
                    return new BaseArrowDateMilliCreator((DateMilliVector) vector);
                } else {
                    return new BaseArrowTimeStampMicroCreator((TimeStampMicroVector) vector);
                }
            case Types.TIME:
                if ("timetz".equals(column.getTypeName())) {
                    return new BaseArrowFixedSizeBinaryCreator((FixedSizeBinaryVector) vector);
                } else {
                    return new BaseArrowTimeMicroCreator((TimeMicroVector) vector);
                }
            case Types.DATE:
                return new BaseArrowDateDayCreator((DateDayVector) vector);
            case Types.ARRAY:
                return new BaseArrowArrayCreator((ListVector) vector, column.getArrayElementType());
            case Types.OTHER:
                if ("roaringbitmap".equals(column.getTypeName())) {
                    return new BaseArrowVarBinaryCreator((VarBinaryVector) vector);
                } else {
                    return new BaseArrowVarCharCreator((VarCharVector) vector);
                }
            default:
                throw new IllegalArgumentException(
                        "Unsupported column type: " + column.getTypeName());
        }
    }
}
