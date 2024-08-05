package com.alibaba.ververica.connectors.hologres.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/** Type utils for Postgres. */
public class PostgresTypeUtil {
    public static final String PG_BYTEA = "bytea";
    public static final String PG_BYTEA_ARRAY = "_bytea";
    public static final String PG_SMALLINT = "int2";
    public static final String PG_SMALLINT_ARRAY = "_int2";
    public static final String PG_INTEGER = "int4";
    public static final String PG_INTEGER_ARRAY = "_int4";
    public static final String PG_BIGINT = "int8";
    public static final String PG_BIGINT_ARRAY = "_int8";
    public static final String PG_REAL = "float4";
    public static final String PG_REAL_ARRAY = "_float4";
    public static final String PG_DOUBLE_PRECISION = "float8";
    public static final String PG_DOUBLE_PRECISION_ARRAY = "_float8";
    public static final String PG_NUMERIC = "numeric";
    public static final String PG_NUMERIC_ARRAY = "_numeric";
    public static final String PG_BOOLEAN = "bool";
    public static final String PG_BOOLEAN_ARRAY = "_bool";
    public static final String PG_TIMESTAMP = "timestamp";
    public static final String PG_TIMESTAMP_ARRAY = "_timestamp";
    public static final String PG_TIMESTAMPTZ = "timestamptz";
    public static final String PG_TIMESTAMPTZ_ARRAY = "_timestamptz";
    public static final String PG_DATE = "date";
    public static final String PG_DATE_ARRAY = "_date";
    public static final String PG_TIME = "time";
    public static final String PG_TIME_ARRAY = "_time";
    public static final String PG_TEXT = "text";
    public static final String PG_TEXT_ARRAY = "_text";
    public static final String PG_CHAR = "bpchar";
    public static final String PG_CHAR_ARRAY = "_bpchar";
    public static final String PG_CHARACTER = "character";
    public static final String PG_CHARACTER_ARRAY = "_character";
    public static final String PG_CHARACTER_VARYING = "varchar";
    public static final String PG_CHARACTER_VARYING_ARRAY = "_varchar";
    public static final String PG_GEOGRAPHY = "geography";
    public static final String PG_GEOMETRY = "geometry";
    public static final String PG_BOX2D = "box2d";
    public static final String PG_BOX3D = "box3d";

    public static DataType fromJDBCType(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String pgType = metadata.getColumnTypeName(colIndex);

        switch (pgType) {
            case PG_BOOLEAN:
                return DataTypes.BOOLEAN();
            case PG_BOOLEAN_ARRAY:
                return DataTypes.ARRAY(DataTypes.BOOLEAN());
            case PG_BYTEA:
            case PG_BYTEA_ARRAY:
                return DataTypes.BYTES();
            case PG_SMALLINT:
                return DataTypes.SMALLINT();
            case PG_SMALLINT_ARRAY:
                return DataTypes.ARRAY(DataTypes.SMALLINT());
            case PG_INTEGER:
                return DataTypes.INT();
            case PG_INTEGER_ARRAY:
                return DataTypes.ARRAY(DataTypes.INT());
            case PG_BIGINT:
                return DataTypes.BIGINT();
            case PG_BIGINT_ARRAY:
                return DataTypes.ARRAY(DataTypes.BIGINT());
            case PG_REAL:
                return DataTypes.FLOAT();
            case PG_REAL_ARRAY:
                return DataTypes.ARRAY(DataTypes.FLOAT());
            case PG_DOUBLE_PRECISION:
                return DataTypes.DOUBLE();
            case PG_DOUBLE_PRECISION_ARRAY:
                return DataTypes.ARRAY(DataTypes.DOUBLE());
            case PG_NUMERIC:
                return DataTypes.DECIMAL(
                        metadata.getPrecision(colIndex), metadata.getScale(colIndex));
            case PG_NUMERIC_ARRAY:
                return DataTypes.ARRAY(
                        DataTypes.DECIMAL(
                                metadata.getPrecision(colIndex), metadata.getScale(colIndex)));
            case PG_CHAR:
            case PG_CHARACTER:
                return DataTypes.CHAR(1);
            case PG_CHAR_ARRAY:
            case PG_CHARACTER_ARRAY:
                return DataTypes.ARRAY(DataTypes.CHAR(1));
            case PG_TEXT:
            case PG_CHARACTER_VARYING:
                return DataTypes.STRING();
            case PG_TEXT_ARRAY:
            case PG_CHARACTER_VARYING_ARRAY:
                return DataTypes.ARRAY(DataTypes.STRING());
            case PG_TIMESTAMP:
                return DataTypes.TIMESTAMP();
            case PG_TIMESTAMP_ARRAY:
                return DataTypes.ARRAY(DataTypes.TIMESTAMP());
                // TODO: add timestamptz for Holo
            case PG_TIMESTAMPTZ:
                return DataTypes.TIMESTAMP();
            case PG_TIMESTAMPTZ_ARRAY:
                return DataTypes.ARRAY(DataTypes.TIMESTAMP());
            case PG_TIME:
                return DataTypes.TIME();
            case PG_TIME_ARRAY:
                return DataTypes.ARRAY(DataTypes.TIME());
            case PG_DATE:
                return DataTypes.DATE();
            case PG_DATE_ARRAY:
                return DataTypes.ARRAY(DataTypes.DATE());
            default:
                throw new UnsupportedOperationException(
                        String.format("Doesn't support Postgres type '%s' yet", pgType));
        }
    }
}
