package com.alibaba.hologres.client.impl.util;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.impl.handler.jdbc.JdbcColumnValues;
import com.alibaba.hologres.client.impl.handler.jdbc.JdbcColumnValuesBuilder;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.TableSchema;
import org.postgresql.jdbc.PgConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.BitSet;
import java.util.PrimitiveIterator;
import java.util.stream.IntStream;

/** 构造statement的工具类. */
public class StatementBuilderUtil {
    public static void prepareColumnValues(
            Connection conn,
            int rows,
            BitSet columnBitSet,
            TableSchema schema,
            HoloConfig config,
            JdbcColumnValues[] arrayList)
            throws SQLException {
        int arrayIndex = -1;
        IntStream columnStream = columnBitSet.stream();
        for (PrimitiveIterator.OfInt it = columnStream.iterator(); it.hasNext(); ) {
            int index = it.next();
            Column column = schema.getColumn(index);
            ++arrayIndex;
            arrayList[arrayIndex] =
                    JdbcColumnValuesBuilder.build(
                            conn.unwrap(PgConnection.class),
                            rows,
                            column.getType(),
                            column.getTypeName(),
                            config);
        }
    }

    public static boolean isTypeSupportForUnnest(int type, String typeName) {
        switch (type) {
            case Types.BOOLEAN:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
            case Types.DATE:
            case Types.TIME_WITH_TIMEZONE:
            case Types.NUMERIC:
                return true;
            case Types.BIT:
                if ("bool".equals(typeName)) {
                    return true;
                }
                return false;
            case Types.BIGINT:
                if ("oid".equals(typeName)) {
                    return false;
                }
                return true;
            case Types.OTHER:
                if ("json".equals(typeName) || "jsonb".equals(typeName)) {
                    return true;
                }
                return false;
            default:
                return false;
        }
    }

    public static String getRealTypeName(int type, String typeName) {
        String ret = null;
        switch (type) {
            case Types.INTEGER:
                if ("serial".equals(typeName)) {
                    ret = "int"; // 避免出现serial
                } else {
                    ret = typeName;
                }
                break;
            case Types.BIGINT:
                if ("bigserial".equals(typeName)) {
                    ret = "bigint"; // 避免出现bigserial
                } else {
                    ret = typeName;
                }
                break;
            default:
                ret = typeName;
        }
        return ret;
    }
}
