/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler.jdbc;

import org.postgresql.jdbc.TimestampUtils;

import java.sql.Clob;
import java.sql.SQLException;

/** long列存类. */
public class JdbcLongColumnValues extends JdbcColumnValues {

    Long[] array;

    public JdbcLongColumnValues(TimestampUtils timestampUtils, int rowCount) {
        super(timestampUtils, rowCount);
        array = new Long[rowCount];
    }

    @Override
    public void doSet(int row, Object obj) throws SQLException {
        array[row] = castToLong(obj);
    }

    private static long castToLong(final Object in) throws SQLException {
        try {
            if (in instanceof String) {
                return Long.parseLong((String) in);
            }
            if (in instanceof Number) {
                return ((Number) in).longValue();
            }
            if (in instanceof java.util.Date) {
                return ((java.util.Date) in).getTime();
            }
            if (in instanceof Boolean) {
                return (Boolean) in ? 1L : 0L;
            }
            if (in instanceof Clob) {
                return Long.parseLong(asString((Clob) in));
            }
            if (in instanceof Character) {
                return Long.parseLong(in.toString());
            }
        } catch (final Exception e) {
            throw cannotCastException(in.getClass().getName(), "long", e);
        }
        throw cannotCastException(in.getClass().getName(), "long");
    }

    @Override
    public Object[] getArray() {
        return array;
    }
}
