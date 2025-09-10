/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler.jdbc;

import org.postgresql.jdbc.TimestampUtils;

import java.sql.Clob;
import java.sql.SQLException;

/** Int 列存类. */
public class JdbcIntegerColumnValues extends JdbcColumnValues {

    Integer[] array;

    public JdbcIntegerColumnValues(TimestampUtils timestampUtils, int rowCount) {
        super(timestampUtils, rowCount);
        array = new Integer[rowCount];
    }

    @Override
    public void doSet(int row, Object obj) throws SQLException {
        array[row] = castToInt(obj);
    }

    private static int castToInt(final Object in) throws SQLException {
        try {
            if (in instanceof String) {
                return Integer.parseInt((String) in);
            }
            if (in instanceof Number) {
                return ((Number) in).intValue();
            }
            if (in instanceof java.util.Date) {
                return (int) ((java.util.Date) in).getTime();
            }
            if (in instanceof Boolean) {
                return (Boolean) in ? 1 : 0;
            }
            if (in instanceof Clob) {
                return Integer.parseInt(asString((Clob) in));
            }
            if (in instanceof Character) {
                return Integer.parseInt(in.toString());
            }
        } catch (final Exception e) {
            throw cannotCastException(in.getClass().getName(), "int", e);
        }
        throw cannotCastException(in.getClass().getName(), "int");
    }

    @Override
    public Object[] getArray() {
        return array;
    }
}
