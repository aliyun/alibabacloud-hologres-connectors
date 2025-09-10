/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler.jdbc;

import org.postgresql.jdbc.TimestampUtils;

import java.sql.Clob;
import java.sql.SQLException;

/** double列存类. */
public class JdbcDoubleColumnValues extends JdbcColumnValues {

    Double[] array;

    public JdbcDoubleColumnValues(TimestampUtils timestampUtils, int rowCount) {
        super(timestampUtils, rowCount);
        array = new Double[rowCount];
    }

    @Override
    public void doSet(int row, Object obj) throws SQLException {
        array[row] = castToDouble(obj);
    }

    private static double castToDouble(final Object in) throws SQLException {
        try {
            if (in instanceof String) {
                return Double.parseDouble((String) in);
            }
            if (in instanceof Number) {
                return ((Number) in).doubleValue();
            }
            if (in instanceof java.util.Date) {
                return ((java.util.Date) in).getTime();
            }
            if (in instanceof Boolean) {
                return (Boolean) in ? 1d : 0d;
            }
            if (in instanceof Clob) {
                return Double.parseDouble(asString((Clob) in));
            }
            if (in instanceof Character) {
                return Double.parseDouble(in.toString());
            }
        } catch (final Exception e) {
            throw cannotCastException(in.getClass().getName(), "double", e);
        }
        throw cannotCastException(in.getClass().getName(), "double");
    }

    @Override
    public Object[] getArray() {
        return array;
    }
}
