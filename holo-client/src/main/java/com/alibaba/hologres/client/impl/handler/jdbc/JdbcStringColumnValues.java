/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler.jdbc;

import com.alibaba.hologres.client.HoloConfig;
import org.postgresql.jdbc.TimestampUtils;

import java.sql.Clob;
import java.sql.SQLException;

/** String 列存类. */
public class JdbcStringColumnValues extends JdbcColumnValues {

    private final HoloConfig config;
    String[] array;

    public JdbcStringColumnValues(TimestampUtils timestampUtils, int rowCount, HoloConfig config) {
        super(timestampUtils, rowCount);
        array = new String[rowCount];
        this.config = config;
    }

    @Override
    public void doSet(int row, Object obj) throws SQLException {
        array[row] = removeU0000(castToString(obj));
    }

    private String removeU0000(final String in) {
        if (config.isRemoveU0000InTextColumnValue() && in != null && in.contains("\u0000")) {
            return in.replaceAll("\u0000", "");
        } else {
            return in;
        }
    }

    private static String castToString(final Object in) throws SQLException {
        try {
            if (in instanceof String) {
                return (String) in;
            }
            if (in instanceof Clob) {
                return asString((Clob) in);
            }
            // convert any unknown objects to string.
            return in.toString();

        } catch (final Exception e) {
            throw cannotCastException(in.getClass().getName(), "String", e);
        }
    }

    @Override
    public Object[] getArray() {
        return array;
    }
}
