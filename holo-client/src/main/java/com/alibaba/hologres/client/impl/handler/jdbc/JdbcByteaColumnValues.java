/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler.jdbc;

import org.postgresql.jdbc.TimestampUtils;

import java.nio.charset.Charset;
import java.sql.SQLException;

/** bytea 列存类. */
public class JdbcByteaColumnValues extends JdbcColumnValues {
    private static final Charset UTF8 = Charset.forName("utf-8");

    byte[][] array;

    public JdbcByteaColumnValues(TimestampUtils timestampUtils, int rowCount) {
        super(timestampUtils, rowCount);
        array = new byte[rowCount][];
    }

    @Override
    public void doSet(int row, Object obj) throws SQLException {
        array[row] = castToBytes(obj);
    }

    private static byte[] castToBytes(final Object in) throws SQLException {
        try {
            if (in instanceof byte[]) {
                return (byte[]) in;
            }
            return in.toString().getBytes(UTF8);
        } catch (final Exception e) {
            throw cannotCastException(in.getClass().getName(), "byte[]", e);
        }
    }

    @Override
    public Object[] getArray() {
        return array;
    }
}
