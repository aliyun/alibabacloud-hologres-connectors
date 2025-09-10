/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler.jdbc;

import org.postgresql.jdbc.TimestampUtils;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.Clob;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.TimeZone;

/** 列存类. */
public abstract class JdbcColumnValues {

    protected int rowCount;
    protected TimestampUtils timestampUtils;
    protected TimeZone defaultTimeZone;

    public JdbcColumnValues(TimestampUtils timestampUtils, int rowCount) {
        this.timestampUtils = timestampUtils;
        this.rowCount = rowCount;
    }

    public void set(int row, Object obj) throws SQLException {
        if (null != obj) {
            doSet(row, obj);
        }
    }

    protected abstract void doSet(int row, Object obj) throws SQLException;

    public abstract Object[] getArray();

    protected static String asString(final Clob in) throws SQLException {
        return in.getSubString(1, (int) in.length());
    }

    protected Calendar getDefaultCalendar() {
        if (timestampUtils.hasFastDefaultTimeZone()) {
            return timestampUtils.getSharedCalendar(null);
        }
        Calendar sharedCalendar = timestampUtils.getSharedCalendar(defaultTimeZone);
        if (defaultTimeZone == null) {
            defaultTimeZone = sharedCalendar.getTimeZone();
        }
        return sharedCalendar;
    }

    protected static PSQLException cannotCoerceException(final Object value) {
        return new PSQLException(
                GT.tr("Cannot cast to boolean: \"{0}\"", String.valueOf(value)),
                PSQLState.CANNOT_COERCE);
    }

    protected static PSQLException cannotCastException(final String fromType, final String toType) {
        return cannotCastException(fromType, toType, null);
    }

    protected static PSQLException cannotCastException(
            final String fromType, final String toType, final Exception cause) {
        return new PSQLException(
                GT.tr("Cannot convert an instance of {0} to type {1}", fromType, toType),
                PSQLState.INVALID_PARAMETER_TYPE,
                cause);
    }
}
