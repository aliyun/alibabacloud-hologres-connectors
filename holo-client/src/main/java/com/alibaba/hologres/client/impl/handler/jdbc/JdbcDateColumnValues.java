/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler.jdbc;

import com.alibaba.hologres.client.HoloConfig;
import org.postgresql.jdbc.TimestampUtils;

import java.sql.Date;
import java.sql.SQLException;

/** date 列存类. */
public class JdbcDateColumnValues extends JdbcColumnValues {

    String[] array;
    HoloConfig config;

    public JdbcDateColumnValues(TimestampUtils timestampUtils, int rowCount, HoloConfig config) {
        super(timestampUtils, rowCount);
        array = new String[rowCount];
        this.config = config;
    }

    @Override
    public void doSet(int row, Object in) throws SQLException {
        if (in instanceof String && config.isInputStringAsEpochMsForDatetimeColumn()) {
            Long value = null;
            try {
                value = Long.parseLong(String.valueOf(in));
            } catch (Exception ignore) {
                value = null;
            }
            if (value != null) {
                setDate(row, new java.sql.Date(value));
                return;
            }
        }
        if (in instanceof Number && config.isInputNumberAsEpochMsForDatetimeColumn()) {
            setDate(row, new java.sql.Date(((Number) in).longValue()));
        } else if (in instanceof java.sql.Date) {
            setDate(row, (java.sql.Date) in);
        } else {
            java.sql.Date tmpd;
            if (in instanceof java.util.Date) {
                tmpd = new java.sql.Date(((java.util.Date) in).getTime());
                // #if mvn.project.property.postgresql.jdbc.spec >= "JDBC4.2"
            } else if (in instanceof java.time.LocalDate) {
                setDate(row, (java.time.LocalDate) in);
                return;
                // #endif
            } else {
                tmpd = timestampUtils.toDate(getDefaultCalendar(), in.toString());
            }
            setDate(row, tmpd);
        }
    }

    private void setDate(int i, java.time.LocalDate localDate) throws SQLException {
        array[i] = timestampUtils.toString(localDate);
    }

    public void setDate(int row, Date x) throws SQLException {
        setDate(row, x, null);
    }

    public void setDate(int i, java.sql.Date d, java.util.Calendar cal) throws SQLException {

        if (d == null) {
            return;
        }
        if (cal == null) {
            cal = getDefaultCalendar();
        }
        array[i] = timestampUtils.toString(cal, d);
    }

    @Override
    public Object[] getArray() {
        return array;
    }
}
