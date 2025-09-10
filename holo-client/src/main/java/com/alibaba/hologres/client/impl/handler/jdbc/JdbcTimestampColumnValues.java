/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler.jdbc;

import com.alibaba.hologres.client.HoloConfig;
import org.postgresql.jdbc.TimestampUtils;
import org.postgresql.util.PGTimestamp;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;

/** timestamp列存类. */
public class JdbcTimestampColumnValues extends JdbcColumnValues {

    String[] array;
    HoloConfig config;

    public JdbcTimestampColumnValues(
            TimestampUtils timestampUtils, int rowCount, HoloConfig config) {
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
                setTimestamp(row, new java.sql.Timestamp(value));
                return;
            }
        }
        if (in instanceof Number && config.isInputNumberAsEpochMsForDatetimeColumn()) {
            setTimestamp(row, new java.sql.Timestamp(((Number) in).longValue()));
        } else if (in instanceof String && "0000-00-00 00:00:00".equals(in)) {
            setTimestamp(row, new java.sql.Timestamp(0L));
        } else if (in instanceof java.sql.Timestamp) {
            setTimestamp(row, (java.sql.Timestamp) in);
        } else {
            java.sql.Timestamp tmpts;
            if (in instanceof java.util.Date) {
                tmpts = new java.sql.Timestamp(((java.util.Date) in).getTime());
                // #if mvn.project.property.postgresql.jdbc.spec >= "JDBC4.2"
            } else if (in instanceof java.time.LocalDateTime) {
                setTimestamp(row, (java.time.LocalDateTime) in);
                return;
                // #endif
            } else if (in instanceof OffsetDateTime) {

                return;
            } else {
                tmpts = timestampUtils.toTimestamp(getDefaultCalendar(), in.toString());
            }
            setTimestamp(row, tmpts);
        }
    }

    private void setTimestamp(int row, java.time.OffsetDateTime offsetDateTime)
            throws SQLException {
        array[row] = timestampUtils.toString(offsetDateTime);
    }

    private void setTimestamp(int row, java.time.LocalDateTime localDateTime) throws SQLException {
        array[row] = timestampUtils.toString(localDateTime);
    }

    public void setTimestamp(int row, Timestamp x) throws SQLException {
        setTimestamp(row, x, null);
    }

    public void setTimestamp(int row, Timestamp t, java.util.Calendar cal) throws SQLException {
        if (t == null) {
            return;
        }

        if (t instanceof PGTimestamp) {
            PGTimestamp pgTimestamp = (PGTimestamp) t;
            if (pgTimestamp.getCalendar() != null) {
                cal = pgTimestamp.getCalendar();
            }
        }
        if (cal == null) {
            cal = getDefaultCalendar();
        }
        array[row] = timestampUtils.toString(cal, t);
    }

    @Override
    public Object[] getArray() {
        return array;
    }
}
