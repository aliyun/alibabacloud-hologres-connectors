/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler.jdbc;

import org.postgresql.core.Oid;
import org.postgresql.jdbc.TimestampUtils;
import org.postgresql.util.PGTime;

import java.sql.SQLException;
import java.sql.Time;

/**
 * Time列存类.
 */
public class JdbcTimeColumnValues extends JdbcColumnValues {

	String[] array;

	public JdbcTimeColumnValues(TimestampUtils timestampUtils, int rowCount) {
		super(timestampUtils, rowCount);
		array = new String[rowCount];
	}

	@Override
	public void doSet(int row, Object in) throws SQLException {
		if (in instanceof java.sql.Time) {
			setTime(row, (java.sql.Time) in);
		} else {
			java.sql.Time tmpt;
			if (in instanceof java.util.Date) {
				tmpt = new java.sql.Time(((java.util.Date) in).getTime());
				//#if mvn.project.property.postgresql.jdbc.spec >= "JDBC4.2"
			} else if (in instanceof java.time.LocalTime) {
				setTime(row, (java.time.LocalTime) in);
				return;
				//#endif
			} else {
				tmpt = timestampUtils.toTime(getDefaultCalendar(), in.toString());
			}
			setTime(row, tmpt);
		}
	}

	private void setTime(int row, java.time.LocalTime localTime) throws SQLException {
		array[row] = timestampUtils.toString(localTime);
	}

	public void setTime(int row, Time x) throws SQLException {
		setTime(row, x, null);
	}

	public void setTime(int row, Time t,
						java.util.Calendar cal) throws SQLException {
		if (t == null) {
			return;
		}
		int oid = Oid.UNSPECIFIED;

		// If a PGTime is used, we can define the OID explicitly.
		if (t instanceof PGTime) {
			PGTime pgTime = (PGTime) t;
			if (pgTime.getCalendar() == null) {
				oid = Oid.TIME;
			} else {
				oid = Oid.TIMETZ;
				cal = pgTime.getCalendar();
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
