/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler.jdbc;

import org.postgresql.jdbc.TimestampUtils;

import java.sql.Clob;
import java.sql.SQLException;

/**
 * SHort列存类.
 */
public class JdbcShortColumnValues extends JdbcColumnValues {

	Short[] array;

	public JdbcShortColumnValues(TimestampUtils timestampUtils, int rowCount) {
		super(timestampUtils, rowCount);
		array = new Short[rowCount];
	}

	@Override
	public void doSet(int row, Object obj) throws SQLException {
		array[row] = castToShort(obj);
	}

	private static short castToShort(final Object in) throws SQLException {
		try {
			if (in instanceof String) {
				return Short.parseShort((String) in);
			}
			if (in instanceof Number) {
				return ((Number) in).shortValue();
			}
			if (in instanceof java.util.Date) {
				return (short) ((java.util.Date) in).getTime();
			}
			if (in instanceof Boolean) {
				return (Boolean) in ? (short) 1 : (short) 0;
			}
			if (in instanceof Clob) {
				return Short.parseShort(asString((Clob) in));
			}
			if (in instanceof Character) {
				return Short.parseShort(in.toString());
			}
		} catch (final Exception e) {
			throw cannotCastException(in.getClass().getName(), "short", e);
		}
		throw cannotCastException(in.getClass().getName(), "short");
	}

	@Override
	public Object[] getArray() {
		return array;
	}
}
