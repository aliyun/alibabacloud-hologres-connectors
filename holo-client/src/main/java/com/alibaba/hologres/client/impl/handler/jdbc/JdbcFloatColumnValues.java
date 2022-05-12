/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler.jdbc;

import org.postgresql.jdbc.TimestampUtils;

import java.sql.Clob;
import java.sql.SQLException;

/**
 * float列存类.
 */
public class JdbcFloatColumnValues extends JdbcColumnValues {

	Float[] array;

	public JdbcFloatColumnValues(TimestampUtils timestampUtils, int rowCount) {
		super(timestampUtils, rowCount);
		array = new Float[rowCount];
	}

	@Override
	public void doSet(int row, Object obj) throws SQLException {
		array[row] = castToFloat(obj);
	}

	private static float castToFloat(final Object in) throws SQLException {
		try {
			if (in instanceof String) {
				return Float.parseFloat((String) in);
			}
			if (in instanceof Number) {
				return ((Number) in).floatValue();
			}
			if (in instanceof java.util.Date) {
				return ((java.util.Date) in).getTime();
			}
			if (in instanceof Boolean) {
				return (Boolean) in ? 1f : 0f;
			}
			if (in instanceof Clob) {
				return Float.parseFloat(asString((Clob) in));
			}
			if (in instanceof Character) {
				return Float.parseFloat(in.toString());
			}
		} catch (final Exception e) {
			throw cannotCastException(in.getClass().getName(), "float", e);
		}
		throw cannotCastException(in.getClass().getName(), "float");
	}

	@Override
	public Object[] getArray() {
		return array;
	}
}
