/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler.jdbc;

import org.postgresql.jdbc.TimestampUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.Clob;
import java.sql.SQLException;

/**
 * numeric列存类.
 */
public class JdbcBigDecimalColumnValues extends JdbcColumnValues {

	String[] array;

	public JdbcBigDecimalColumnValues(TimestampUtils timestampUtils, int rowCount) {
		super(timestampUtils, rowCount);
		array = new String[rowCount];
	}

	@Override
	public void doSet(int row, Object obj) throws SQLException {
		array[row] = castToBigDecimal(obj, -1).toString();
	}

	private static BigDecimal castToBigDecimal(final Object in, final int scale) throws SQLException {
		try {
			BigDecimal rc = null;
			if (in instanceof String) {
				rc = new BigDecimal((String) in);
			} else if (in instanceof BigDecimal) {
				rc = ((BigDecimal) in);
			} else if (in instanceof BigInteger) {
				rc = new BigDecimal((BigInteger) in);
			} else if (in instanceof Long || in instanceof Integer || in instanceof Short
					|| in instanceof Byte) {
				rc = BigDecimal.valueOf(((Number) in).longValue());
			} else if (in instanceof Double || in instanceof Float) {
				rc = BigDecimal.valueOf(((Number) in).doubleValue());
			} else if (in instanceof java.util.Date) {
				rc = BigDecimal.valueOf(((java.util.Date) in).getTime());
			} else if (in instanceof Boolean) {
				rc = (Boolean) in ? BigDecimal.ONE : BigDecimal.ZERO;
			} else if (in instanceof Clob) {
				rc = new BigDecimal(asString((Clob) in));
			} else if (in instanceof Character) {
				rc = new BigDecimal(new char[]{(Character) in});
			}
			if (rc != null) {
				if (scale >= 0) {
					rc = rc.setScale(scale, RoundingMode.HALF_UP);
				}
				return rc;
			}
		} catch (final Exception e) {
			throw cannotCastException(in.getClass().getName(), "BigDecimal", e);
		}
		throw cannotCastException(in.getClass().getName(), "BigDecimal");
	}

	@Override
	public Object[] getArray() {
		return array;
	}
}
