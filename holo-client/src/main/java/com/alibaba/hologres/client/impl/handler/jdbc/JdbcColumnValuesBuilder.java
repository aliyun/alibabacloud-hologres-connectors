/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler.jdbc;

import com.alibaba.hologres.client.HoloConfig;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.TimestampUtils;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.SQLException;
import java.sql.Types;

/**
 * 列存对象构建.
 */
public class JdbcColumnValuesBuilder {

	public static JdbcColumnValues build(PgConnection connection, int rowCount, int targetSqlType, String typeName, HoloConfig config) throws SQLException {
		TimestampUtils timestampUtils = connection.getTimestampUtils();
		switch (targetSqlType) {
			case Types.INTEGER:
				return new JdbcIntegerColumnValues(timestampUtils, rowCount);
			case Types.TINYINT:
			case Types.SMALLINT:
				return new JdbcShortColumnValues(timestampUtils, rowCount);
			case Types.BIGINT:
				return new JdbcLongColumnValues(timestampUtils, rowCount);
			case Types.REAL:
				return new JdbcFloatColumnValues(timestampUtils, rowCount);
			case Types.DOUBLE:
			case Types.FLOAT:
				return new JdbcDoubleColumnValues(timestampUtils, rowCount);
			case Types.DECIMAL:
			case Types.NUMERIC:
				return new JdbcBigDecimalColumnValues(timestampUtils, rowCount);
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
				return new JdbcStringColumnValues(timestampUtils, rowCount);
			case Types.DATE:
				return new JdbcDateColumnValues(timestampUtils, rowCount, config);
			case Types.TIME:
				return new JdbcTimeColumnValues(timestampUtils, rowCount);
			case Types.TIMESTAMP:
			case Types.TIMESTAMP_WITH_TIMEZONE:
				return new JdbcTimestampColumnValues(timestampUtils, rowCount, config);
			case Types.BOOLEAN:
			case Types.BIT:
				return new JdbcBooleanColumnValues(timestampUtils, rowCount);
			case Types.BINARY:
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
				return new JdbcByteaColumnValues(timestampUtils, rowCount);
			case Types.OTHER:
				if ("json".equals(typeName) || "jsonb".equals(typeName)) {
					return new JdbcStringColumnValues(timestampUtils, rowCount);
				}
				if ("roaringbitmap".equals(typeName)) {
					return new JdbcByteaColumnValues(timestampUtils, rowCount);
				} else {
					throw new PSQLException(GT.tr("Unsupported Types value: {0}", targetSqlType),
							PSQLState.INVALID_PARAMETER_TYPE);
				}
			default:
				throw new PSQLException(GT.tr("Unsupported Types value: {0}", targetSqlType),
						PSQLState.INVALID_PARAMETER_TYPE);
		}

	}
}
