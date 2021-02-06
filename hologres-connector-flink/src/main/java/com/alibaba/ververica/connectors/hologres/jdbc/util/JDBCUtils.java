package com.alibaba.ververica.connectors.hologres.jdbc.util;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.types.logical.RowType;

import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresJdbcConfigs;
import com.alibaba.ververica.connectors.hologres.jdbc.config.JDBCOptions;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.alibaba.ververica.connectors.hologres.jdbc.config.HologresJdbcConfigs.ARRAY_DELIMITER;
import static com.alibaba.ververica.connectors.hologres.jdbc.config.HologresJdbcConfigs.FIELD_DELIMITER;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * JDBCUtils.
 */
public class JDBCUtils {
	private static Map<JDBCOptions, Connection> connections = new ConcurrentHashMap<>();

	public static String getDbUrl(String holoFrontend, String db) {
		return "jdbc:postgresql://" + holoFrontend + "/" + db;
	}

	public static String getSimpleSelectFromStatement(String table, String[] selectFields) {
		String selectExpressions = Arrays.stream(selectFields)
				.map(e -> quoteIdentifier(e.toLowerCase()))
				.collect(Collectors.joining(", "));

		return "SELECT " + selectExpressions + " FROM " + table;
	}

	public static String quoteIdentifier(String identifier) {
		return "\"" + identifier + "\"";
	}

	public static Connection createConnection(JDBCOptions options) {
		try {
			return DriverManager.getConnection(options.getDbUrl(), options.getUsername(), options.getPassword());
		} catch (SQLException e) {
			throw new RuntimeException(String.format("Failed getting connection to %s because %s", options.getDbUrl(), ExceptionUtils.getStackTrace(e)));
		}
	}

	public static RowType resolveTableRowType(
			String database,
			String table,
			String holoFrontend,
			String username,
			String pwd
	) {
		try (Connection conn = DriverManager.getConnection(getDbUrl(holoFrontend, database), username, pwd)) {
			PreparedStatement ps = conn.prepareStatement(String.format("SELECT * FROM %s", table));

			ResultSet rs = ps.executeQuery();

			ResultSetMetaData rsmd = rs.getMetaData();
			int numCols = rsmd.getColumnCount();

			List<RowType.RowField> fields = new ArrayList<>(rsmd.getColumnCount());
			for (int i = 1; i <= numCols; i++) {
				fields.add(new RowType.RowField(rsmd.getColumnName(i), PostgresTypeUtil.fromJDBCType(rsmd, i).getLogicalType()));
			}

			return new RowType(fields);
		} catch (SQLException e) {
			throw new RuntimeException(
					String.format("Failed resolving data types for table %s", table), e);
		}
	}

	public static JDBCOptions getJDBCOptions(ReadableConfig properties) {
		// required property
		String frontend = properties.get(HologresJdbcConfigs.ENDPOINT);
		String db = properties.get(HologresJdbcConfigs.DATABASE);
		String table = properties.get(HologresJdbcConfigs.TABLE);
		String username = properties.get(HologresJdbcConfigs.USERNAME);
		String pwd = properties.get(HologresJdbcConfigs.PASSWORD);
		String delimiter = properties.getOptional(ARRAY_DELIMITER).isPresent() ? properties.get(ARRAY_DELIMITER) : properties.get(FIELD_DELIMITER);

		validateEndpoint(frontend);
		return new JDBCOptions(db, table, username, pwd, frontend, delimiter);
	}

	public static void validateEndpoint(String endpoint) {
		String[] parts = endpoint.trim().split(":");
		checkArgument(parts.length == 2);
	}
}
