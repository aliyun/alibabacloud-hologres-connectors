package com.alibaba.ververica.connectors.hologres.jdbc.catalog;

import org.apache.flink.util.StringUtils;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Table path of PostgreSQL in Flink. Can be of formats "table_name" or "schema_name.table_name".
 * When it's "table_name", the schema name defaults to "public".
 */
public class PostgresTablePath {

	private static final String DEFAULT_POSTGRES_SCHEMA_NAME = "public";

	private final String pgSchemaName;
	private final String pgTableName;

	public PostgresTablePath(String pgSchemaName, String pgTableName) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(pgSchemaName));
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(pgTableName));

		this.pgSchemaName = pgSchemaName;
		this.pgTableName = pgTableName;
	}

	public static PostgresTablePath fromFlinkTableName(String flinkTableName) {
		if (flinkTableName.contains(".")) {
			String[] path = flinkTableName.split("\\.");
			checkArgument(
					path.length == 2,
					String.format(
							"Table name '%s' is not valid. The parsed length is %d",
							flinkTableName, path.length));
			return new PostgresTablePath(path[0], path[1]);
		} else {
			return new PostgresTablePath(DEFAULT_POSTGRES_SCHEMA_NAME, flinkTableName);
		}
	}

	public static String toFlinkTableName(String schema, String table) {
		return new PostgresTablePath(schema, table).getFullPath();
	}

	public String getFullPath() {
		return String.format("%s.%s", pgSchemaName, pgTableName);
	}

	public String getPgTableName() {
		return pgTableName;
	}

	public String getPgSchemaName() {
		return pgSchemaName;
	}

	@Override
	public String toString() {
		return getFullPath();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		PostgresTablePath that = (PostgresTablePath) o;
		return Objects.equals(pgSchemaName, that.pgSchemaName)
				&& Objects.equals(pgTableName, that.pgTableName);
	}

	@Override
	public int hashCode() {
		return Objects.hash(pgSchemaName, pgTableName);
	}
}

