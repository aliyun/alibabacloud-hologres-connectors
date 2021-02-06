package com.alibaba.ververica.connectors.hologres.jdbc.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.util.StringUtils;

import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresJdbcConfigs;
import com.alibaba.ververica.connectors.hologres.jdbc.factory.HologresJdbcTableFactory;
import com.alibaba.ververica.connectors.hologres.jdbc.util.JDBCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.alibaba.ververica.connectors.hologres.jdbc.config.HologresJdbcConfigs.DATABASE;
import static com.alibaba.ververica.connectors.hologres.jdbc.config.HologresJdbcConfigs.ENDPOINT;
import static com.alibaba.ververica.connectors.hologres.jdbc.config.HologresJdbcConfigs.PASSWORD;
import static com.alibaba.ververica.connectors.hologres.jdbc.config.HologresJdbcConfigs.TABLE;
import static com.alibaba.ververica.connectors.hologres.jdbc.config.HologresJdbcConfigs.USERNAME;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Catalogs for Hologres. */
public class HologresCatalog extends AbstractCatalog {
	private static final Logger LOG = LoggerFactory.getLogger(HologresCatalog.class);

	public static final String DEFAULT_DATABASE = "postgres";
	private final String username;
	private final String password;
	private final String endpoint;
	private final String defaultUrl;

	// ------ Postgres default objects that shouldn't be exposed to users ------

	private static final Set<String> builtinDatabases =
			new HashSet<String>() {
				{
					add("template0");
					add("template1");
				}
			};

	private static final Set<String> builtinSchemas =
			new HashSet<String>() {
				{
					add("pg_toast");
					add("pg_temp_1");
					add("pg_toast_temp_1");
					add("pg_catalog");
					add("information_schema");
					add("hologres");
				}
			};

	public HologresCatalog(
			String catalogName,
			String defaultDatabase,
			String username,
			String password,
			String endpoint) {
		super(catalogName, defaultDatabase);

		checkArgument(!StringUtils.isNullOrWhitespaceOnly(username));
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(password));
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(endpoint));

		JDBCUtils.validateEndpoint(endpoint);

		this.username = username;
		this.password = password;
		this.endpoint = endpoint;
		this.defaultUrl = JDBCUtils.getDbUrl(endpoint, defaultDatabase);
	}

	@Override
	public void open() throws CatalogException {
		// test connection, fail early if we cannot connect to database
		try (Connection conn = DriverManager.getConnection(defaultUrl, username, password)) {
		} catch (SQLException e) {
			throw new ValidationException(
					String.format("Failed connecting to default database %s.", getDefaultDatabase()), e);
		}

		LOG.info("Catalog {} established connection to Hologres", getName());
	}

	@Override
	public void close() throws CatalogException {
		LOG.info("Catalog {} closing", getName());
	}

	@Override
	public List<String> listDatabases() throws CatalogException {
		List<String> pgDatabases = new ArrayList<>();
		try (Connection conn = DriverManager.getConnection(defaultUrl, username, password)) {
			PreparedStatement ps = conn.prepareStatement("SELECT datname FROM pg_database;");
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				String dbName = rs.getString(1);
				if (!builtinDatabases.contains(dbName)) {
					pgDatabases.add(rs.getString(1));
				}
			}
			return pgDatabases;
		} catch (Exception e) {
			throw new CatalogException(
					String.format("Failed listing database in catalog %s", getName()), e);
		}
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
		if (listDatabases().contains(databaseName)) {
			return new CatalogDatabaseImpl(Collections.emptyMap(), null);
		} else {
			throw new DatabaseNotExistException(getName(), databaseName);
		}
	}

	@Override
	public boolean databaseExists(String databaseName) throws CatalogException {
		return listDatabases().contains(databaseName);
	}

	@Override
	public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
		if (!databaseExists(databaseName)) {
			throw new DatabaseNotExistException(getName(), databaseName);
		}
		// get all schemas
		try (Connection conn = DriverManager.getConnection(JDBCUtils.getDbUrl(endpoint, databaseName), username, password)) {
			PreparedStatement ps =
					conn.prepareStatement("SELECT schema_name FROM information_schema.schemata;");
			ResultSet rs = ps.executeQuery();
			List<String> schemas = new ArrayList<>();
			while (rs.next()) {
				String pgSchema = rs.getString(1);
				if (!builtinSchemas.contains(pgSchema)) {
					schemas.add(pgSchema);
				}
			}
			List<String> tables = new ArrayList<>();
			for (String schema : schemas) {
				PreparedStatement stmt =
						conn.prepareStatement(
								"SELECT * \n"
										+ "FROM information_schema.tables \n"
										+ "WHERE table_type = 'BASE TABLE' \n"
										+ "    AND table_schema = ? \n"
										+ "ORDER BY table_type, table_name;");
				stmt.setString(1, schema);
				ResultSet results = stmt.executeQuery();
				while (results.next()) {
					// position 1 is database name, position 2 is schema name, position 3 is table
					// name
					tables.add(schema + "." + results.getString(3));
				}
			}
			return tables;
		} catch (Exception e) {
			throw new CatalogException(
					String.format("Failed listing database in catalog %s", getName()), e);
		}
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		if (!tableExists(tablePath)) {
			throw new TableNotExistException(getName(), tablePath);
		}

		PostgresTablePath pgPath = PostgresTablePath.fromFlinkTableName(tablePath.getObjectName());

		String dbUrl = JDBCUtils.getDbUrl(endpoint, tablePath.getDatabaseName());
		try (Connection conn = DriverManager.getConnection(dbUrl, username, password)) {
			PreparedStatement ps =
					conn.prepareStatement(String.format("SELECT * FROM %s;", pgPath.getFullPath()));

			ResultSetMetaData rsmd = ps.getMetaData();

			String[] names = new String[rsmd.getColumnCount()];
			DataType[] types = new DataType[rsmd.getColumnCount()];

			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				names[i - 1] = rsmd.getColumnName(i);
				types[i - 1] = fromJDBCType(rsmd, i);
				if (rsmd.isNullable(i) == ResultSetMetaData.columnNoNulls) {
					types[i - 1] = types[i - 1].notNull();
				}
			}

			TableSchema.Builder tableBuilder = new TableSchema.Builder().fields(names, types);
			TableSchema tableSchema = tableBuilder.build();
			Map<String, String> props = new HashMap<>();
			props.put(CONNECTOR.key(), HologresJdbcConfigs.HOLOGRES_TABLE_TYPE);
			props.put(ENDPOINT.key(), endpoint);
			props.put(DATABASE.key(), tablePath.getDatabaseName());
			props.put(TABLE.key(), pgPath.getFullPath());
			props.put(USERNAME.key(), username);
			props.put(PASSWORD.key(), password);
			return new HologresCatalogTable(tableSchema, props, "");
		} catch (Exception e) {
			throw new CatalogException(
					String.format("Failed getting table %s", tablePath.getFullName()), e);
		}
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		List<String> tables = null;
		LOG.info("Object path:" + tablePath);
		try {
			tables = listTables(tablePath.getDatabaseName());
		} catch (DatabaseNotExistException e) {
			LOG.info("Database " + tablePath.getDatabaseName() + " does not exist");
			return false;
		}

		return tables.contains(
				PostgresTablePath.fromFlinkTableName(tablePath.getObjectName()).getFullPath());
	}

	@Override
	public Optional<Factory> getFactory() {
		return Optional.of(new HologresJdbcTableFactory());
	}

	// Postgres jdbc driver maps several alias to real type, we use real type rather than alias:
	// serial2 <=> int2
	// smallserial <=> int2
	// serial4 <=> serial
	// serial8 <=> bigserial
	// smallint <=> int2
	// integer <=> int4
	// int <=> int4
	// bigint <=> int8
	// float <=> float8
	// boolean <=> bool
	// decimal <=> numeric
	public static final String PG_SERIAL = "serial";
	public static final String PG_BIGSERIAL = "bigserial";
	public static final String PG_BYTEA = "bytea";
	public static final String PG_BYTEA_ARRAY = "_bytea";
	public static final String PG_SMALLINT = "int2";
	public static final String PG_SMALLINT_ARRAY = "_int2";
	public static final String PG_INTEGER = "int4";
	public static final String PG_INTEGER_ARRAY = "_int4";
	public static final String PG_BIGINT = "int8";
	public static final String PG_BIGINT_ARRAY = "_int8";
	public static final String PG_REAL = "float4";
	public static final String PG_REAL_ARRAY = "_float4";
	public static final String PG_DOUBLE_PRECISION = "float8";
	public static final String PG_DOUBLE_PRECISION_ARRAY = "_float8";
	public static final String PG_NUMERIC = "numeric";
	public static final String PG_NUMERIC_ARRAY = "_numeric";
	public static final String PG_BOOLEAN = "bool";
	public static final String PG_BOOLEAN_ARRAY = "_bool";
	public static final String PG_TIMESTAMP = "timestamp";
	public static final String PG_TIMESTAMP_ARRAY = "_timestamp";
	public static final String PG_TIMESTAMPTZ = "timestamptz";
	public static final String PG_TIMESTAMPTZ_ARRAY = "_timestamptz";
	public static final String PG_DATE = "date";
	public static final String PG_DATE_ARRAY = "_date";
	public static final String PG_TIME = "time";
	public static final String PG_TIME_ARRAY = "_time";
	public static final String PG_TEXT = "text";
	public static final String PG_TEXT_ARRAY = "_text";
	public static final String PG_CHAR = "bpchar";
	public static final String PG_CHAR_ARRAY = "_bpchar";
	public static final String PG_CHARACTER = "character";
	public static final String PG_CHARACTER_ARRAY = "_character";
	public static final String PG_CHARACTER_VARYING = "varchar";
	public static final String PG_CHARACTER_VARYING_ARRAY = "_varchar";
	public static final String PG_GEOGRAPHY = "geography";
	public static final String PG_GEOMETRY = "geometry";
	public static final String PG_BOX2D = "box2d";
	public static final String PG_BOX3D = "box3d";

	/**
	 * Converts Postgres type to Flink {@link DataType}.
	 *
	 * @see org.postgresql.jdbc.TypeInfoCache
	 */
	private DataType fromJDBCType(ResultSetMetaData metadata, int colIndex) throws SQLException {
		String pgType = metadata.getColumnTypeName(colIndex);

		int precision = metadata.getPrecision(colIndex);
		int scale = metadata.getScale(colIndex);

		switch (pgType) {
			case PG_BOOLEAN:
				return DataTypes.BOOLEAN();
			case PG_BOOLEAN_ARRAY:
				return DataTypes.ARRAY(DataTypes.BOOLEAN());
			case PG_BYTEA:
				return DataTypes.BYTES();
			case PG_SMALLINT:
				return DataTypes.SMALLINT();
			case PG_INTEGER:
			case PG_SERIAL:
				return DataTypes.INT();
			case PG_INTEGER_ARRAY:
				return DataTypes.ARRAY(DataTypes.INT());
			case PG_BIGINT:
			case PG_BIGSERIAL:
				return DataTypes.BIGINT();
			case PG_BIGINT_ARRAY:
				return DataTypes.ARRAY(DataTypes.BIGINT());
			case PG_REAL:
				return DataTypes.FLOAT();
			case PG_REAL_ARRAY:
				return DataTypes.ARRAY(DataTypes.FLOAT());
			case PG_DOUBLE_PRECISION:
				return DataTypes.DOUBLE();
			case PG_DOUBLE_PRECISION_ARRAY:
				return DataTypes.ARRAY(DataTypes.DOUBLE());
			case PG_NUMERIC:
				if (precision > 0) {
					return DataTypes.DECIMAL(precision, metadata.getScale(colIndex));
				}
				return DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 18);
			case PG_CHAR:
			case PG_CHARACTER:
				return DataTypes.CHAR(precision);
			case PG_CHAR_ARRAY:
			case PG_CHARACTER_ARRAY:
				return DataTypes.ARRAY(DataTypes.CHAR(precision));
			case PG_CHARACTER_VARYING:
				return DataTypes.VARCHAR(precision);
			case PG_CHARACTER_VARYING_ARRAY:
				return DataTypes.ARRAY(DataTypes.VARCHAR(precision));
			case PG_GEOGRAPHY:
			case PG_GEOMETRY:
			case PG_BOX2D:
			case PG_BOX3D:
			case PG_TEXT:
				return DataTypes.STRING();
			case PG_TEXT_ARRAY:
				return DataTypes.ARRAY(DataTypes.STRING());
			case PG_TIMESTAMP:
				return DataTypes.TIMESTAMP(scale);
			case PG_TIMESTAMPTZ:
				return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(scale);
			case PG_TIME:
				return DataTypes.TIME(scale);
			case PG_DATE:
				return DataTypes.DATE();
			default:
				throw new UnsupportedOperationException(
						String.format("Doesn't support Postgres type '%s' yet", pgType));
		}
	}

	@Override
	public void createDatabase(String s, CatalogDatabase catalogDatabase, boolean b) throws DatabaseAlreadyExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void dropDatabase(String s, boolean b, boolean b1) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterDatabase(String s, CatalogDatabase catalogDatabase, boolean b) throws DatabaseNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<String> listViews(String s) throws DatabaseNotExistException, CatalogException {
		return Collections.emptyList();
	}

	@Override
	public void dropTable(ObjectPath objectPath, boolean b) throws TableNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void renameTable(ObjectPath objectPath, String s, boolean b) throws TableNotExistException, TableAlreadyExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void createTable(ObjectPath objectPath, CatalogBaseTable catalogBaseTable, boolean b) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterTable(ObjectPath objectPath, CatalogBaseTable catalogBaseTable, boolean b) throws TableNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath objectPath) throws TableNotExistException, TableNotPartitionedException, CatalogException {
		return Collections.emptyList();
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec) throws TableNotExistException, TableNotPartitionedException, CatalogException {
		return Collections.emptyList();
	}

	@Override
	public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath objectPath, List<Expression> list) throws TableNotExistException, TableNotPartitionedException, CatalogException {
		return Collections.emptyList();
	}

	@Override
	public CatalogPartition getPartition(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec) throws PartitionNotExistException, CatalogException {
		throw new PartitionNotExistException(getName(), objectPath, catalogPartitionSpec);
	}

	@Override
	public boolean partitionExists(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec) throws CatalogException {
		return false;
	}

	@Override
	public void createPartition(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, CatalogPartition catalogPartition, boolean b) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void dropPartition(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, boolean b) throws PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterPartition(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, CatalogPartition catalogPartition, boolean b) throws PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<String> listFunctions(String s) throws DatabaseNotExistException, CatalogException {
		return Collections.emptyList();
	}

	@Override
	public CatalogFunction getFunction(ObjectPath objectPath) throws FunctionNotExistException, CatalogException {
		throw new FunctionNotExistException(getName(), objectPath);
	}

	@Override
	public boolean functionExists(ObjectPath objectPath) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void createFunction(ObjectPath objectPath, CatalogFunction catalogFunction, boolean b) throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterFunction(ObjectPath objectPath, CatalogFunction catalogFunction, boolean b) throws FunctionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void dropFunction(ObjectPath objectPath, boolean b) throws FunctionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogTableStatistics getTableStatistics(ObjectPath objectPath) throws TableNotExistException, CatalogException {
		return CatalogTableStatistics.UNKNOWN;
	}

	@Override
	public CatalogColumnStatistics getTableColumnStatistics(ObjectPath objectPath) throws TableNotExistException, CatalogException {
		return CatalogColumnStatistics.UNKNOWN;
	}

	@Override
	public CatalogTableStatistics getPartitionStatistics(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec) throws PartitionNotExistException, CatalogException {
		return CatalogTableStatistics.UNKNOWN;
	}

	@Override
	public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec) throws PartitionNotExistException, CatalogException {
		return CatalogColumnStatistics.UNKNOWN;
	}

	@Override
	public void alterTableStatistics(ObjectPath objectPath, CatalogTableStatistics catalogTableStatistics, boolean b) throws TableNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterTableColumnStatistics(ObjectPath objectPath, CatalogColumnStatistics catalogColumnStatistics, boolean b) throws TableNotExistException, CatalogException, TablePartitionedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterPartitionStatistics(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, CatalogTableStatistics catalogTableStatistics, boolean b) throws PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterPartitionColumnStatistics(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, CatalogColumnStatistics catalogColumnStatistics, boolean b) throws PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}
}
