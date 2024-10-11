package com.alibaba.ververica.connectors.hologres.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;

import com.alibaba.hologres.client.model.SSLMode;
import com.alibaba.hologres.org.postgresql.PGProperty;
import com.alibaba.ververica.connectors.hologres.config.JDBCOptions;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidParameterException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.alibaba.ververica.connectors.hologres.config.HologresConfigs.ARRAY_DELIMITER;
import static com.alibaba.ververica.connectors.hologres.config.HologresConfigs.DATABASE;
import static com.alibaba.ververica.connectors.hologres.config.HologresConfigs.ENDPOINT;
import static com.alibaba.ververica.connectors.hologres.config.HologresConfigs.FIELD_DELIMITER;
import static com.alibaba.ververica.connectors.hologres.config.HologresConfigs.PASSWORD;
import static com.alibaba.ververica.connectors.hologres.config.HologresConfigs.TABLE;
import static com.alibaba.ververica.connectors.hologres.config.HologresConfigs.USERNAME;

/** JDBCUtils. */
public class JDBCUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JDBCUtils.class);
    private static final String DEFAULT_APP_NAME = "hologres-connector-flink-utils";

    public static String getDbUrl(String endpoint, String db) {
        return "jdbc:hologres://" + endpoint + "/" + db;
    }

    public static String getSimpleSelectFromStatement(String table, String[] selectFields) {
        String selectExpressions =
                Arrays.stream(selectFields)
                        .map(e -> quoteIdentifier(e.toLowerCase()))
                        .collect(Collectors.joining(", "));

        return "SELECT " + selectExpressions + " FROM " + table;
    }

    public static String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    // use special jdbc url
    public static Connection createConnection(
            JDBCOptions options,
            String url,
            boolean sslModeConnection,
            int maxRetryCount,
            String appName) {
        try {
            DriverManager.getDrivers();
            Class.forName("com.alibaba.hologres.org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        int retryCount = 0;
        SQLException exception = new SQLException("Failed getting connection.");
        while (retryCount < maxRetryCount) {
            try {
                Thread.sleep(5000L * retryCount);
            } catch (InterruptedException ignore) {
            }
            try {
                Properties properties = new Properties();
                PGProperty.USER.set(properties, options.getUsername());
                PGProperty.PASSWORD.set(properties, options.getPassword());
                PGProperty.APPLICATION_NAME.set(properties, appName);
                PGProperty.SOCKET_TIMEOUT.set(properties, 360);
                if (sslModeConnection && options.getSslMode() != SSLMode.DISABLE) {
                    PGProperty.SSL.set(properties, true);
                    PGProperty.SSL_MODE.set(properties, options.getSslMode().getPgPropertyValue());
                    if (options.getSslMode() == SSLMode.VERIFY_CA
                            || options.getSslMode() == SSLMode.VERIFY_FULL) {
                        if (options.getSslRootCertLocation() == null) {
                            throw new InvalidParameterException(
                                    "When SSL_MODE is set to VERIFY_CA or VERIFY_FULL, the location of the ssl root certificate must be configured.");
                        }
                        PGProperty.SSL_ROOT_CERT.set(properties, options.getSslRootCertLocation());
                    }
                }
                return DriverManager.getConnection(url, properties);
            } catch (SQLException e) {
                exception = e;
                retryCount++;
            }
        }
        throw new RuntimeException(
                String.format("Failed getting connection to %s because:", url), exception);
    }

    public static Connection createConnection(JDBCOptions options) {
        return createConnection(
                options, options.getDbUrl(), /*sslModeConnection*/ false, 3, DEFAULT_APP_NAME);
    }

    public static JDBCOptions getJDBCOptions(ReadableConfig properties) {
        // required property
        String endpoint = properties.get(ENDPOINT);
        String db = properties.get(DATABASE);
        String table = properties.get(TABLE);
        String username = properties.get(USERNAME);
        String pwd = properties.get(PASSWORD);
        String delimiter =
                properties.getOptional(ARRAY_DELIMITER).isPresent()
                        ? properties.get(ARRAY_DELIMITER)
                        : properties.get(FIELD_DELIMITER);
        String sslMode = properties.get(HologresJDBCConfigs.OPTIONAL_CONNECTION_SSL_MODE);
        String sslRootCertLocation =
                properties.get(HologresJDBCConfigs.OPTIONAL_CONNECTION_SSL_ROOT_CERT_LOCATION);

        return new JDBCOptions(
                db, table, username, pwd, endpoint, sslMode, sslRootCertLocation, delimiter);
    }

    public static int getShardCount(JDBCOptions options) {
        Tuple2<String, String> schemaAndTable = getSchemaAndTableName(options.getTable());
        String sql =
                String.format(
                        "select tg.property_value from (select * from"
                                + " hologres.hg_table_properties where table_namespace='%s' and table_name = '%s' and property_key = 'table_group') as a"
                                + " join hologres.hg_table_group_properties as tg on a.property_value = tg.tablegroup_name where tg.property_key='shard_count';",
                        schemaAndTable.f0, schemaAndTable.f1);
        try (Connection connection = JDBCUtils.createConnection(options);
                Statement statement = connection.createStatement();
                ResultSet set = statement.executeQuery(sql)) {
            if (set.next()) {
                return set.getInt("property_value");
            } else {
                throw new RuntimeException(
                        "Failed to query shard count of table " + options.getTable());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean couldDirectConnect(JDBCOptions options) {
        String url = options.getDbUrl();
        Properties info = new Properties();
        PGProperty.USER.set(info, options.getUsername());
        PGProperty.PASSWORD.set(info, options.getPassword());
        PGProperty.APPLICATION_NAME.set(info, DEFAULT_APP_NAME);
        String directUrl = JDBCUtils.getJdbcDirectConnectionUrl(options, url);
        LOG.info("try connect directly to holo with url {}", url);
        try (Connection ignored = DriverManager.getConnection(directUrl, info)) {
        } catch (Exception e) {
            LOG.warn("could not connect directly to holo.");
            return false;
        }
        return true;
    }

    public static String getJdbcDirectConnectionUrl(JDBCOptions options, String url) {
        // Returns the jdbc url directly connected to fe
        LOG.info("Try to connect {} for getting fe endpoint", url);
        String endpoint = null;
        try (Connection conn =
                createConnection(
                        options,
                        url, /*sslModeConnection*/
                        false, /*maxRetryCount*/
                        3, /*appName*/
                        DEFAULT_APP_NAME)) {
            try (Statement stat = conn.createStatement()) {
                try (ResultSet rs =
                        stat.executeQuery("select inet_server_addr(), inet_server_port()")) {
                    while (rs.next()) {
                        endpoint = rs.getString(1) + ":" + rs.getString(2);
                        break;
                    }
                    if (Objects.isNull(endpoint)) {
                        throw new RuntimeException(
                                "Failed to query \"select inet_server_addr(), inet_server_port()\".");
                    }
                }
            }
        } catch (SQLException t) {
            throw new RuntimeException(t);
        }
        return getDbUrl(endpoint, options.getDatabase());
    }

    public static Tuple2<String, String> getSchemaAndTableName(String name) {
        String[] schemaAndTable = name.split("\\.");
        if (schemaAndTable.length > 2) {
            throw new RuntimeException("Table name is in wrong format.");
        }
        String schemaName = schemaAndTable.length == 2 ? schemaAndTable[0] : "public";
        String tableName = schemaAndTable.length == 2 ? schemaAndTable[1] : name;
        return new Tuple2(schemaName.toLowerCase(), tableName.toLowerCase());
    }

    public static int getNumberFrontends(JDBCOptions options) {
        try (Connection connection = JDBCUtils.createConnection(options);
                Statement statement = connection.createStatement()) {
            int maxConnections = 128;
            try (ResultSet rs = statement.executeQuery("show max_connections;")) {
                if (rs.next()) {
                    maxConnections = rs.getInt(1);
                }
            }
            int instanceMaxConnections = 0;
            try (ResultSet rs = statement.executeQuery("select instance_max_connections();")) {
                if (rs.next()) {
                    instanceMaxConnections = rs.getInt(1);
                }
            }
            return instanceMaxConnections / maxConnections;
        } catch (SQLException e) {
            // function instance_max_connections is only supported for hologres version > 1.3.20.
            if (e.getMessage().contains("function instance_max_connections() does not exist")) {
                LOG.warn("Failed to get hologres frontends number.", e);
                return 0;
            }
            throw new RuntimeException("Failed to get hologres frontends number.", e);
        }
    }

    public static void executeSql(Connection pgConn, String sql) {
        executeSql(pgConn, sql, false);
    }

    public static void executeSql(Connection pgConn, String sql, boolean ignoreException) {
        try (Statement stat = pgConn.createStatement()) {
            LOG.info("execute sql: {}", sql);
            stat.execute(sql);
        } catch (SQLException e) {
            if (ignoreException) {
                return;
            }
            throw new RuntimeException(
                    String.format("Failed to execute sql \"%s\". because:", sql), e);
        }
    }
}
