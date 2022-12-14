package com.alibaba.ververica.connectors.hologres.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;

import com.alibaba.hologres.org.postgresql.PGProperty;
import com.alibaba.ververica.connectors.hologres.config.HologresConfigs;
import com.alibaba.ververica.connectors.hologres.config.JDBCOptions;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.alibaba.ververica.connectors.hologres.config.HologresConfigs.ARRAY_DELIMITER;
import static com.alibaba.ververica.connectors.hologres.config.HologresConfigs.FIELD_DELIMITER;

/** JDBCUtils. */
public class JDBCUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JDBCUtils.class);
    private static Map<JDBCOptions, Connection> connections = new ConcurrentHashMap<>();

    public static String getDbUrl(String holoFrontend, String db) {
        return "jdbc:hologres://" + holoFrontend + "/" + db;
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

    public static Connection createConnection(JDBCOptions options, String url) {
        try {
            Class.forName("com.alibaba.hologres.org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        try {
            return DriverManager.getConnection(
                    options.getDbUrl(), options.getUsername(), options.getPassword());
        } catch (SQLException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed getting connection to %s because %s",
                            options.getDbUrl(), ExceptionUtils.getStackTrace(e)));
        }
    }

    public static Connection createConnection(JDBCOptions options) {
        return createConnection(options, options.getDbUrl());
    }

    public static JDBCOptions getJDBCOptions(ReadableConfig properties) {
        // required property
        String frontend = properties.get(HologresConfigs.ENDPOINT);
        String db = properties.get(HologresConfigs.DATABASE);
        String table = properties.get(HologresConfigs.TABLE);
        String username = properties.get(HologresConfigs.USERNAME);
        String pwd = properties.get(HologresConfigs.PASSWORD);
        String delimiter =
                properties.getOptional(ARRAY_DELIMITER).isPresent()
                        ? properties.get(ARRAY_DELIMITER)
                        : properties.get(FIELD_DELIMITER);

        return new JDBCOptions(db, table, username, pwd, frontend, delimiter);
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
        PGProperty.APPLICATION_NAME.set(info, "hologres-connector-flink_copy");
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
        String endpoint = null;
        try (Connection conn = createConnection(options, url)) {
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
        return replaceJdbcUrlEndpoint(url, endpoint);
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

    private static String replaceJdbcUrlEndpoint(String originalUrl, String newEndpoint) {
        String replacement = "//" + newEndpoint + "/";
        return originalUrl.replaceFirst("//\\S+/", replacement);
    }
}
