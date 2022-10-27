package com.alibaba.ververica.connectors.hologres.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;

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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.alibaba.ververica.connectors.hologres.config.HologresConfigs.ARRAY_DELIMITER;
import static com.alibaba.ververica.connectors.hologres.config.HologresConfigs.FIELD_DELIMITER;

/** JDBCUtils. */
public class JDBCUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JDBCUtils.class);
    private static Map<JDBCOptions, Connection> connections = new ConcurrentHashMap<>();

    public static String getDbUrl(String holoFrontend, String db) {
        return "jdbc:postgresql://" + holoFrontend + "/" + db;
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

    public static Connection createConnection(JDBCOptions options) {
        try {
            Class.forName("org.postgresql.Driver");
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
        String filter = properties.get(HologresConfigs.FILTER);

        return new JDBCOptions(db, table, username, pwd, frontend, delimiter, filter);
    }

    public static String getFrontendEndpoints(JDBCOptions options) throws SQLException {
        try (Connection connection = JDBCUtils.createConnection(options);
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SHOW hg_frontend_endpoints;")) {
            String endpointStr = null;
            while (rs.next()) {
                endpointStr = rs.getString(1);
                break;
            }
            if (endpointStr == null || endpointStr.isEmpty()) {
                throw new RuntimeException("Failed to query hg_frontend_endpoints.");
            }
            return endpointStr;
        }
    }

    public static String getHolohubEndpoint(JDBCOptions opt) {
        Connection conn = JDBCUtils.createConnection(opt);
        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SHOW hg_datahub_endpoints;");

            String endpointStr = null;
            while (rs.next()) {
                endpointStr = rs.getString(1);
                break;
            }

            if (endpointStr == null || endpointStr.isEmpty()) {
                throw new RuntimeException(
                        "Realtime ingestion service (holohub) not enabled or its endpoint invalid.");
            }

            LOG.info("Got holohub endpoint from FE: {}", endpointStr);

            Map<String, String> endpointMap = new HashMap<>();
            String[] endpoints = endpointStr.split(";");
            for (String endpoint : endpoints) {
                String[] endpointElements = endpoint.split(",");
                if (endpointElements.length != 2) {
                    LOG.error("Endpoint format invalid: {}. FE: {}", endpoint, opt.getEndpoint());
                    throw new RuntimeException(
                            "Realtime ingestion endpoint invalid. endpoint: " + endpointStr);
                }

                endpointMap.put(endpointElements[0], endpointElements[1]);
            }

            String result;
            String feNetwork;
            if (endpoints.length == 1) {
                result = endpoints[0].split(",")[1];
                feNetwork = "single";
            } else {
                feNetwork = getFeEndpointNetwork(opt.getEndpoint());
                if (!endpointMap.containsKey(feNetwork)) {
                    LOG.error(
                            "No network-matching holohub endpoint '{}' found for fe: {}",
                            endpointStr,
                            opt.getEndpoint());
                    throw new RuntimeException(
                            String.format(
                                    "No network-matching realtime ingestion endpoint '%s' found for fe: %s",
                                    endpointStr, opt.getEndpoint()));
                }

                result = endpointMap.get(feNetwork);
            }

            LOG.info(
                    "Choose holohub endpoint: {}, FE: {}, network: {}",
                    result,
                    opt.getEndpoint(),
                    feNetwork);
            return result;
        } catch (SQLException e) {
            LOG.error("Failed to query holohub endpoint. option: {}", opt.toString(), e);
            throw new RuntimeException(
                    "Failed to query holohub endpoint. option: " + opt.toString(), e);
        } finally {
            try {
                if (null != rs) {
                    rs.close();
                }
                if (null != stmt) {
                    stmt.close();
                }
                if (null != conn) {
                    conn.close();
                }
            } catch (SQLException unused) {
            }
        }
    }

    public static String getFeEndpointNetwork(String fe) {
        String firstDomain = fe.split("\\.")[0];
        if (firstDomain.endsWith("-vpc")) {
            return "vpc";
        }
        if (firstDomain.endsWith("-internal")) {
            return "intranet";
        }
        return "internet";
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

    public static Tuple2<String, String> getSchemaAndTableName(String name) {
        String[] schemaAndTable = name.split("\\.");
        if (schemaAndTable.length > 2) {
            throw new RuntimeException("Table name is in wrong format.");
        }
        String schemaName = schemaAndTable.length == 2 ? schemaAndTable[0] : "public";
        String tableName = schemaAndTable.length == 2 ? schemaAndTable[1] : name;
        return new Tuple2(schemaName.toLowerCase(), tableName.toLowerCase());
    }
}
