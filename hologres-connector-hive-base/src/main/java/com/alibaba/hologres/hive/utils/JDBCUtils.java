package com.alibaba.hologres.hive.utils;

import com.alibaba.hologres.client.auth.AKv4AuthenticationPlugin;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.hive.conf.HoloClientParam;
import com.alibaba.hologres.org.postgresql.PGProperty;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

/** JDBCUtils. */
public class JDBCUtils {
    private static final Logger logger = LoggerFactory.getLogger(JDBCUtils.class);

    public static void setAKv4Region(Properties info, String akv4Region) {
        PGProperty.USER.set(info, AKv4AuthenticationPlugin.AKV4_PREFIX + PGProperty.USER.get(info));
        PGProperty.AUTHENTICATION_PLUGIN_CLASS_NAME.set(
                info, AKv4AuthenticationPlugin.class.getName());
        if (akv4Region != null && !akv4Region.isEmpty()) {
            info.setProperty(AKv4AuthenticationPlugin.REGION, akv4Region);
        }
    }

    public static boolean couldDirectConnect(HoloClientParam param) {
        Properties info = new Properties();
        PGProperty.USER.set(info, param.getUsername());
        PGProperty.PASSWORD.set(info, param.getPassword());
        PGProperty.SOCKET_TIMEOUT.set(info, 5);
        PGProperty.APPLICATION_NAME.set(info, "hologres-connector-hive_copy");
        if (param.isEnableAkv4()) {
            setAKv4Region(info, param.getAkv4Region());
        }
        boolean couldDirectConnect = false;
        try {
            String directUrl = ConnectionUtil.getDirectConnectionUrl(param.getUrl(), info, false);
            couldDirectConnect = !directUrl.equals(param.getUrl());
        } catch (SQLException ignored) {
        }
        return couldDirectConnect;
    }

    public static String formatUrlWithHologres(String oldUrl) {
        String url = oldUrl;
        // the copyWriter just supports jdbc:hologres
        if (oldUrl != null && oldUrl.startsWith("jdbc:postgresql:")) {
            url = "jdbc:hologres:" + oldUrl.substring("jdbc:postgresql:".length());
        }
        return url;
    }

    public static int getConnectionsNumberOfThisJob(HoloClientParam param, String appName) {
        int number = -1;
        try (Connection conn = createConnection(param)) {
            try (Statement stat = conn.createStatement()) {
                String sql =
                        String.format(
                                "select count(*) from pg_stat_activity where application_name='%s';",
                                appName);
                try (ResultSet rs = stat.executeQuery(sql)) {
                    while (rs.next()) {
                        number = rs.getInt(1);
                        break;
                    }
                    if (number == -1) {
                        throw new RuntimeException("Failed to query " + sql);
                    }
                }
            }
        } catch (SQLException t) {
            throw new RuntimeException(t);
        }
        return number;
    }

    public static Connection createConnection(HoloClientParam param) {
        try {
            Class.forName("com.alibaba.hologres.org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        try {
            Properties info = new Properties();
            PGProperty.USER.set(info, param.getUsername());
            PGProperty.PASSWORD.set(info, param.getPassword());
            PGProperty.SOCKET_TIMEOUT.set(info, 360);
            if (param.isEnableAkv4()) {
                setAKv4Region(info, param.getAkv4Region());
            }
            return DriverManager.getConnection(param.getUrl(), info);
        } catch (SQLException e) {
            logErrorAndExceptionInConsole(
                    String.format("Failed getting connection to %s because:", param.getUrl()), e);
            throw new RuntimeException(
                    String.format(
                            "Failed getting connection to %s because %s",
                            param.getUrl(), ExceptionUtils.getStackTrace(e)));
        }
    }

    public static int getFrontendsNumber(Connection conn) {
        try (Statement statement = conn.createStatement()) {
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
                logger.warn("Failed to get hologres frontends number.", e);
                return 0;
            }
            logErrorAndExceptionInConsole("Failed to get hologres frontends number.", e);
            throw new RuntimeException("Failed to get hologres frontends number.", e);
        }
    }

    public static String getSimpleSelectFromStatement(String table, Column[] selectFields) {
        String selectExpressions =
                Arrays.stream(selectFields)
                        .map(a -> quoteIdentifier(a.getName()))
                        .collect(Collectors.joining(", "));

        return "SELECT " + selectExpressions + " FROM " + table;
    }

    public static String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    public static void logErrorAndExceptionInConsole(String error, Exception e) {
        System.out.println(LocalDateTime.now() + " [ERROR] " + error);
        e.printStackTrace(System.out);
    }

    public static void executeSql(Connection pgConn, String sql) {
        executeSql(pgConn, sql, false);
    }

    public static void executeSql(Connection pgConn, String sql, boolean ignoreException) {
        try (Statement stat = pgConn.createStatement()) {
            logger.info("execute sql: {}", sql);
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
