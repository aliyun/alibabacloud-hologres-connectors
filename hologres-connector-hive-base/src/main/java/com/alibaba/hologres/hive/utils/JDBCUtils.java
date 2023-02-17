package com.alibaba.hologres.hive.utils;

import com.alibaba.hologres.hive.conf.HoloClientParam;
import com.alibaba.hologres.org.postgresql.PGProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;

/** JDBCUtils. */
public class JDBCUtils {
    private static final Logger logger = LoggerFactory.getLogger(JDBCUtils.class);

    public static boolean couldDirectConnect(HoloClientParam param) {
        String url = param.getUrl();
        Properties info = new Properties();
        PGProperty.USER.set(info, param.getUsername());
        PGProperty.PASSWORD.set(info, param.getPassword());
        PGProperty.APPLICATION_NAME.set(info, "hologres-connector-hive_copy");
        String directUrl = JDBCUtils.getJdbcDirectConnectionUrl(param);
        try (Connection ignored = DriverManager.getConnection(directUrl, info)) {
        } catch (Exception e) {
            logger.warn("could not connect directly to holo.");
            return false;
        }
        return true;
    }

    public static String getJdbcDirectConnectionUrl(HoloClientParam param) {
        // Returns the jdbc url directly connected to fe
        String endpoint = null;
        String url = getJdbcUrlWithRandomFrontendId(param);
        logger.info("try connect directly to holo with url {}", url);
        try (Connection conn =
                DriverManager.getConnection(url, param.getUsername(), param.getPassword())) {
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
        return replaceJdbcUrlEndpoint(param.getUrl(), endpoint);
    }

    public static String formatUrlWithHologres(String oldUrl) {
        String url = oldUrl;
        // the copyWriter just supports jdbc:hologres
        if (oldUrl != null && oldUrl.startsWith("jdbc:postgresql:")) {
            url = "jdbc:hologres:" + oldUrl.substring("jdbc:postgresql:".length());
        }
        return url;
    }

    private static String getJdbcUrlWithRandomFrontendId(HoloClientParam param) {
        String url = param.getUrl();
        if (param.getHologresFrontendsNumber() > 0) {
            url +=
                    (url.contains("?") ? "&" : "?")
                            + "options=fe="
                            + (Math.abs(new Random().nextInt()) % param.getHologresFrontendsNumber()
                                    + 1);
        }
        return url;
    }

    private static String replaceJdbcUrlEndpoint(String originalUrl, String newEndpoint) {
        String replacement = "//" + newEndpoint + "/";
        return originalUrl.replaceFirst("//\\S+/", replacement);
    }

    public static int getConnectionsNumberOfThisJob(HoloClientParam param, String appName) {
        int number = -1;
        try (Connection conn =
                DriverManager.getConnection(
                        param.getUrl(), param.getUsername(), param.getPassword())) {
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
            throw new RuntimeException("Failed to get hologres frontends number.", e);
        }
    }
}
