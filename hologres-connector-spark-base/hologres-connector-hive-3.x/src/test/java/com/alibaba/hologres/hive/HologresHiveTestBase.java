package com.alibaba.hologres.hive;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/** HologresHiveTestBase. */
public class HologresHiveTestBase {
    public final String hiveUsername;
    public final String hivePassword;
    public final String hiveJdbcUrl;
    public final String holoUsername;
    public final String holoPassword;
    public final String holoJdbcUrl;

    public HologresHiveTestBase() throws IOException {
        InputStream inputStream =
                getClass().getClassLoader().getResourceAsStream("setting.properties");
        Properties prop = new Properties();
        prop.load(inputStream);

        // Modify these parameters if don't skip the test.
        hiveUsername = prop.getProperty("HIVE_USERNAME");
        hivePassword = prop.getProperty("HIVE_PASSWORD");
        hiveJdbcUrl = prop.getProperty("HIVE_JDBCURL");
        holoUsername = prop.getProperty("HOLO_USERNAME");
        holoPassword = prop.getProperty("HOLO_PASSWORD");
        holoJdbcUrl = prop.getProperty("HOLO_JDBCURL");
    }

    public Connection getHoloConnection() throws SQLException {
        try {
            Class.forName("com.alibaba.hologres.org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return DriverManager.getConnection(holoJdbcUrl, holoUsername, holoPassword);
    }

    public Connection getHiveConnection() throws SQLException {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return DriverManager.getConnection(hiveJdbcUrl, hiveUsername, hivePassword);
    }
}
