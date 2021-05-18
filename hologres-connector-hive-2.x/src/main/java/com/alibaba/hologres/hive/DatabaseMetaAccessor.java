package com.alibaba.hologres.hive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/** DatabaseMetaAccessor. */
public class DatabaseMetaAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseMetaAccessor.class);

    String url;
    String tableName;
    Properties props;

    public DatabaseMetaAccessor(String url, String tableName, String username, String password) {
        this.url = url;
        this.tableName = tableName;
        this.props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
    }

    public List<Column> getColumns() throws SQLException {
        LOGGER.info("getColumns,url:{},props:{}", url, props);
        DriverManager.registerDriver(new org.postgresql.Driver());
        try (Connection conn = DriverManager.getConnection(url, props)) {
            try (Statement stat = conn.createStatement()) {
                try (ResultSet rs =
                        stat.executeQuery("select * from " + tableName + " where 1=2")) {
                    ResultSetMetaData meta = rs.getMetaData();
                    List<Column> columns = new ArrayList<>();
                    for (int i = 0; i < meta.getColumnCount(); ++i) {
                        columns.add(
                                new Column(meta.getColumnName(i + 1), meta.getColumnType(i + 1)));
                    }
                    return columns;
                }
            }
        }
    }
}
