package com.alibaba.hologres.hive;

import com.alibaba.hologres.hive.conf.HoloStorageConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
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
    String filterStr;

    public DatabaseMetaAccessor(String url, String tableName, String username, String password) {
        this.url = url;
        this.tableName = tableName;
        this.props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
    }

    public DatabaseMetaAccessor(Configuration conf) {
        this.tableName = conf.get(HoloStorageConfig.TABLE.getPropertyName());
        this.url = conf.get(HoloStorageConfig.JDBC_URL.getPropertyName());

        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("Table name should be defined");
        }
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("Url should be defined");
        }
        String username = conf.get(HoloStorageConfig.USERNAME.getPropertyName());
        if (username == null || username.isEmpty()) {
            throw new IllegalArgumentException("username should be defined");
        }
        String password = conf.get(HoloStorageConfig.PASSWORD.getPropertyName());
        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException("password should be defined");
        }
        this.filterStr = conf.get(TableScanDesc.FILTER_TEXT_CONF_STR);
        if (this.filterStr == null) {
            this.filterStr = "";
        } else {
            this.filterStr = " where " + filterStr;
        }

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
