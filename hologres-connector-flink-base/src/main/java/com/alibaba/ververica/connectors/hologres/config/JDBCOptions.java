package com.alibaba.ververica.connectors.hologres.config;

import java.io.Serializable;
import java.util.Objects;

/** Configs for JDBCOptions. */
public class JDBCOptions implements Serializable {

    private final String database;
    private final String table;
    private final String username;
    private final String password;
    private final String endpoint;
    private final String delimiter;

    private final String filter;

    public JDBCOptions(
            String database, String table, String username, String password, String endpoint) {
        this(
                database,
                table,
                username,
                password,
                endpoint,
                HologresConfigs.DEFAULT_FIELD_DELIMITER);
    }

    public JDBCOptions(
            String database,
            String table,
            String username,
            String password,
            String endpoint,
            String delimiter) {
        this.database = database;
        this.table = table;
        this.username = username;
        this.password = password;
        this.endpoint = endpoint;
        this.delimiter = delimiter;
        this.filter = null;
    }

    public JDBCOptions(
            String database,
            String table,
            String username,
            String password,
            String endpoint,
            String delimiter,
            String filter) {
        this.database = database;
        this.table = table;
        this.username = username;
        this.password = password;
        this.endpoint = endpoint;
        this.delimiter = delimiter;
        this.filter = filter;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public String getDbUrl() {
        return "jdbc:postgresql://" + endpoint + "/" + database;
    }

    public String getFilter() {
        return this.filter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JDBCOptions that = (JDBCOptions) o;

        return Objects.equals(database, that.database)
                && Objects.equals(table, that.table)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(endpoint, that.endpoint)
                && Objects.equals(delimiter, that.delimiter)
                && Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, table, username, password, endpoint, delimiter, filter);
    }

    @Override
    public String toString() {
        return "JDBCOptions{"
                + "database='"
                + database
                + '\''
                + ", table='"
                + table
                + '\''
                + ", username='"
                + username
                + '\''
                + ", password='"
                + password
                + '\''
                + ", frontend='"
                + endpoint
                + '\''
                + ", delimiter='"
                + delimiter
                + '\''
                + ", filter='"
                + filter
                + "\'"
                + '}';
    }
}
