package com.alibaba.ververica.connectors.hologres.config;

import com.alibaba.hologres.client.model.SSLMode;

import java.io.Serializable;
import java.util.Objects;

/** Configs for JDBCOptions. */
public class JDBCOptions implements Serializable {

    private final String database;
    private final String table;
    private final String username;
    private final String password;
    private final String endpoint;
    private final SSLMode sslMode;
    private final String sslRootCertLocation;
    private final String delimiter;

    public JDBCOptions(
            String database,
            String table,
            String username,
            String password,
            String endpoint,
            String sslMode,
            String sslRootCertLocation,
            String delimiter) {
        this.database = database;
        this.table = table;
        this.username = username;
        this.password = password;
        this.endpoint = endpoint;
        this.sslMode = SSLMode.fromPgPropertyValue(sslMode);
        this.sslRootCertLocation = sslRootCertLocation;
        this.delimiter = delimiter;
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

    public SSLMode getSslMode() {
        return sslMode;
    }

    public String getSslRootCertLocation() {
        return sslRootCertLocation;
    }

    public String getDelimiter() {
        return delimiter;
    }

    // All jdbc url use the format of hologres(corresponds to
    // com.alibaba.hologres.org.postgresql.Driver)
    // to prevent occasional exceptions where the driver cannot be found.
    public String getDbUrl() {
        return "jdbc:hologres://" + endpoint + "/" + database;
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
                && Objects.equals(sslMode, that.sslMode)
                && Objects.equals(sslRootCertLocation, that.sslRootCertLocation)
                && Objects.equals(delimiter, that.delimiter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, table, username, password, endpoint, delimiter);
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
                + "********"
                + '\''
                + ", endpoint='"
                + endpoint
                + '\''
                + ", connection.ssl.mode='"
                + sslMode
                + '\''
                + ", connection.ssl.root-cert.location='"
                + sslRootCertLocation
                + '\''
                + ", delimiter='"
                + delimiter
                + '\''
                + '}';
    }
}
