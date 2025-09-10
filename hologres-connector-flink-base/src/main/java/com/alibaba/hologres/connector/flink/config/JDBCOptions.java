package com.alibaba.hologres.connector.flink.config;

import com.alibaba.hologres.client.model.SSLMode;

import java.io.Serializable;

/** Configs for JDBCOptions. */
public class JDBCOptions implements Serializable {

    private final String database;
    private final String table;
    private final String username;
    private final String password;
    private final String endpoint;
    private final SSLMode sslMode;
    private final String sslRootCertLocation;
    private final boolean enableAkv4;
    private final String akv4Region;

    public JDBCOptions(
            String database,
            String table,
            String username,
            String password,
            String endpoint,
            String sslMode,
            String sslRootCertLocation,
            boolean enableAkv4,
            String akv4Region) {
        this.database = database;
        this.table = table;
        this.username = username;
        this.password = password;
        this.endpoint = endpoint;
        this.sslMode = SSLMode.fromPgPropertyValue(sslMode);
        this.sslRootCertLocation = sslRootCertLocation;
        this.enableAkv4 = enableAkv4;
        this.akv4Region = akv4Region;
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

    // All jdbc url use the format of hologres(corresponds to
    // com.alibaba.hologres.org.postgresql.Driver)
    // to prevent occasional exceptions where the driver cannot be found.
    public String getDbUrl() {
        return "jdbc:hologres://" + endpoint + "/" + database;
    }

    public boolean isEnableAkv4() {
        return enableAkv4;
    }

    public String getAkv4Region() {
        return akv4Region;
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
                + ", enableAkv4='"
                + enableAkv4
                + '\''
                + ", akv4Region='"
                + akv4Region
                + '\''
                + '}';
    }
}
