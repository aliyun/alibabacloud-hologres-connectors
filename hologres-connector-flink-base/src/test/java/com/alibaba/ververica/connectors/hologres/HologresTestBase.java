/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.connectors.hologres;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.org.postgresql.util.PSQLState;
import com.alibaba.ververica.connectors.hologres.utils.JDBCUtils;
import org.junit.Before;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

/** Test configs. */
public class HologresTestBase {

    protected EnvironmentSettings streamSettings;

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;
    private static final String ACCESS_ID = "HOLO_ACCESS_ID";
    private static final String ACCESS_KEY = "HOLO_ACCESS_KEY";
    private static final String ENDPOINT = "HOLO_ENDPOINT";
    private static final String TEST_DB = "HOLO_TEST_DB";

    public final String randomSuffix = String.valueOf(Math.abs(new Random().nextLong()));

    public String endpoint;
    public String database;
    public String username;
    public String password;
    public String dimTable;
    public String sinkTable;

    public HologresTestBase() throws IOException {
        endpoint = System.getenv(ENDPOINT);
        database = System.getenv(TEST_DB);
        username = System.getenv(ACCESS_ID);
        password = System.getenv(ACCESS_KEY);
    }

    @Before
    public void prepare() throws Exception {
        EnvironmentSettings.Builder streamBuilder =
                EnvironmentSettings.newInstance().inStreamingMode();
        this.streamSettings = streamBuilder.build();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env, streamSettings);
    }

    public HoloClient getHoloClient() throws HoloClientException {
        HoloConfig config = new HoloConfig();
        config.setJdbcUrl(JDBCUtils.getDbUrl(endpoint, database));
        config.setUsername(username);
        config.setPassword(password);
        return new HoloClient(config);
    }

    public Connection getConnection() throws SQLException {
        return getConnection(database);
    }

    public Connection getConnection(String dbName) throws SQLException {
        Connection conn =
                DriverManager.getConnection(
                        JDBCUtils.getDbUrl(endpoint, dbName), username, password);
        ConnectionUtil.refreshMeta(conn, 10000);
        return conn;
    }

    public void cleanTable(String tableName) {
        try (Connection connection = getConnection(database);
                Statement statement = connection.createStatement()) {
            statement.executeUpdate("Delete from " + tableName);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to clean table: " + tableName, e);
        }
    }

    public void dropTable(String tableName) {
        executeSql("Drop table if exists " + tableName, true);
    }

    protected void executeSql(String sql, boolean ignoreException) {
        executeSql(sql, ignoreException, 60);
    }

    protected void executeSql(String sql, boolean ignoreException, int timeout) {
        executeSql(sql, ignoreException, timeout, database);
    }

    protected void executeSql(String sql, boolean ignoreException, int timeout, String dbName) {
        try (Connection connection = getConnection(dbName);
                Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(timeout);
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            if (PSQLState.QUERY_CANCELED.getState().equals(e.getSQLState())
                    && e.getMessage().contains("canceling statement due to user request")) {
                throw new RuntimeException(
                        String.format(
                                "failed to execute: %s because set query timeout %ss",
                                sql, timeout),
                        e);
            }
            if (!ignoreException) {
                throw new RuntimeException("Failed to execute: " + sql, e);
            }
        }
    }
}
