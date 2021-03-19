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

import com.alibaba.ververica.connectors.hologres.utils.JDBCUtils;
import org.junit.Before;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assume.assumeNotNull;

/** Test configs. */
public class HologresTestBase {
    private static final String ACCESS_ID = "HOLO_ACCESS_ID";
    private static final String ACCESS_KEY = "HOLO_ACCESS_KEY";
    private static final String ENDPOINT = "HOLO_ENDPOINT";
    private static final String TEST_DB = "HOLO_TEST_DB";
    private static final String TEST_DIM_TABLE = "HOLO_TEST_DIM_TABLE";
    private static final String TEST_SINK_TABLE = "HOLO_TEST_SINK_TABLE";
    private static final String TEST_EMPTY_TABLE = "HOLO_TEST_EMPTY_TABLE";
    private static final String TEST_SERIAL_DIM_TABLE = "HOLO_TEST_SERIAL_DIM_TABLE";
    private static final String TEST_ONE_TO_MANY_DIM_TABLE = "HOLO_TEST_ONE_2_MANY_DIM_TABLE";

    public String endpoint;
    public String database;
    public String username;
    public String password;
    public String dimTable;
    public String sinkTable;
    public String emptyValueTable;
    public String serialDimTable;
    public String one2ManyTable;

    @Before
    public void init() {
        assumeNotNull(System.getenv(ACCESS_ID));
        assumeNotNull(System.getenv(ACCESS_KEY));
        assumeNotNull(System.getenv(ENDPOINT));
    }

    public HologresTestBase() throws IOException {
        endpoint = System.getenv(ENDPOINT);
        database = System.getenv(TEST_DB);
        username = System.getenv(ACCESS_ID);
        password = System.getenv(ACCESS_KEY);
        dimTable = System.getenv(TEST_DIM_TABLE);
        sinkTable = System.getenv(TEST_SINK_TABLE);
        emptyValueTable = System.getenv(TEST_EMPTY_TABLE);
        serialDimTable = System.getenv(TEST_SERIAL_DIM_TABLE);
        one2ManyTable = System.getenv(TEST_ONE_TO_MANY_DIM_TABLE);
    }

    public void cleanTable(String tableName) throws SQLException {
        try (Connection connection =
                        DriverManager.getConnection(
                                JDBCUtils.getDbUrl(endpoint, database), username, password);
                Statement statement = connection.createStatement()) {
            statement.executeUpdate("Delete from " + tableName);
        }
    }

    public void createTable(String createSql) throws SQLException {
        try (Connection connection =
                        DriverManager.getConnection(
                                JDBCUtils.getDbUrl(endpoint, database), username, password);
                Statement statement = connection.createStatement()) {
            statement.executeUpdate(createSql);
        }
    }

    public void dropTable(String tableName) throws SQLException {
        try (Connection connection =
                        DriverManager.getConnection(
                                JDBCUtils.getDbUrl(endpoint, database), username, password);
                Statement statement = connection.createStatement()) {
            statement.executeUpdate("Drop table " + tableName);
        }
    }
}
