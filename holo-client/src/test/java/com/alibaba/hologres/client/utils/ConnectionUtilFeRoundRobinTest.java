/*
 * Copyright (c) 2026. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.utils;

import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.HoloVersion;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

/** Unit tests for frontend-id discovery and round-robin selection. */
@Test(singleThreaded = true)
public class ConnectionUtilFeRoundRobinTest {
    private static final String URL_PREFIX = "jdbc:round-robin-test://host/";
    private FakeDriver driver;

    @BeforeClass
    public void setUp() throws SQLException {
        driver = new FakeDriver();
        DriverManager.registerDriver(driver);
    }

    @AfterClass
    public void tearDown() throws SQLException {
        DriverManager.deregisterDriver(driver);
    }

    @Test
    public void testAppendFeOption() {
        Assert.assertEquals(
                ConnectionUtil.appendFeOption(URL_PREFIX + "db", 3),
                URL_PREFIX + "db?options=fe=3");
        Assert.assertEquals(
                ConnectionUtil.appendFeOption(URL_PREFIX + "db?token=x", 7),
                URL_PREFIX + "db?token=x&options=fe=7");
        Assert.assertEquals(
                ConnectionUtil.appendFeOption(URL_PREFIX + "db?options=type=fixed%20", 9),
                URL_PREFIX + "db?options=fe=9%20type=fixed%20");
        Assert.assertEquals(
                ConnectionUtil.appendFeOption(URL_PREFIX + "db?token=options=x", 10),
                URL_PREFIX + "db?token=options=x&options=fe=10");
        Assert.assertEquals(
                ConnectionUtil.appendFeOption(
                        URL_PREFIX + "db?options=first&token=x&options=second", 11),
                URL_PREFIX + "db?options=first&token=x&options=fe=11%20second");
        Assert.assertEquals(
                ConnectionUtil.appendFeOption(URL_PREFIX + "db?options=type=fixed&token=x", 12),
                URL_PREFIX + "db?options=fe=12%20type=fixed&token=x");
    }

    @Test
    public void testGetFeIdsUsesExistingConnection() throws SQLException {
        String url = URL_PREFIX + "direct-connection";
        driver.setVersion(url, "5.0.0");
        driver.setRows(url, Arrays.asList(51L, 52L));
        driver.setCurrentWarehouse(url, "warehouse");
        Connection connection = DriverManager.getConnection(url, new Properties());

        try {
            Assert.assertEquals(ConnectionUtil.getFeIds(connection), Arrays.asList(51L, 52L));
            Assert.assertEquals(driver.getConnectionCount(url), 1);
            Assert.assertEquals(driver.getLastSql(url), "SELECT fe_id FROM hg_get_fe_id(?)");
            Assert.assertEquals(driver.getBoundParameter(url), "warehouse");
            Assert.assertEquals(
                    driver.getSqlHistory(url),
                    Arrays.asList(
                            "select hg_version()",
                            "SELECT current_warehouse();",
                            "SELECT fe_id FROM hg_get_fe_id(?)"));
        } finally {
            connection.close();
        }
    }

    @Test
    public void testGetFeIdsSkipsUnsupportedVersion() throws SQLException {
        String url = URL_PREFIX + "unsupported-version";
        driver.setVersion(url, "4.2.9");
        driver.setRows(url, Arrays.asList(51L, 52L));
        driver.setCurrentWarehouse(url, "warehouse");
        Connection connection = DriverManager.getConnection(url, new Properties());

        try {
            Assert.assertEquals(ConnectionUtil.getFeIds(connection), Collections.emptyList());
            Assert.assertNull(ConnectionUtil.pickRoundRobinFeId(connection, 0));
            Assert.assertEquals(
                    driver.getSqlHistory(url),
                    Arrays.asList("select hg_version()", "select hg_version()"));
        } finally {
            connection.close();
        }
    }

    @Test
    public void testIsFeRoundRobinSupported() {
        Assert.assertTrue(ConnectionUtil.isFeRoundRobinSupported(new HoloVersion(5, 0, 0)));
        Assert.assertTrue(ConnectionUtil.isFeRoundRobinSupported(new HoloVersion(6, 1, 3)));
        Assert.assertTrue(ConnectionUtil.isFeRoundRobinSupported(new HoloVersion(4, 2, 10)));
        Assert.assertTrue(ConnectionUtil.isFeRoundRobinSupported(new HoloVersion(4, 1, 30)));
        Assert.assertTrue(ConnectionUtil.isFeRoundRobinSupported(new HoloVersion(4, 0, 45)));
        Assert.assertTrue(ConnectionUtil.isFeRoundRobinSupported(new HoloVersion(3, 2, 42)));
        Assert.assertTrue(ConnectionUtil.isFeRoundRobinSupported(new HoloVersion(3, 1, 52)));

        Assert.assertFalse(ConnectionUtil.isFeRoundRobinSupported(new HoloVersion(4, 2, 9)));
        Assert.assertFalse(ConnectionUtil.isFeRoundRobinSupported(new HoloVersion(4, 1, 29)));
        Assert.assertFalse(ConnectionUtil.isFeRoundRobinSupported(new HoloVersion(4, 0, 44)));
        Assert.assertFalse(ConnectionUtil.isFeRoundRobinSupported(new HoloVersion(3, 2, 41)));
        Assert.assertFalse(ConnectionUtil.isFeRoundRobinSupported(new HoloVersion(3, 1, 51)));
        Assert.assertFalse(ConnectionUtil.isFeRoundRobinSupported(new HoloVersion(3, 0, 99)));
        Assert.assertFalse(ConnectionUtil.isFeRoundRobinSupported(new HoloVersion(2, 1, 27)));
        Assert.assertFalse(ConnectionUtil.isFeRoundRobinSupported(null));
    }

    private static class FakeDriver implements Driver {
        private final java.util.Map<String, List<Long>> rows = new java.util.HashMap<>();
        private final java.util.Map<String, String> versions = new java.util.HashMap<>();
        private final java.util.Map<String, String> currentWarehouses = new java.util.HashMap<>();
        private final java.util.Map<String, Integer> connectionCounts = new java.util.HashMap<>();
        private final java.util.Map<String, String> lastSql = new java.util.HashMap<>();
        private final java.util.Map<String, List<String>> sqlHistory = new java.util.HashMap<>();
        private final java.util.Map<String, String> boundParameters = new java.util.HashMap<>();

        void setRows(String url, List<Long> values) {
            rows.put(url, values);
        }

        void setVersion(String url, String version) {
            versions.put(url, "Hologres " + version + " (tag: release-" + version + ")");
        }

        void setCurrentWarehouse(String url, String warehouse) {
            currentWarehouses.put(url, warehouse);
        }

        int getConnectionCount(String url) {
            return connectionCounts.containsKey(url) ? connectionCounts.get(url) : 0;
        }

        String getLastSql(String url) {
            return lastSql.get(url);
        }

        String getBoundParameter(String url) {
            return boundParameters.get(url);
        }

        List<String> getSqlHistory(String url) {
            return sqlHistory.containsKey(url) ? sqlHistory.get(url) : Collections.emptyList();
        }

        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            if (!acceptsURL(url)) {
                return null;
            }
            connectionCounts.put(url, getConnectionCount(url) + 1);
            return connection(url);
        }

        private Connection connection(final String url) {
            final Statement statement = statement(url);
            return proxy(
                    Connection.class,
                    (proxy, method, args) -> {
                        if ("createStatement".equals(method.getName())) {
                            return statement;
                        }
                        if ("prepareStatement".equals(method.getName())) {
                            return preparedStatement(url, (String) args[0]);
                        }
                        return defaultValue(method.getReturnType());
                    });
        }

        private PreparedStatement preparedStatement(final String url, final String sql) {
            recordSql(url, sql);
            return proxy(
                    PreparedStatement.class,
                    (proxy, method, args) -> {
                        if ("setString".equals(method.getName())) {
                            boundParameters.put(url, (String) args[1]);
                        } else if ("setNull".equals(method.getName())) {
                            boundParameters.put(url, null);
                        } else if ("executeQuery".equals(method.getName())) {
                            return resultSet(rows.get(url), null);
                        }
                        return defaultValue(method.getReturnType());
                    });
        }

        private Statement statement(final String url) {
            return proxy(
                    Statement.class,
                    (proxy, method, args) -> {
                        if ("executeQuery".equals(method.getName())) {
                            String sql = (String) args[0];
                            recordSql(url, sql);
                            if ("select hg_version()".equals(sql)) {
                                return stringResultSet(versions.get(url));
                            }
                            if ("SELECT current_warehouse();".equals(sql)) {
                                return stringResultSet(currentWarehouses.get(url));
                            }
                            return resultSet(rows.get(url), null);
                        }
                        return defaultValue(method.getReturnType());
                    });
        }

        private ResultSet stringResultSet(final String value) {
            return resultSet(
                    value == null ? Collections.emptyList() : Collections.singletonList(0L), value);
        }

        private void recordSql(String url, String sql) {
            lastSql.put(url, sql);
            sqlHistory.computeIfAbsent(url, ignored -> new java.util.ArrayList<>()).add(sql);
        }

        private ResultSet resultSet(final List<Long> values, final String warehouse) {
            final List<Long> result = values == null ? Collections.emptyList() : values;
            return proxy(
                    ResultSet.class,
                    new InvocationHandler() {
                        private int index;

                        @Override
                        public Object invoke(Object proxy, Method method, Object[] args) {
                            if ("next".equals(method.getName())) {
                                return index++ < result.size();
                            }
                            if ("getLong".equals(method.getName())) {
                                return result.get(index - 1);
                            }
                            if ("getString".equals(method.getName())) {
                                return warehouse;
                            }
                            return defaultValue(method.getReturnType());
                        }
                    });
        }

        @Override
        public boolean acceptsURL(String url) {
            return url != null && url.startsWith(URL_PREFIX);
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0F;
        }
        if (type == double.class) {
            return 0D;
        }
        if (type == char.class) {
            return '\0';
        }
        return null;
    }
}
