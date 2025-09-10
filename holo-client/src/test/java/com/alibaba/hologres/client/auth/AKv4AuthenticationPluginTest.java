/*
 * Copyright (c) 2025. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.auth;

import com.alibaba.hologres.client.HoloClientTestBase;
import org.postgresql.PGProperty;
import org.postgresql.plugin.AuthenticationRequestType;
import org.postgresql.util.PSQLException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class AKv4AuthenticationPluginTest extends HoloClientTestBase {
    @Test
    void testExtractRegionFromHost() {
        Assert.assertEquals(
                AKv4AuthenticationPlugin.extractRegionFromHost(
                        "hgpostcn-cn-test-cn-shanghai.hologres.aliyuncs.com"),
                "cn-shanghai");
        Assert.assertEquals(
                AKv4AuthenticationPlugin.extractRegionFromHost(
                        "hgpostcn-cn-test-cn-hangzhou-vpc-st.hologres.aliyuncs.com"),
                "cn-hangzhou");
        Assert.assertEquals(
                AKv4AuthenticationPlugin.extractRegionFromHost(
                        "hgprecn-cn-test-cn-beijing-vpc.hologres.aliyuncs.com"),
                "cn-beijing");
        Assert.assertEquals(
                AKv4AuthenticationPlugin.extractRegionFromHost(
                        "hgprecn-cn-test-cn-shanghai-finance-1-vpc.hologres.aliyuncs.com"),
                "cn-shanghai-finance-1");
        Assert.assertNull(AKv4AuthenticationPlugin.extractRegionFromHost("1234"));
        Assert.assertNull(AKv4AuthenticationPlugin.extractRegionFromHost(null));
    }

    @Test
    void testEncodeAKv4Password() {
        Assert.assertNotEquals(
                AKv4AuthenticationPlugin.encodeAKv4Password("password", "cn-shanghai", "hologres"),
                "");
    }

    @Test
    void testGetPassword() throws PSQLException {
        Properties connectionProperties = new Properties();
        try {
            new AKv4AuthenticationPlugin(connectionProperties);
            Assert.fail("Expected an IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(
                    e.getMessage(),
                    "Unable to extract user, you may specify PGProperty.USER in connection properties");
        }

        PGProperty.USER.set(connectionProperties, AKv4AuthenticationPlugin.AKV4_PREFIX + "user");
        try {
            new AKv4AuthenticationPlugin(connectionProperties);
            Assert.fail("Expected an IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(
                    e.getMessage(),
                    "Unable to extract password, you may specify PGProperty.PASSWORD in connection properties");
        }

        PGProperty.PASSWORD.set(connectionProperties, "password");
        try {
            new AKv4AuthenticationPlugin(connectionProperties);
            Assert.fail("Expected an IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(
                    e.getMessage(),
                    "Unable to extract region from host, you may specify AKv4AuthenticationPlugin.REGION in connection properties");
        }

        PGProperty.PG_HOST.set(connectionProperties, "1234");
        try {
            new AKv4AuthenticationPlugin(connectionProperties);
            Assert.fail("Expected an IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(
                    e.getMessage(),
                    "Unable to extract region from host, you may specify AKv4AuthenticationPlugin.REGION in connection properties");
        }

        PGProperty.PG_HOST.set(
                connectionProperties, "hgpostcn-cn-test-cn-shanghai.hologres.aliyuncs.com");
        AKv4AuthenticationPlugin plugin = new AKv4AuthenticationPlugin(connectionProperties);
        String encodedPassword =
                AKv4AuthenticationPlugin.encodeAKv4Password("password", "cn-shanghai", "hologres");
        Assert.assertEquals(
                new String(plugin.getPassword(AuthenticationRequestType.MD5_PASSWORD)),
                encodedPassword);

        PGProperty.PG_HOST.set(connectionProperties, "1234");
        connectionProperties.setProperty(AKv4AuthenticationPlugin.REGION, "cn-shanghai");
        plugin = new AKv4AuthenticationPlugin(connectionProperties);
        Assert.assertEquals(
                new String(plugin.getPassword(AuthenticationRequestType.MD5_PASSWORD)),
                encodedPassword);

        PGProperty.USER.set(connectionProperties, "user");
        plugin = new AKv4AuthenticationPlugin(connectionProperties);
        Assert.assertEquals(
                new String(plugin.getPassword(AuthenticationRequestType.MD5_PASSWORD)), "password");
    }

    // only run this in local honebox
    @Test
    void TestAKv4AuthenticationPlugin() throws ClassNotFoundException, SQLException {
        if (properties == null) {
            return;
        }

        String url = properties.getProperty("url");
        String user = properties.getProperty("user");
        String password = properties.getProperty("password");
        if (url == null || user == null || password == null) {
            return;
        }
        if (user.startsWith("BASIC$")) {
            return;
        }

        Properties connectionProperties = new Properties();
        PGProperty.USER.set(connectionProperties, AKv4AuthenticationPlugin.AKV4_PREFIX + user);
        PGProperty.PASSWORD.set(connectionProperties, password);
        PGProperty.AUTHENTICATION_PLUGIN_CLASS_NAME.set(
                connectionProperties, AKv4AuthenticationPlugin.class.getName());
        connectionProperties.setProperty(AKv4AuthenticationPlugin.REGION, "local");

        Class.forName("org.postgresql.Driver");
        Connection connection = DriverManager.getConnection(url, connectionProperties);

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT 1");

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(resultSet.getInt(1), 1);
    }
}
