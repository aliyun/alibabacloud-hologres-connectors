/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.util;

import com.alibaba.hologres.client.model.TableName;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link ConnectionUtil#getTableProperties} 回退到 pg_catalog.pg_holo_table_properties 的单元测试.
 *
 * <p>由于只有特殊用户才能触发把表属性写入全局系统表 pg_catalog.pg_holo_table_properties 的路径, 无法进行集成测试, 因此这里通过 JDBC 动态代理构造
 * Connection/PreparedStatement/ResultSet 的桩, 覆盖以下四类场景:
 *
 * <ol>
 *   <li>hg_table_properties 命中: 直接返回, 不触发回退查询;
 *   <li>hg_table_properties 未命中, pg_holo_table_properties 回退命中;
 *   <li>两者均未命中: 抛出与改动前一致的 "can not found table";
 *   <li>低版本实例(无 pg_holo_table_properties 系统表): 回退查询抛 42P01, 被吞掉, 行为与改动前完全一致(仍抛 "can not found
 *       table").
 * </ol>
 */
public class ConnectionUtilTablePropertiesFallbackTest {

    private static final String PG_TABLE = "pg_catalog.pg_holo_table_properties";

    /** 场景1: hg_table_properties 命中, 不应触发对全局系统表的回退查询. */
    @Test
    public void testHitFromHgTableProperties() throws Exception {
        List<String[]> hgRows = new ArrayList<>();
        hgRows.add(new String[] {"table_id", "1001"});
        hgRows.add(new String[] {"schema_version", "1"});
        hgRows.add(new String[] {"distribution_key", "id"});

        AtomicBoolean pgQueried = new AtomicBoolean(false);
        Connection conn = fakeConnection(new StatementStub(hgRows), null, pgQueried);

        Map<String, String> properties =
                ConnectionUtil.getTableProperties(conn, TableName.valueOf("public", "t"));

        Assert.assertEquals(properties.get("table_id"), "1001");
        Assert.assertEquals(properties.get("schema_version"), "1");
        Assert.assertEquals(properties.get("distribution_key"), "id");
        Assert.assertFalse(pgQueried.get(), "命中 hg_table_properties 时不应查询全局系统表");
    }

    /** 场景2: hg_table_properties 未命中, 回退到 pg_holo_table_properties 命中. */
    @Test
    public void testFallbackHitFromGlobalTable() throws Exception {
        List<String[]> pgRows = new ArrayList<>();
        pgRows.add(new String[] {"table_id", "2002"});
        pgRows.add(new String[] {"schema_version", "3"});
        pgRows.add(new String[] {"orientation", "column"});

        AtomicBoolean pgQueried = new AtomicBoolean(false);
        Connection conn =
                fakeConnection(
                        new StatementStub(new ArrayList<>()), new StatementStub(pgRows), pgQueried);

        Map<String, String> properties =
                ConnectionUtil.getTableProperties(conn, TableName.valueOf("public", "special"));

        Assert.assertTrue(pgQueried.get(), "hg 未命中时应回退查询全局系统表");
        Assert.assertEquals(properties.get("table_id"), "2002");
        Assert.assertEquals(properties.get("schema_version"), "3");
        Assert.assertEquals(properties.get("orientation"), "column");
    }

    /** 场景3: 两者均未命中, 抛出与改动前一致的 "can not found table". */
    @Test
    public void testBothMiss() {
        AtomicBoolean pgQueried = new AtomicBoolean(false);
        Connection conn =
                fakeConnection(
                        new StatementStub(new ArrayList<>()),
                        new StatementStub(new ArrayList<>()),
                        pgQueried);

        try {
            ConnectionUtil.getTableProperties(conn, TableName.valueOf("public", "missing"));
            Assert.fail("两者均未命中时应抛出 SQLException");
        } catch (SQLException e) {
            Assert.assertTrue(
                    e.getMessage().contains("can not found table"), "错误信息应保持不变: " + e.getMessage());
        }
        Assert.assertTrue(pgQueried.get(), "hg 未命中时应回退查询全局系统表");
    }

    /** 场景4: 低版本实例没有 pg_holo_table_properties, 回退查询抛 42P01, 行为与改动前一致. */
    @Test
    public void testLowVersionInstanceWithoutGlobalTable() {
        StatementStub pgStub = new StatementStub(new ArrayList<>());
        pgStub.throwOnQuery =
                new SQLException(
                        "ERROR: relation \"pg_catalog.pg_holo_table_properties\" does not exist",
                        "42P01");

        AtomicBoolean pgQueried = new AtomicBoolean(false);
        Connection conn = fakeConnection(new StatementStub(new ArrayList<>()), pgStub, pgQueried);

        try {
            ConnectionUtil.getTableProperties(conn, TableName.valueOf("public", "any"));
            Assert.fail("低版本实例下仍应抛出 SQLException");
        } catch (SQLException e) {
            Assert.assertTrue(
                    e.getMessage().contains("can not found table"),
                    "低版本实例行为应与改动前完全一致, 而不是暴露 42P01: " + e.getMessage());
        }
        Assert.assertTrue(pgQueried.get(), "hg 未命中时应回退查询全局系统表");
    }

    /** isTableNotExistException 仅对 42P01 返回 true. */
    @Test
    public void testIsTableNotExistException() {
        Assert.assertTrue(ConnectionUtil.isTableNotExistException(new SQLException("x", "42P01")));
        Assert.assertFalse(ConnectionUtil.isTableNotExistException(new SQLException("x", "42703")));
        Assert.assertFalse(ConnectionUtil.isTableNotExistException(new SQLException("x")));
    }

    // ----------------------------------------------------------------------
    // 下面是基于 JDBC 接口的轻量级桩(通过动态代理实现), 不依赖任何数据库.
    // ----------------------------------------------------------------------

    /** PreparedStatement 桩配置: 返回的行, 或在 executeQuery 时抛出的异常. */
    private static final class StatementStub {
        final List<String[]> rows;
        SQLException throwOnQuery;

        StatementStub(List<String[]> rows) {
            this.rows = rows;
        }
    }

    private static Connection fakeConnection(
            StatementStub hgStub, StatementStub pgStub, AtomicBoolean pgQueried) {
        InvocationHandler handler =
                (proxy, method, args) -> {
                    if ("prepareStatement".equals(method.getName())) {
                        String sql = (String) args[0];
                        if (sql.contains(PG_TABLE)) {
                            pgQueried.set(true);
                            return fakeStatement(pgStub);
                        }
                        return fakeStatement(hgStub);
                    }
                    return defaultValue(method.getReturnType());
                };
        return (Connection)
                Proxy.newProxyInstance(
                        ConnectionUtilTablePropertiesFallbackTest.class.getClassLoader(),
                        new Class<?>[] {Connection.class},
                        handler);
    }

    private static PreparedStatement fakeStatement(StatementStub stub) {
        InvocationHandler handler =
                (proxy, method, args) -> {
                    if ("executeQuery".equals(method.getName())) {
                        if (stub.throwOnQuery != null) {
                            throw stub.throwOnQuery;
                        }
                        return fakeResultSet(stub.rows);
                    }
                    return defaultValue(method.getReturnType());
                };
        return (PreparedStatement)
                Proxy.newProxyInstance(
                        ConnectionUtilTablePropertiesFallbackTest.class.getClassLoader(),
                        new Class<?>[] {PreparedStatement.class},
                        handler);
    }

    private static ResultSet fakeResultSet(List<String[]> rows) {
        InvocationHandler handler =
                new InvocationHandler() {
                    private int cursor = -1;

                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) {
                        switch (method.getName()) {
                            case "next":
                                cursor++;
                                return cursor < rows.size();
                            case "getString":
                                return rows.get(cursor)[((Number) args[0]).intValue() - 1];
                            case "getInt":
                                return Integer.parseInt(
                                        rows.get(cursor)[((Number) args[0]).intValue() - 1]);
                            default:
                                return defaultValue(method.getReturnType());
                        }
                    }
                };
        return (ResultSet)
                Proxy.newProxyInstance(
                        ConnectionUtilTablePropertiesFallbackTest.class.getClassLoader(),
                        new Class<?>[] {ResultSet.class},
                        handler);
    }

    private static Object defaultValue(Class<?> returnType) {
        if (!returnType.isPrimitive()) {
            return null;
        }
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == void.class) {
            return null;
        }
        if (returnType == long.class) {
            return 0L;
        }
        if (returnType == double.class) {
            return 0d;
        }
        if (returnType == float.class) {
            return 0f;
        }
        return 0;
    }
}
