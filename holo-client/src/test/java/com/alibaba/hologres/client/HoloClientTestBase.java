/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.TableSchema;
import org.postgresql.PGProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** 测试基类. */
public class HoloClientTestBase {
    public static final Logger LOG = LoggerFactory.getLogger(HoloClientTestBase.class);
    protected static Properties properties;
    protected static HoloVersion holoVersion = new HoloVersion(0, 0, 0);
    private static final Random RANDOM = new Random();

    public static int getShardCount(HoloClient client, TableSchema schema)
            throws HoloClientException {
        return get(
                client.sql(
                        conn -> {
                            int shardCount = -1;
                            try (PreparedStatement ps =
                                    conn.prepareStatement(
                                            "select g.property_value from hologres.hg_table_properties t,hologres.hg_table_group_properties g\n"
                                                    + "where t.property_key='table_group' and g.property_key='shard_count' and table_namespace=? and table_name=? and t.property_value = g.tablegroup_name")) {
                                ps.setObject(1, schema.getTableNameObj().getSchemaName());
                                ps.setObject(2, schema.getTableNameObj().getTableName());
                                try (ResultSet rs = ps.executeQuery()) {
                                    if (rs.next()) {
                                        shardCount = rs.getInt(1);
                                    } else {
                                        throw new SQLException(
                                                "table "
                                                        + schema.getTableNameObj().getFullName()
                                                        + " not exists");
                                    }
                                }
                            }
                            return shardCount;
                        }));
    }

    private static <T> T get(CompletableFuture<T> future) throws HoloClientException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "interrupt", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof HoloClientException) {
                throw (HoloClientException) cause;
            } else {
                throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "", cause);
            }
        }
    }

    /** url=jdbc:postgres://..... user= password= */
    @BeforeTest
    public static void loadProperties() throws Exception {
        Class.forName("org.postgresql.Driver");
        properties = new Properties();

        File file = new File("endpoint3.properties");
        if (file.exists()) {
            try (InputStream is = new FileInputStream(file)) {
                properties.load(is);
            }
        } else {
            String temp = System.getenv("holo_client_test_url");
            if (temp == null) {
                properties = null;
                return;
            }
            properties.setProperty("url", temp);

            temp = System.getenv("holo_client_test_user");
            if (temp == null) {
                properties = null;
                return;
            }
            properties.setProperty("user", temp);

            temp = System.getenv("holo_client_test_password");
            if (temp == null) {
                properties = null;
                return;
            }
            properties.setProperty("password", temp);
        }

        holoVersion = ConnectionUtil.getHoloVersion(buildConnection());
    }

    @BeforeMethod
    public void before() throws Exception {
        doBefore();
    }

    protected void doBefore() throws Exception {}

    @AfterMethod
    public void after() throws Exception {
        doAfter();
    }

    protected void doAfter() throws Exception {}

    protected void execute(Connection conn, String[] sqls) throws SQLException {
        for (String sql : sqls) {
            try (Statement stat = conn.createStatement()) {
                LOG.info("try execute {}", sql);
                stat.execute(sql);
            }
        }
    }

    protected void tryExecute(Connection conn, String[] sqls) {
        for (String sql : sqls) {
            try (Statement stat = conn.createStatement()) {
                LOG.info("try execute {}", sql);
                stat.execute(sql);
            } catch (SQLException e) {
                LOG.info("sql " + sql + " execute failed because: " + e.getMessage());
            }
        }
    }

    protected static Connection buildConnection() throws SQLException {
        return buildConnection(false);
    }

    protected static Connection buildConnection(boolean fixed) throws SQLException {
        Properties info = new Properties();
        if (fixed) {
            info.setProperty(PGProperty.OPTIONS.getName(), "type=fixed");
            PGProperty.PREFER_QUERY_MODE.set(info, "extendedForPrepared");
        }
        return buildConnection(info);
    }

    protected static Connection buildConnection(Properties info) throws SQLException {
        info.putAll(properties);
        return DriverManager.getConnection(info.getProperty("url"), info);
    }

    protected HoloConfig buildConfig() {
        return buildConfig(false);
    }

    protected HoloConfig buildConfig(boolean fixed) {
        HoloConfig config = new HoloConfig();
        config.setJdbcUrl(properties.getProperty("url"));
        config.setUsername(properties.getProperty("user"));
        config.setPassword(properties.getProperty("password"));
        if (properties.getProperty("region") != null) {
            config.setRegion(properties.getProperty("region"));
        }
        config.setRefreshMetaAfterConnectionCreated(true);
        config.setUseAKv4(false);
        // enable direct connection 目前仅进行直连的尝试,无法直连回退到通过vip连接
        config.setEnableDirectConnection(true);
        config.setUseFixedFe(fixed);
        return config;
    }

    // 包含异常信息的异常断言
    public static void assertThrowsWithMessage(
            Class<? extends Throwable> expectedClass,
            String expectedMessage,
            Assert.ThrowingRunnable runnable) {
        try {
            runnable.run();
            Assert.fail(
                    String.format(
                            "Expected %s to be thrown, but nothing was thrown",
                            expectedClass.getSimpleName()));
        } catch (Throwable actualException) {
            actualException.printStackTrace();
            Assert.assertEquals(
                    actualException.getClass(),
                    expectedClass,
                    String.format(
                            "Expected %s to be thrown, but %s was thrown",
                            expectedClass.getSimpleName(), actualException.getClass()));
            Assert.assertTrue(
                    actualException.getMessage().contains(expectedMessage),
                    String.format(
                            "Expected %s to be thrown, but %s was thrown",
                            expectedMessage, actualException.getMessage()));
        }
    }

    public static String genRandomStr(int size) {
        String chars = "abcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(size);
        sb.append(" ,\" A'");
        for (int i = 0; i < size; i++) {
            int index = RANDOM.nextInt(chars.length());
            sb.append(chars.charAt(index));
        }
        return sb.toString();
    }

    // ==================== Parallel Test Infrastructure ====================

    /**
     * Run a list of test tasks in parallel with bounded concurrency and timeout. Usage:
     *
     * <pre>{@code
     * List<Callable<Void>> tasks = new ArrayList<>();
     * for (...) {
     *     tasks.add(() -> { doTest(); return null; });
     * }
     * runParallelTasks(tasks, 16);
     * }</pre>
     */
    protected void runParallelTasks(List<Callable<Void>> tasks, int maxThreads) throws Exception {
        if (tasks.isEmpty()) {
            return;
        }
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(tasks.size(), maxThreads));
        try {
            List<Future<Void>> futures = executor.invokeAll(tasks);
            List<Throwable> errors = new ArrayList<>();
            for (Future<Void> future : futures) {
                try {
                    future.get(5, TimeUnit.MINUTES);
                } catch (Exception e) {
                    errors.add(e.getCause() != null ? e.getCause() : e);
                }
            }
            if (!errors.isEmpty()) {
                StringBuilder msg =
                        new StringBuilder(errors.size() + "/" + tasks.size() + " tasks failed:\n");
                for (Throwable err : errors) {
                    msg.append("  - ").append(err.getMessage()).append("\n");
                }
                AssertionError ae = new AssertionError(msg.toString());
                errors.forEach(ae::addSuppressed);
                throw ae;
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /** Convenience overload with default 16 threads. */
    protected void runParallelTasks(List<Callable<Void>> tasks) throws Exception {
        runParallelTasks(tasks, 32);
    }

    /** Functional interface for test logic that may throw. */
    @FunctionalInterface
    protected interface ThrowingConsumer<T> {
        void accept(T t) throws Exception;
    }

    /**
     * Create a parallel task that manages table lifecycle: drop-if-exists → create → test → drop.
     * The task creates its own connection. Usage:
     *
     * <pre>{@code
     * tasks.add(tableTask("my_table", "create table my_table(id int primary key)", conn -> {
     *     // test logic using conn
     * }));
     * }</pre>
     */
    protected Callable<Void> tableTask(
            String tableName, String createSql, ThrowingConsumer<Connection> body) {
        return () -> {
            try (Connection conn = buildConnection()) {
                execute(conn, new String[] {"set hg_experimental_force_sync_replay = on"});
                execute(conn, new String[] {"drop table if exists " + tableName});
                execute(conn, new String[] {createSql});
                try {
                    body.accept(conn);
                } finally {
                    execute(conn, new String[] {"drop table if exists " + tableName});
                }
            }
            return null;
        };
    }
}
