/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Version;
import com.alibaba.hologres.client.auth.AKv4AuthenticationPlugin;
import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.function.FunctionWithSQLException;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.SSLMode;
import com.alibaba.hologres.client.model.expression.Expression;
import com.alibaba.hologres.client.utils.Tuple;
import org.postgresql.PGProperty;
import org.postgresql.core.Oid;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.security.InvalidParameterException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

/** holder a Hologres connection. */
public class ConnectionHolder implements Closeable {
    public static final Logger LOGGER = LoggerFactory.getLogger(ConnectionHolder.class);

    final String originalJdbcUrl;
    Properties originalInfo;
    final ConnectionWithVersion connWithVersion;
    final boolean isFixed;

    final int tryCount;
    final long retrySleepStepMs;
    final long retrySleepInitMs;
    final int refreshMetaTimeout;
    final boolean refreshMetaAfterConnectionCreated;
    final boolean isEnableDirectConnection;
    final boolean isEnableAffectedRows;
    final boolean isEnableGenerateBinlog;
    final boolean isEnableLogSlowQuery;

    long lastActiveTs;
    long connCreateTs;
    private static List<String> preSqlList;
    private static byte[] lock = new byte[] {};

    static {
        preSqlList = new ArrayList<>();
        preSqlList.add("set hg_experimental_enable_fixed_dispatcher = on");
        preSqlList.add("set hg_experimental_enable_fixed_dispatcher_for_multi_values = on");
        preSqlList.add("set hg_experimental_enable_fixed_dispatcher_for_update = on");
        preSqlList.add("set hg_experimental_enable_fixed_dispatcher_for_delete = on");
        preSqlList.add("set hg_experimental_enable_fixed_dispatcher_for_scan = on");
    }

    /** 两个对象放一起. */
    public class ConnectionWithVersion {
        private PgConnection conn = null;
        private HoloVersion version = null;
        private String jdbcUrl = null;
        private long backendPid = -1;

        public PgConnection getConn() {
            return conn;
        }

        public HoloVersion getVersion() {
            return version;
        }

        public String getJdbcUrl() {
            return jdbcUrl;
        }

        public long getBackendPid() {
            return backendPid;
        }

        public String toString() {
            String versionInfo =
                    connWithVersion.version == null
                            ? ""
                            : (", version:r" + connWithVersion.version.toVersionString());
            String pidInfo =
                    connWithVersion.backendPid == -1
                            ? ""
                            : (", backendPid:" + connWithVersion.backendPid);
            String fixedInfo = isFixed ? ", isFixed:true" : "";
            return "Connection{" + "jdbcUrl=" + jdbcUrl + versionInfo + pidInfo + fixedInfo + '}';
        }
    }

    public static void addPreSql(String sql) {
        synchronized (lock) {
            List<String> temp = new ArrayList<>();
            temp.addAll(preSqlList);
            temp.add(sql);
            preSqlList = temp;
        }
    }

    private void adaptTypeInfoCache(PgConnection conn) {
        conn.getTypeInfo()
                .addCoreType(
                        "jsonb",
                        Oid.JSONB,
                        Types.OTHER,
                        "org.postgresql.util.PGobject",
                        Oid.JSONB_ARRAY);
    }

    private Object owner;

    public ConnectionHolder(HoloConfig config, Object owner, boolean shadingMode, boolean isFixed) {
        this(config, owner, shadingMode, isFixed, null);
    }

    public ConnectionHolder(
            HoloConfig config,
            Object owner,
            boolean shadingMode,
            boolean isFixed,
            Properties userInfo) {
        originalInfo = new Properties();
        String url = config.getJdbcUrl();
        if (shadingMode) {
            if (url.startsWith("jdbc:postgresql:")) {
                url = "jdbc:hologres:" + url.substring("jdbc:postgresql:".length());
            }
        }

        String username = config.getUsername();
        if (config.isUseAKv4() && !username.startsWith("BASIC$")) {
            username = AKv4AuthenticationPlugin.AKV4_PREFIX + username;
        }

        this.isFixed = isFixed;

        if (userInfo != null) {
            originalInfo.putAll(userInfo);
        }

        this.originalJdbcUrl = url;
        PGProperty.USER.set(originalInfo, username);
        PGProperty.PASSWORD.set(originalInfo, config.getPassword());
        if (config.isUseAKv4()) {
            PGProperty.AUTHENTICATION_PLUGIN_CLASS_NAME.set(
                    originalInfo, AKv4AuthenticationPlugin.class.getName());
            if (config.getRegion() != null) {
                originalInfo.setProperty(AKv4AuthenticationPlugin.REGION, config.getRegion());
            }
        }

        PGProperty.APPLICATION_NAME.set(originalInfo, Version.version + "_" + config.getAppName());
        PGProperty.SOCKET_TIMEOUT.set(originalInfo, 360);
        PGProperty.LOGIN_TIMEOUT.set(originalInfo, 60);
        if (config.getSslMode() != SSLMode.DISABLE) {
            PGProperty.SSL.set(originalInfo, true);
            PGProperty.SSL_MODE.set(originalInfo, config.getSslMode().getPgPropertyValue());
            if (config.getSslMode() == SSLMode.VERIFY_CA
                    || config.getSslMode() == SSLMode.VERIFY_FULL) {
                if (config.getSslRootCertLocation() == null) {
                    throw new InvalidParameterException(
                            "When SSL_MODE is set to VERIFY_CA or VERIFY_FULL, the location of the ssl root certificate must be configured.");
                }
                PGProperty.SSL_ROOT_CERT.set(originalInfo, config.getSslRootCertLocation());
            }
        }

        this.tryCount = config.getRetryCount();
        this.retrySleepInitMs = config.getRetrySleepInitMs();
        this.retrySleepStepMs = config.getRetrySleepStepMs();
        this.refreshMetaTimeout = config.getRefreshMetaTimeout();
        this.refreshMetaAfterConnectionCreated = config.isRefreshMetaAfterConnectionCreated();
        this.isEnableDirectConnection = config.isEnableDirectConnection();
        this.isEnableAffectedRows = config.isEnableAffectedRows();
        this.isEnableGenerateBinlog = config.isEnableGenerateBinlog();
        this.isEnableLogSlowQuery = config.isEnableLogSlowQuery();
        lastActiveTs = System.currentTimeMillis();
        this.owner = owner;
        this.connWithVersion = new ConnectionWithVersion();
        this.connWithVersion.jdbcUrl = originalJdbcUrl;
    }

    private PgConnection buildConnection() throws SQLException {
        long start = System.nanoTime();
        Properties info = new Properties();
        String url = originalJdbcUrl;
        info.putAll(originalInfo);
        if (isEnableDirectConnection) {
            url =
                    ConnectionUtil.getDirectConnectionUrl(
                            this.originalJdbcUrl, originalInfo, isFixed);
        }
        if (isFixed) {
            // 当使用Properties和url同时设置options时,会以url中设置的为准,用户可能在url设置过了options,我们采取在url增加参数的方式避免被覆盖
            url = ConnectionUtil.generateFixedUrl(url);
            // set application_name in startup message.
            PGProperty.ASSUME_MIN_SERVER_VERSION.set(info, "9.4");
            PGProperty.PREFER_QUERY_MODE.set(info, "extendedForPrepared");
        }
        this.connWithVersion.jdbcUrl = url;
        LOGGER.info("Try to connect {}, owner:{}", this.connWithVersion.jdbcUrl, owner);
        PgConnection conn = null;
        try {
            conn =
                    DriverManager.getConnection(this.connWithVersion.jdbcUrl, info)
                            .unwrap(PgConnection.class);
            conn.setAutoCommit(true);
            if (!isFixed) {
                if (refreshMetaAfterConnectionCreated && refreshMetaTimeout > 0) {
                    ConnectionUtil.refreshMeta(conn, refreshMetaTimeout);
                }
                List<String> pre = new ArrayList<>(preSqlList);
                if (!this.isEnableAffectedRows) {
                    pre.add("set hg_experimental_enable_fixed_dispatcher_affected_rows = off");
                }
                if (!this.isEnableGenerateBinlog) {
                    pre.add("set hg_experimental_generate_binlog = off");
                }
                if (!this.isEnableLogSlowQuery) {
                    // default on
                    pre.add("set hg_experimental_display_slow_fixed_query_id = off");
                }
                for (String sql : pre) {
                    try (Statement stat = conn.createStatement()) {
                        stat.execute(sql);
                        LOGGER.info("execute preSql success:{}", sql);
                    } catch (SQLException e) {
                        LOGGER.warn("execute preSql fail:{},emsg:{}", sql, e.getMessage());
                    }
                }
                connWithVersion.version = ConnectionUtil.getHoloVersion(conn);
                connWithVersion.backendPid = ConnectionUtil.getBackendPid(conn);
                if (connWithVersion.version.compareTo(Expression.INSERT_SUPPORT_VERSION) >= 0) {
                    pre.add("set hg_experimental_enable_fixed_plan_expression = on");
                }
            } else {
                Tuple<HoloVersion, PgConnection> tuple =
                        ConnectionUtil.getHoloVersionByFixedFe(
                                conn, this.connWithVersion.jdbcUrl, info);
                connWithVersion.version = tuple.l;
                if (connWithVersion.version.compareTo(new HoloVersion(4, 0, 0)) >= 0) {
                    connWithVersion.backendPid = ConnectionUtil.getFixedBackendPid(conn);
                }
                conn = tuple.r;
            }
            adaptTypeInfoCache(conn);
        } catch (Exception e) {
            if (e.getMessage()
                            .contains(
                                    "invalid command-line argument for server process: type=fixed")
                    || e.getMessage().contains("unrecognized configuration parameter \"type\"")) {
                throw new SQLException("FixedFe mode is only supported after hologres version 1.3");
            }
            if (null != conn) {
                try {
                    conn.close();
                } catch (Exception ignore) {
                }
            }
            throw e;
        }

        long end = System.nanoTime();

        LOGGER.info(
                "Build {} succeed, owner:{}, cost:{} ms",
                this.connWithVersion,
                owner,
                (end - start) / 1000000L);

        connCreateTs = System.currentTimeMillis();
        return conn;
    }

    /**
     * 返回连接是否可用. 两种情况算不可用， 1
     * 最近一次的异常是CONNECTION_ERROR(连接坏了)或者META_NOT_MATCH（DDL版本没跟上）或者TABLE_NOT_FOUND(可能DDL版本没跟上) 2
     * select 1 运行失败
     *
     * @param conn 连接
     * @param lastException 最近一次的异常
     * @return true代表连接是可用的；false代表连接坏了需要重建
     */
    private boolean testConnection(Connection conn, HoloClientException lastException) {
        if (lastException != null) {
            switch (lastException.getCode()) {
                case CONNECTION_ERROR:
                case META_NOT_MATCH:
                    return false;
                default:
            }
        }
        if (isFixed) {
            return false;
        }
        try (Statement stat = conn.createStatement()) {
            try (ResultSet rs = stat.executeQuery("select 1")) {
                rs.next();
            }
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    public synchronized <T> T retryExecuteWithVersion(
            FunctionWithSQLException<ConnectionWithVersion, T> action) throws HoloClientException {
        return doRetryExecute(action, tryCount, this::getConnWithVersion);
    }

    public synchronized <T> T retryExecuteWithVersion(
            FunctionWithSQLException<ConnectionWithVersion, T> action, int tryCount)
            throws HoloClientException {
        return doRetryExecute(action, tryCount, this::getConnWithVersion);
    }

    public synchronized <T> T retryExecute(FunctionWithSQLException<PgConnection, T> action)
            throws HoloClientException {
        return doRetryExecute(action, tryCount, this::getPgConnection);
    }

    public synchronized Object retryExecute(
            FunctionWithSQLException<PgConnection, Object> action, int tryCount)
            throws HoloClientException {
        return doRetryExecute(action, tryCount, this::getPgConnection);
    }

    private ConnectionWithVersion getConnWithVersion() {
        return connWithVersion;
    }

    private PgConnection getPgConnection() {
        return connWithVersion.conn;
    }

    public synchronized <C, T> T doRetryExecute(
            FunctionWithSQLException<C, T> action, int tryCount, Supplier<C> supplier)
            throws HoloClientException {
        if (tryCount < 1) {
            tryCount = this.tryCount;
        }
        HoloClientException e = null;
        for (int i = 0; i < tryCount; ++i) {
            try {
                if (connWithVersion.conn == null || connWithVersion.conn.isClosed()) {
                    connWithVersion.conn = buildConnection();
                }
                lastActiveTs = System.currentTimeMillis();
                return action.apply(supplier.get());
            } catch (SQLException exception) {
                e = HoloClientException.fromSqlException(exception, connWithVersion.backendPid);
                try {
                    if (null != connWithVersion.conn && !testConnection(connWithVersion.conn, e)) {
                        Connection tempConn = connWithVersion.conn;
                        connWithVersion.conn = null;
                        connWithVersion.version = null;
                        tempConn.close();
                    }
                } catch (Exception ignore) {
                }
                if (i == tryCount - 1 || !needRetry(e)) {
                    throw e;
                } else {
                    long sleepTime = retrySleepStepMs * i + retrySleepInitMs;
                    LOGGER.warn(
                            "execute sql fail, try again["
                                    + (i + 1)
                                    + "/"
                                    + tryCount
                                    + "], sleepMs = "
                                    + sleepTime
                                    + " ms",
                            e);
                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException ignore) {

                    }
                }
            } catch (Exception exception) {
                throw new HoloClientException(
                        ExceptionCode.INTERNAL_ERROR, "execute fail", exception);
            } finally {
                lastActiveTs = System.currentTimeMillis();
            }
        }
        throw e;
    }

    private boolean needRetry(HoloClientException e) {
        boolean ret = false;
        switch (e.getCode()) {
            case CONNECTION_ERROR:
            case META_NOT_MATCH:
            case READ_ONLY:
            case TOO_MANY_CONNECTIONS:
            case BUSY:
            case TIMEOUT:
                ret = true;
                break;
            default:
        }
        return ret;
    }

    public long getLastActiveTs() {
        return lastActiveTs;
    }

    public long getConnCreateTs() {
        return connCreateTs;
    }

    public HoloVersion getVersion() throws HoloClientException {
        if (connWithVersion.version == null) {
            connWithVersion.version = retryExecute(conn -> ConnectionUtil.getHoloVersion(conn));
        }
        return connWithVersion.version;
    }

    public void close(String closeReason) {
        connWithVersion.version = null;
        if (connWithVersion.conn != null) {
            try {
                LOGGER.info("Close {}, owner:{}, reason:{}", connWithVersion, owner, closeReason);
                connWithVersion.conn.close();
                connWithVersion.conn = null;
            } catch (SQLException ignore) {
            }
        }
    }

    @Override
    public void close() {
        connWithVersion.version = null;
        if (connWithVersion.conn != null) {
            try {
                LOGGER.info("Close {}, owner:{}", connWithVersion, owner);
                connWithVersion.conn.close();
                connWithVersion.conn = null;
            } catch (SQLException ignore) {
            }
        }
    }
}
