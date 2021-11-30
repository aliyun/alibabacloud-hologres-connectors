/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Version;
import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import org.postgresql.PGProperty;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.FunctionWithSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * holder a Hologres connection.
 */
public class ConnectionHolder implements Closeable {
	public static final Logger LOGGER = LoggerFactory.getLogger(ConnectionHolder.class);

	final String jdbcUrl;
	Properties info;
	PgConnection conn;
	boolean isReplicationConnection = false;

	final int tryCount;
	final long retrySleepStepMs;
	final long retrySleepInitMs;
	final int refreshMetaTimeout;
	final boolean refreshMetaAfterConnectionCreated;

	long lastActiveTs;
	private static List<String> preSqlList;
	private static byte[] lock = new byte[]{};

	static {
		preSqlList = new ArrayList<>();
		preSqlList.add("set hg_experimental_enable_fixed_dispatcher = on");
		preSqlList.add("set hg_experimental_enable_fixed_dispatcher_for_multi_values = on");
		preSqlList.add("set hg_experimental_enable_fixed_dispatcher_for_update = on");
		preSqlList.add("set hg_experimental_enable_fixed_dispatcher_for_delete = on");
	}

	public static void addPreSql(String sql) {
		synchronized (lock) {
			List<String> temp = new ArrayList<>();
			temp.addAll(preSqlList);
			temp.add(sql);
			preSqlList = temp;
		}
	}

	private Object owner;

	public ConnectionHolder(HoloConfig config, Object owner, boolean shadingMode) {
		this(config, owner, shadingMode, null);
	}

	public ConnectionHolder(HoloConfig config, Object owner, boolean shadingMode, Properties userInfo) {
		info = new Properties();
		String url = config.getJdbcUrl();
		if (shadingMode) {
			if (url.startsWith("jdbc:postgresql:")) {
				url = "jdbc:hologres:" + url.substring("jdbc:postgresql:".length());
			}
		}
		PGProperty.REWRITE_BATCHED_INSERTS.set(info, true);
		if (config.isDynamicPartition()) {
			PGProperty.DYNAMIC_PARTITION.set(info, true);
		}
		PGProperty.INPUT_NUMBER_AS_EPOCH_MS_FOR_DATETIME_COLUMN.set(info, config.isInputNumberAsEpochMsForDatetimeColumn());
		PGProperty.INPUT_STRING_AS_EPOCH_MS_FOR_DATETIME_COLUMN.set(info, config.isInputStringAsEpochMsForDatetimeColumn());
		if (config.isEnableClientDynamicPartition()) {
			url += (url.indexOf("?") > -1 ? "&" : "?") + "reWriteInsertsToPartition=false";
		}
		this.jdbcUrl = url;
		PGProperty.USER.set(info, config.getUsername());
		PGProperty.PASSWORD.set(info, config.getPassword());
		PGProperty.META_CACHE_TTL.set(info, (int) config.getMetaCacheTTL());
		PGProperty.APPLICATION_NAME.set(info, Version.version + "_" + config.getAppName());
		PGProperty.REMOVE_U0000_IN_TEXT_COLUMN_VALUE.set(info, config.isRemoveU0000InTextColumnValue());
		PGProperty.REWRITE_BATCHED_DELETES.set(info, config.isReWriteBatchedDeletes());
		PGProperty.BINARY_TRANSFER_ENABLE_NAMES.set(info, "roaringbitmap");
		PGProperty.REWRITE_SQL_MAX_BATCH_SIZE.set(info, config.getRewriteSqlMaxBatchSize());
		PGProperty.SOCKET_TIMEOUT.set(info, 60);
		if (userInfo != null) {
			info.putAll(userInfo);
			if (userInfo.getProperty("replication") != null) {
				LOGGER.info("Create a replication connection holder");
				PGProperty.BINARY_TRANSFER_ENABLE_NAMES.set(info, null);
				isReplicationConnection = true;
			}
		}
		this.tryCount = config.getRetryCount();
		this.retrySleepInitMs = config.getRetrySleepInitMs();
		this.retrySleepStepMs = config.getRetrySleepStepMs();
		this.refreshMetaTimeout = config.getRefreshMetaTimeout();
		this.refreshMetaAfterConnectionCreated = config.isRefreshMetaAfterConnectionCreated();
		lastActiveTs = System.currentTimeMillis();
		this.owner = owner;
	}

	private PgConnection buildConnection() throws SQLException {
		long start = System.nanoTime();
		LOGGER.info("Try to connect {}, owner:{}", this.jdbcUrl, owner);
		PgConnection conn = null;
		try {
			conn = DriverManager.getConnection(this.jdbcUrl, info).unwrap(PgConnection.class);
			conn.setAutoCommit(true);
			if (!isReplicationConnection) {
				if (refreshMetaAfterConnectionCreated && refreshMetaTimeout > 0) {
					ConnectionUtil.refreshMeta(conn, refreshMetaTimeout);
				}
				List<String> pre = preSqlList;
				for (String sql : pre) {
					try (Statement stat = conn.createStatement()) {
						stat.execute(sql);
					} catch (SQLException e) {
						LOGGER.warn("execute preSql fail:{},emsg:{}", sql, e.getMessage());
					}
				}
			}
		} catch (Exception e) {
			if (null != conn) {
				try {
					conn.close();
				} catch (Exception ignore) {
				}
			}
			throw e;
		}

		long end = System.nanoTime();
		LOGGER.info("Connected to {}, owner:{}, cost:{} ms", this.jdbcUrl, owner, (end - start) / 1000000L);
		return conn;
	}


	/**
	 * 返回连接是否可用.
	 * 两种情况算不可用，
	 * 1 最近一次的异常是CONNECTION_ERROR(连接坏了)或者META_NOT_MATCH（DDL版本没跟上）或者TABLE_NOT_FOUND(可能DDL版本没跟上)
	 * 2 select 1 运行失败
	 *
	 * @param conn          连接
	 * @param lastException 最近一次的异常
	 * @return true代表连接是可用的；false代表连接坏了需要重建
	 */
	private boolean testConnection(Connection conn, HoloClientException lastException) {
		if (lastException != null) {
			switch (lastException.getCode()) {
				case CONNECTION_ERROR:
				case META_NOT_MATCH:
				case TABLE_NOT_FOUND:
					return false;
				default:
			}
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

	public synchronized Object retryExecute(FunctionWithSQLException<PgConnection, Object> action) throws HoloClientException {
		return retryExecute(action, tryCount);
	}

	public synchronized Object retryExecute(FunctionWithSQLException<PgConnection, Object> action, int tryCount) throws HoloClientException {
		if (tryCount < 1) {
			tryCount = this.tryCount;
		}
		HoloClientException e = null;
		for (int i = 0; i < tryCount; ++i) {
			try {
				if (conn == null || conn.isClosed()) {
					conn = buildConnection();
				}
				lastActiveTs = System.currentTimeMillis();
				return action.apply(conn);
			} catch (SQLException exception) {
				e = HoloClientException.fromSqlException(exception);
				try {
					if (null != conn && !testConnection(conn, e)) {
						Connection tempConn = conn;
						conn = null;
						tempConn.close();
					}
				} catch (Exception ignore) {
				}
				if (i == tryCount - 1 || !needRetry(e)) {
					throw e;
				} else {
					long sleepTime = retrySleepStepMs * i + retrySleepInitMs;
					LOGGER.warn("execute sql fail, try again[" + (i + 1) + "/" + tryCount + "], sleepMs = " + sleepTime + " ms", exception);
					try {
						Thread.sleep(sleepTime);
					} catch (InterruptedException ignore) {

					}
				}
			} catch (Exception exception) {
				throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "execute fail", exception);
			} finally {
				lastActiveTs = System.currentTimeMillis();
			}
		}
		throw e;
	}

	private boolean needRetry(HoloClientException e) {
		if (conn != null && testConnection(conn, e)) {
			boolean ret = false;
			switch (e.getCode()) {
				case CONNECTION_ERROR:
				case META_NOT_MATCH:
				case READ_ONLY:
				case TABLE_NOT_FOUND:
				case BUSY:
					ret = true;
					break;
				default:
			}
			return ret;
		} else {
			if (e.getCode() == ExceptionCode.AUTH_FAIL) {
				return false;
			}
			return true;
		}
	}

	public long getLastActiveTs() {
		return lastActiveTs;
	}

	@Override
	public void close() {
		if (conn != null) {
			try {
				LOGGER.info("Close connection to {}, owner:{}", this.jdbcUrl, owner);
				conn.close();
				LOGGER.info("Closed connection to {}, owner:{}", this.jdbcUrl, owner);
				conn = null;
			} catch (SQLException ignore) {
			}
		}
	}
}
