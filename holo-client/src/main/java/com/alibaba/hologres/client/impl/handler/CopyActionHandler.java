/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.ConnectionHolder;
import com.alibaba.hologres.client.impl.action.CopyAction;
import com.alibaba.hologres.client.impl.copy.CopyContext;
import com.alibaba.hologres.client.impl.copy.InternalPipedOutputStream;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.IdentifierUtil;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.copy.CopyOut;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.sql.SQLException;
import java.sql.Statement;

/** CopyAction处理类. */
public class CopyActionHandler extends ActionHandler<CopyAction> {

    public static final Logger LOGGER = LoggerFactory.getLogger(CopyActionHandler.class);

    private static final String NAME = "copy";
    private final HoloConfig config;
    private final ConnectionHolder connectionHolder;

    public CopyActionHandler(ConnectionHolder connectionHolder, HoloConfig config) {
        super(config);
        this.config = config;
        this.connectionHolder = connectionHolder;
    }

    public long doCopyOut(final CopyContext copyContext, final OutputStream to)
            throws SQLException, IOException {
        byte[] buf;
        final CopyOut cp = (CopyOut) copyContext.getCopyOperation();
        try {
            while ((buf = cp.readFromCopy()) != null) {
                to.write(buf);
            }
            return cp.getHandledRowCount();
        } catch (Exception e) {
            try {
                copyContext.cancel();
            } catch (SQLException sqlEx) {
                LOGGER.error("copy out cancel failed", sqlEx);
            }
            if (e instanceof IOException) {
                try { // read until exhausted or operation cancelled SQLException
                    while ((buf = cp.readFromCopy()) != null) {}
                } catch (SQLException sqlEx) {
                } // typically after several kB
            }
            throw e;
        }
    }

    public long doCopyIn(final CopyContext copyContext, final InputStream from, int bufferSize)
            throws SQLException, IOException {
        final CopyIn cp = (CopyIn) copyContext.getCopyOperation();
        byte[] buf = new byte[bufferSize];
        int len;
        boolean hasException = false;
        try {
            while ((len = from.read(buf)) >= 0) {
                if (len > 0) {
                    cp.writeToCopy(buf, 0, len);
                }
            }
            return cp.endCopy();
        } catch (Exception e) {
            hasException = true;
            try {
                copyContext.cancel();
            } catch (SQLException sqlEx) {
                LOGGER.error("copy in cancel failed", sqlEx);
            }
            throw e;
        } finally {
            try {
                if (from instanceof PipedInputStream) {
                    from.close();
                }
            } catch (IOException ioEx) {
                if (hasException) {
                    LOGGER.error("close piped input stream failed", ioEx);
                } else {
                    throw ioEx;
                }
            }
        }
    }

    @Override
    public void handle(final CopyAction action) {
        try {
            action.getFuture()
                    .complete(
                            (Long)
                                    connectionHolder.retryExecute(
                                            (conn) -> {
                                                PgConnection pgConn =
                                                        conn.unwrap(PgConnection.class);
                                                CopyManager manager = new CopyManager(pgConn);
                                                TableSchema schema = action.getSchema();
                                                OutputStream os = action.getOs();
                                                try {
                                                    long ret = -1;
                                                    switch (action.getMode()) {
                                                        case OUT:
                                                            try {
                                                                StringBuilder sb =
                                                                        new StringBuilder();
                                                                sb.append("COPY (select ");
                                                                boolean first = true;
                                                                for (Column column :
                                                                        schema.getColumnSchema()) {
                                                                    if (!first) {
                                                                        sb.append(",");
                                                                    }
                                                                    first = false;
                                                                    sb.append(
                                                                            IdentifierUtil
                                                                                    .quoteIdentifier(
                                                                                            column
                                                                                                    .getName(),
                                                                                            true));
                                                                }
                                                                sb.append(" from ")
                                                                        .append(
                                                                                schema.getTableNameObj()
                                                                                        .getFullName());
                                                                if (action.getStartShardId() > -1
                                                                        && action.getEndShardId()
                                                                                > -1) {
                                                                    sb.append(
                                                                                    " where hg_shard_id>=")
                                                                            .append(
                                                                                    action
                                                                                            .getStartShardId())
                                                                            .append(
                                                                                    " and hg_shard_id<")
                                                                            .append(
                                                                                    action
                                                                                            .getEndShardId());
                                                                }
                                                                sb.append(
                                                                        ") TO STDOUT DELIMITER ',' ESCAPE '\\' CSV QUOTE '\"' NULL AS 'N'");
                                                                String sql = sb.toString();
                                                                LOGGER.info("copy sql:{}", sql);
                                                                os = action.getOs();
                                                                CopyOut copyOut =
                                                                        manager.copyOut(sql);
                                                                CopyContext copyContext =
                                                                        new CopyContext(
                                                                                conn, copyOut);
                                                                action.getReadyToStart()
                                                                        .complete(
                                                                                new CopyContext(
                                                                                        conn,
                                                                                        copyOut));
                                                                long rowCount =
                                                                        doCopyOut(copyContext, os);

                                                                if (os
                                                                        instanceof
                                                                        InternalPipedOutputStream) {
                                                                    os.close();
                                                                }
                                                                ret = rowCount;
                                                            } catch (Exception e) {
                                                                action.getReadyToStart()
                                                                        .completeExceptionally(e);
                                                                throw e;
                                                            }
                                                            break;
                                                        case IN:
                                                            {
                                                                boolean hasException = false;
                                                                try {
                                                                    if (action.getStartShardId()
                                                                                    > -1
                                                                            && action
                                                                                            .getEndShardId()
                                                                                    > -1) {
                                                                        StringBuilder sql =
                                                                                new StringBuilder(
                                                                                        "set hg_experimental_target_shard_list='");
                                                                        boolean first = true;
                                                                        for (int i =
                                                                                        action
                                                                                                .getStartShardId();
                                                                                i
                                                                                        < action
                                                                                                .getEndShardId();
                                                                                ++i) {
                                                                            if (!first) {
                                                                                sql.append(",");
                                                                            }
                                                                            first = false;
                                                                            sql.append(i);
                                                                        }
                                                                        sql.append("'");
                                                                        try (Statement stat =
                                                                                pgConn
                                                                                        .createStatement()) {
                                                                            stat.execute(
                                                                                    sql.toString());
                                                                        } catch (SQLException e) {
                                                                            LOGGER.error("", e);
                                                                        }
                                                                    }

                                                                    StringBuilder sb =
                                                                            new StringBuilder();
                                                                    sb.append("COPY ")
                                                                            .append(
                                                                                    schema.getTableNameObj()
                                                                                            .getFullName());
                                                                    sb.append(
                                                                            " FROM STDIN DELIMITER ',' ESCAPE '\\' CSV QUOTE '\"' NULL AS 'N'");
                                                                    String sql = sb.toString();
                                                                    LOGGER.info("copy sql:{}", sql);
                                                                    CopyIn copyIn =
                                                                            manager.copyIn(sql);
                                                                    CopyContext copyContext =
                                                                            new CopyContext(
                                                                                    conn, copyIn);
                                                                    action.getReadyToStart()
                                                                            .complete(copyContext);
                                                                    ret =
                                                                            doCopyIn(
                                                                                    copyContext,
                                                                                    action.getIs(),
                                                                                    action
                                                                                                            .getBufferSize()
                                                                                                    > -1
                                                                                            ? action
                                                                                                    .getBufferSize()
                                                                                            : config
                                                                                                    .getCopyInBufferSize());
                                                                } catch (Exception e) {
                                                                    hasException = true;
                                                                    action.getReadyToStart()
                                                                            .completeExceptionally(
                                                                                    e);
                                                                    throw e;
                                                                } finally {
                                                                    if (action.getStartShardId()
                                                                                    > -1
                                                                            && action
                                                                                            .getEndShardId()
                                                                                    > -1) {
                                                                        try (Statement stat =
                                                                                pgConn
                                                                                        .createStatement()) {
                                                                            stat.execute(
                                                                                    "reset hg_experimental_target_shard_list");
                                                                        } catch (SQLException e) {
                                                                            if (hasException) {
                                                                                LOGGER.error(
                                                                                        "reset hg_experimental_target_shard_list failed",
                                                                                        e);
                                                                            } else {
                                                                                throw e;
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            break;
                                                        default:
                                                            throw new SQLException(
                                                                    "copy but InputStream and OutputStream both null");
                                                    }
                                                    return ret;
                                                } catch (Exception e) {
                                                    if (os instanceof InternalPipedOutputStream) {
                                                        try {
                                                            os.close();
                                                        } catch (IOException ignore) {
                                                        }
                                                    }
                                                    throw new SQLException(e);
                                                }
                                            },
                                            1));
        } catch (HoloClientException e) {
            action.getFuture().completeExceptionally(e);
        }
    }

    @Override
    public String getCostMsMetricName() {
        return NAME + METRIC_COST_MS;
    }
}
