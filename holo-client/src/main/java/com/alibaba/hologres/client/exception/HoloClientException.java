/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.exception;

import org.postgresql.util.PSQLState;

import java.sql.SQLException;

/** Base class for holo-client exceptions. */
public class HoloClientException extends Exception {

    private final ExceptionCode code;

    public HoloClientException(ExceptionCode code, String message) {
        super(message);
        this.code = code;
    }

    public HoloClientException(ExceptionCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public ExceptionCode getCode() {
        return code;
    }

    public static HoloClientException fromSqlException(SQLException e) {
        return fromSqlException(e, -1);
    }

    public static HoloClientException fromSqlException(SQLException e, long backendPid) {
        ExceptionCode code = ExceptionCode.UNKNOWN_ERROR;
        String state = e.getSQLState();
        // 08P02 (IDLE_SESSION_TIMEOUT) 虽然属于 SQLState class 08，但语义上是服务端主动断开
        // idle 会话超时，归类为 TIMEOUT；必须在下面的 class 08 兜底之前判断。
        if ("08P02".equals(/*IDLE_SESSION_TIMEOUT*/ state)) {
            code = ExceptionCode.TIMEOUT;
        } else if (PSQLState.isConnectionError(state)
                // SQLState class 08 即 SQL 标准的 Connection Exception 类。
                // PSQLState.isConnectionError 只覆盖 08001/08003/08004/08006/08007，
                // 不包含协议级错乱 08P01（"Expected command status BEGIN, got ."）等。
                // 这种异常通常发生在 Hologres frontend/shard 缩容、server 已关闭 TCP
                // 但客户端仍持有 PgConnection 的场景，必须强制丢弃当前连接并重建，
                // 否则同一坏连接会被反复复用导致持续报错。
                || (state != null && state.startsWith("08"))
                || (e.getMessage() != null
                        && (e.getMessage().contains("This connection has been closed")
                                || e.getMessage().contains("kConnectError")
                                || e.getMessage().contains("Connection refused")
                                || e.getMessage().contains("ERPC_ERROR_CONNECTION_CLOSED")
                                || (PSQLState.INVALID_PASSWORD.getState().equals(state)
                                        && e.getMessage().contains("Invalid expire_time"))))) {
            code = ExceptionCode.CONNECTION_ERROR;
        } else if (e.getMessage() != null
                && e.getMessage().contains("not allowed in readonly mode")) {
            code = ExceptionCode.READ_ONLY;
        } else if ("53000".equalsIgnoreCase(/*INSUFFICIENT_RESOURCES*/ state)
                || (e.getMessage() != null
                        && (e.getMessage().contains("Resource busy")
                                || e.getMessage()
                                        .contains(
                                                "Fail to fetch table group meta from store master")
                                || e.getMessage()
                                        .contains("Get rundown is not allowed in recovering state")
                                || e.getMessage()
                                        .contains("the workers or shards are unhealthy")))) {
            code = ExceptionCode.BUSY;
        } else if (e.getMessage() != null
                && (e.getMessage().contains("too many clients already")
                        || e.getMessage().contains("remaining connection slots are reserved"))) {
            code = ExceptionCode.TOO_MANY_CONNECTIONS;
        } else if (e.getMessage() != null
                && (e.getMessage().contains("too many wal senders already")
                        || e.getMessage().contains("exceeds max_wal_senders"))) {
            code = ExceptionCode.TOO_MANY_WAL_SENDERS;
        } else if (e.getMessage() != null
                && e.getMessage().contains("violates partition constraint")) {
            code = ExceptionCode.CONSTRAINT_VIOLATION;
        } else if (e.getMessage() != null
                && e.getMessage().contains("Could not generate fixed plan")) {
            code = ExceptionCode.NOT_SUPPORTED;
        } else {
            if ("42501".equalsIgnoreCase(state)) {
                code = ExceptionCode.PERMISSION_DENY;
            } else if (PSQLState.SYNTAX_ERROR.getState().equals(state)) {
                code = ExceptionCode.SYNTAX_ERROR;
            } else if (PSQLState.UNDEFINED_COLUMN.getState().equals(state)
                    || "HG000".equals(/*HG_NEED_RETRY*/ state)
                    || (e.getMessage() != null
                            && (e.getMessage().contains("Invalid table id")
                                    || e.getMessage().contains("Refresh meta timeout")
                                    || e.getMessage()
                                            .contains("mismatches the version of the table")
                                    || e.getMessage().contains("could not open relation with OID")
                                    || e.getMessage().contains("replay not finished yet")
                                    || e.getMessage().contains("Table version mismatch")
                                    || e.getMessage()
                                            .contains("fail to execute query Table not found")
                                    || e.getMessage().contains("Table not found, table id")
                                    || e.getMessage()
                                            .contains(
                                                    "Schema version changed during getTableSchema")
                                    || e.getMessage().contains("Table not found, table id")))) {
                // 维表查询过程中, 在transaction中对维表进行RENAME替换, 会报Table not found, table id
                // 大量删分区的时, 查表分区是否存在 会报could not open relation with OID
                // Invalid table id , SQLState = UNDEFINED_TABLE
                // Check META_NOT_MATCH First.
                code = ExceptionCode.META_NOT_MATCH;
            } else if (PSQLState.UNDEFINED_TABLE.getState().equals(state)) {
                code = ExceptionCode.TABLE_NOT_FOUND;
            } else if (PSQLState.INVALID_AUTHORIZATION_SPECIFICATION.getState().equals(state)
                    || PSQLState.INVALID_PASSWORD.getState().equals(state)) {
                code = ExceptionCode.AUTH_FAIL;
            } else if (PSQLState.NOT_NULL_VIOLATION.getState().equals(state)
                    || PSQLState.UNIQUE_VIOLATION.getState().equals(state)
                    || PSQLState.CHECK_VIOLATION.getState().equals(state)) {
                code = ExceptionCode.CONSTRAINT_VIOLATION;
            } else if (PSQLState.DATA_ERROR.getState().equals(state)
                    || PSQLState.STRING_DATA_RIGHT_TRUNCATION.getState().equals(state)
                    || PSQLState.NUMERIC_VALUE_OUT_OF_RANGE.getState().equals(state)
                    || PSQLState.BAD_DATETIME_FORMAT.getState().equals(state)
                    || PSQLState.DATETIME_OVERFLOW.getState().equals(state)
                    || PSQLState.INVALID_PARAMETER_VALUE.getState().equals(state)
                    || PSQLState.NUMERIC_CONSTANT_OUT_OF_RANGE.getState().equals(state)
                    || PSQLState.INVALID_PARAMETER_TYPE.getState().equals(state)
                    || "22P02".equals(/*INVALID_TEXT_REPRESENTATION*/ state)
                    || "22021".equals(/*CHARACTER_NOT_IN_REPERTOIRE*/ state)
                    || "22P05".equals(/*UNTRANSLATABLE_CHARACTER*/ state)) {
                code = ExceptionCode.DATA_VALUE_ERROR;
            } else if (PSQLState.DATA_TYPE_MISMATCH.getState().equals(state)
                    || PSQLState.INVALID_NAME.getState().equals(state)
                    || PSQLState.DATATYPE_MISMATCH.getState().equals(state)
                    || PSQLState.CANNOT_COERCE.getState().equals(state)) {
                code = ExceptionCode.DATA_TYPE_ERROR;
            }
        }
        String msg = "";
        if (backendPid != -1) {
            msg += " [BackendPid:" + backendPid + "]";
        }
        if (code == ExceptionCode.UNKNOWN_ERROR) {
            msg += "[UNKNOW:" + state + "]" + e.getMessage();
        } else {
            // 报错返回CODE NAME，方便用户理解
            msg += "[" + code.name() + "]" + e.getMessage();
        }
        return new HoloClientException(code, msg, e);
    }
}
