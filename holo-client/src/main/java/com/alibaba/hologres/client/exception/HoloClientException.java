/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.exception;

import org.postgresql.util.PSQLState;

import java.sql.SQLException;

/**
 * Base class for holo-client exceptions.
 */
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
		ExceptionCode code = ExceptionCode.UNKNOWN_ERROR;
		if (PSQLState.isConnectionError(e.getSQLState()) || (e.getMessage() != null && e.getMessage().contains("This connection has been closed"))) {
			code = ExceptionCode.CONNECTION_ERROR;
		} else if (e.getMessage() != null && e.getMessage().contains("WriteLogRecord is not allowed in readonly mode")) {
			code = ExceptionCode.READ_ONLY;
		} else if (e.getMessage() != null && e.getMessage().contains("Resource busy")) {
			code = ExceptionCode.BUSY;
		} else if (e.getMessage() != null && (e.getMessage().contains("too many clients already") || e.getMessage().contains("remaining connection slots are reserved"))) {
			code = ExceptionCode.TOO_MANY_CONNECTIONS;
		} else if (e.getMessage() != null && (e.getMessage().contains("too many wal senders already") || e.getMessage().contains("exceeds max_wal_senders"))) {
			code = ExceptionCode.TOO_MANY_WAL_SENDERS;
		} else {
			String state = e.getSQLState();
			if ("42501".equalsIgnoreCase(state)) {
				code = ExceptionCode.PERMISSION_DENY;
			} else if (PSQLState.SYNTAX_ERROR.getState().equals(state)) {
				code = ExceptionCode.SYNTAX_ERROR;
			} else if (PSQLState.UNDEFINED_TABLE.getState().equals(state)) {
				code = ExceptionCode.TABLE_NOT_FOUND;
			} else if (PSQLState.UNDEFINED_COLUMN.getState().equals(state) || (e.getMessage() != null && (e.getMessage().contains("Invalid table id") || e.getMessage().contains("Refresh meta timeout") || e.getMessage().contains("mismatches the version of the table")))) {
				code = ExceptionCode.META_NOT_MATCH;
			} else if (PSQLState.INVALID_AUTHORIZATION_SPECIFICATION.getState().equals(state) || PSQLState.INVALID_PASSWORD.getState().equals(state)) {
				code = ExceptionCode.AUTH_FAIL;
			}
		}
		String msg;
		if (code == ExceptionCode.UNKNOWN_ERROR) {
			msg = "[UNKNOW:" + e.getSQLState() + "]" + e.getMessage();
		} else {
			msg = "[" + code.getCode() + "]" + e.getMessage();
		}
		return new HoloClientException(code, msg, e);
	}
}
