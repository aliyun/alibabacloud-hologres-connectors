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
		if (PSQLState.isConnectionError(e.getSQLState()) || (e.getMessage() != null && (e.getMessage().contains("This connection has been closed")
				|| e.getMessage().contains("kConnectError") || e.getMessage().contains("Connection refused") || e.getMessage().contains("ERPC_ERROR_CONNECTION_CLOSED")))) {
			code = ExceptionCode.CONNECTION_ERROR;
		} else if (e.getMessage() != null && e.getMessage().contains("not allowed in readonly mode")) {
			code = ExceptionCode.READ_ONLY;
		} else if (e.getMessage() != null && e.getMessage().contains("Resource busy")) {
			code = ExceptionCode.BUSY;
		} else if (e.getMessage() != null && (e.getMessage().contains("too many clients already") || e.getMessage().contains("remaining connection slots are reserved"))) {
			code = ExceptionCode.TOO_MANY_CONNECTIONS;
		} else if (e.getMessage() != null && (e.getMessage().contains("too many wal senders already") || e.getMessage().contains("exceeds max_wal_senders"))) {
			code = ExceptionCode.TOO_MANY_WAL_SENDERS;
		} else if (e.getMessage() != null && e.getMessage().contains("violates partition constraint")) {
			code = ExceptionCode.CONSTRAINT_VIOLATION;
		} else if (e.getMessage() != null && e.getMessage().contains("Could not generate fixed plan")) {
			code = ExceptionCode.NOT_SUPPORTED;
		} else {
			String state = e.getSQLState();
			if ("42501".equalsIgnoreCase(state)) {
				code = ExceptionCode.PERMISSION_DENY;
			} else if (PSQLState.SYNTAX_ERROR.getState().equals(state)) {
				code = ExceptionCode.SYNTAX_ERROR;
			} else if (PSQLState.UNDEFINED_COLUMN.getState().equals(state) || (e.getMessage() != null && (e.getMessage().contains("Invalid table id") || e.getMessage().contains("Refresh meta timeout") ||
					e.getMessage().contains("mismatches the version of the table") || e.getMessage().contains("could not open relation with OID") || e.getMessage().contains("replay not finished yet")))) {
				//大量删分区的时, 查表分区是否存在 会报could not open relation with OID
				//Invalid table id , SQLState = UNDEFINED_TABLE
				//Check META_NOT_MATCH First.
				code = ExceptionCode.META_NOT_MATCH;
			} else if (PSQLState.UNDEFINED_TABLE.getState().equals(state)) {
				code = ExceptionCode.TABLE_NOT_FOUND;
			} else if (PSQLState.INVALID_AUTHORIZATION_SPECIFICATION.getState().equals(state) || PSQLState.INVALID_PASSWORD.getState().equals(state)) {
				code = ExceptionCode.AUTH_FAIL;
			} else if (PSQLState.NOT_NULL_VIOLATION.getState().equals(state) || PSQLState.UNIQUE_VIOLATION.getState().equals(state) || PSQLState.CHECK_VIOLATION.getState().equals(state)) {
				code = ExceptionCode.CONSTRAINT_VIOLATION;
			} else if (PSQLState.DATA_ERROR.getState().equals(state) || PSQLState.STRING_DATA_RIGHT_TRUNCATION.getState().equals(state) || PSQLState.NUMERIC_VALUE_OUT_OF_RANGE.getState().equals(state) || PSQLState.BAD_DATETIME_FORMAT.getState().equals(state)
					|| PSQLState.DATETIME_OVERFLOW.getState().equals(state) || PSQLState.INVALID_PARAMETER_VALUE.getState().equals(state) || PSQLState.NUMERIC_CONSTANT_OUT_OF_RANGE.getState().equals(state)) {
				code = ExceptionCode.DATA_VALUE_ERROR;
			} else if (PSQLState.DATA_TYPE_MISMATCH.getState().equals(state) || PSQLState.INVALID_NAME.getState().equals(state) || PSQLState.DATATYPE_MISMATCH.getState().equals(state) || PSQLState.CANNOT_COERCE.getState().equals(state)) {
				code = ExceptionCode.DATA_TYPE_ERROR;
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
