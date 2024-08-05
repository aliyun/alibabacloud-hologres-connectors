#include "exception.h"
#include "utils.h"
#include "logger_private.h"

bool is_dirty_data_error(HoloErrCode errCode) {
    switch (errCode)
    {
    // case TABLE_NOT_FOUND:
    case CONSTRAINT_VIOLATION:
    case DATA_TYPE_ERROR:
    case DATA_VALUE_ERROR:
        return true;
    default:
        break;
    }
    return false;
}

HoloErrCode get_errcode_from_pg_res(PGresult* res) {
    char* errMsg = PQresultErrorMessage(res);
    char* sqlState = PQresultErrorField(res, PG_DIAG_SQLSTATE);

    if (sqlState == NULL) {
        sqlState = "";
    }
    LOG_DEBUG("Get ErrCode from PGResult. sqlState is %s, errMsg is %s", sqlState, errMsg);

    if (strcmp(sqlState, ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION) == 0
        || strcmp(sqlState, ERRCODE_CONNECTION_DOES_NOT_EXIST) == 0
        || strcmp(sqlState, ERRCODE_SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION) == 0
        || strcmp(sqlState, ERRCODE_CONNECTION_FAILURE) == 0
        || strcmp(sqlState, ERRCODE_TRANSACTION_RESOLUTION_UNKNOWN) == 0
        || (errMsg != NULL && (strstr(errMsg, "This connection has been closed")
            || strstr(errMsg, "kConnectError")
            || strstr(errMsg, "Connection refused")
            || strstr(errMsg, "ERPC_ERROR_CONNECTION_CLOSED")))) {
        return CONNECTION_ERROR;
    } else if (errMsg != NULL && strstr(errMsg, "not allowed in readonly mode")) {
        return READ_ONLY;
    } else if (errMsg != NULL && strstr(errMsg, "Resource busy")) {
        return BUSY;
    } else if (errMsg != NULL && (strstr(errMsg, "too many clients already")
                || strstr(errMsg, "remaining connection slots are reserved"))) {
        return TOO_MANY_CONNECTIONS;
    } else if (errMsg != NULL && (strstr(errMsg, "too many wal senders already")
                || strstr(errMsg, "exceeds max_wal_senders"))) {
        return TOO_MANY_WAL_SENDERS;
    } else if (errMsg != NULL && strstr(errMsg, "violates partition constraint")) {
        return CONSTRAINT_VIOLATION;
    } else if (errMsg != NULL && strstr(errMsg, "Could not generate fixed plan")) {
        return NOT_SUPPORTED;
    } else {
        if (strcmp(sqlState, ERRCODE_INSUFFICIENT_PRIVILEGE) == 0) {
            return PERMISSION_DENY;
        } else if (strcmp(sqlState, ERRCODE_SYNTAX_ERROR) == 0) {
            return SYNTAX_ERROR;
        } else if (strcmp(sqlState, ERRCODE_UNDEFINED_COLUMN) == 0
                    || (errMsg != NULL && (strstr(errMsg, "Invalid table id")
                    || strstr(errMsg, "Refresh meta timeout")
                    || strstr(errMsg, "mismatches the version of the table")
                    || strstr(errMsg, "could not open relation with OID")
                    || strstr(errMsg, "replay not finished yet")))) {
            return META_NOT_MATCH;
        } else if (strcmp(sqlState, ERRCODE_UNDEFINED_TABLE) == 0) {
            return TABLE_NOT_FOUND;
        } else if (strcmp(sqlState, ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION) == 0
                    || strcmp(sqlState, ERRCODE_INVALID_PASSWORD) == 0) {
            return AUTH_FAIL;
        } else if (strcmp(sqlState, ERRCODE_NOT_NULL_VIOLATION) == 0
                    || strcmp(sqlState, ERRCODE_UNIQUE_VIOLATION) == 0
                    || strcmp(sqlState, ERRCODE_CHECK_VIOLATION) == 0) {
            return CONSTRAINT_VIOLATION;
        } else if (strcmp(sqlState, ERRCODE_DATA_EXCEPTION) == 0
                    || strcmp(sqlState, ERRCODE_STRING_DATA_RIGHT_TRUNCATION) == 0
                    || strcmp(sqlState, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE) == 0
                    || strcmp(sqlState, ERRCODE_INVALID_DATETIME_FORMAT) == 0
                    || strcmp(sqlState, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) == 0
                    || strcmp(sqlState, ERRCODE_INVALID_PARAMETER_VALUE) == 0) {
            return DATA_VALUE_ERROR;
        } else if (strcmp(sqlState, ERRCODE_INVALID_NAME) == 0
                    || strcmp(sqlState, ERRCODE_DATATYPE_MISMATCH) == 0
                    || strcmp(sqlState, ERRCODE_CANNOT_COERCE) == 0) {
            return DATA_TYPE_ERROR;
        } else if (strcmp(sqlState, ERRCODE_INTERNAL_ERROR) == 0) {
            return INTERNAL_ERROR;
        }
    }
    return UNKNOWN_ERROR;
}

