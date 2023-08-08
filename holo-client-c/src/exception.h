#ifndef _EXCEPTION_H_
#define _EXCEPTION_H_

#include <libpq-fe.h>
#include <stdbool.h>

/*
 * SqlState in Postgres, copy from errcodes.txt
 */
#define ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION "08001"
#define ERRCODE_CONNECTION_DOES_NOT_EXIST "08003"
#define ERRCODE_SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION "08004"
#define ERRCODE_CONNECTION_FAILURE "08006"
#define ERRCODE_TRANSACTION_RESOLUTION_UNKNOWN "08007"
#define ERRCODE_INSUFFICIENT_PRIVILEGE "42501"
#define ERRCODE_SYNTAX_ERROR "42601"
#define ERRCODE_UNDEFINED_COLUMN "42703"
#define ERRCODE_UNDEFINED_TABLE "42P01"
#define ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION "28000"
#define ERRCODE_INVALID_PASSWORD "28P01"
#define ERRCODE_NOT_NULL_VIOLATION "23502"
#define ERRCODE_UNIQUE_VIOLATION "23505"
#define ERRCODE_CHECK_VIOLATION "23514"
#define ERRCODE_DATA_EXCEPTION "22000"
#define ERRCODE_STRING_DATA_RIGHT_TRUNCATION "22001"
#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE "22003"
#define ERRCODE_INVALID_DATETIME_FORMAT "22007"
#define ERRCODE_DATETIME_VALUE_OUT_OF_RANGE "22008"
#define ERRCODE_INVALID_PARAMETER_VALUE "22023"
#define ERRCODE_INVALID_NAME "42602"
#define ERRCODE_DATATYPE_MISMATCH "42804"
#define ERRCODE_CANNOT_COERCE "42846"
#define ERRCODE_INTERNAL_ERROR "XX000"

typedef enum _HoloErrCode {
    INVALID_CONFIG = 1,
    INVALID_REQUEST = 2,
    GENERATOR_PARAMS_ERROR = 51,
	CONNECTION_ERROR = 100,
	READ_ONLY = 103,
    META_NOT_MATCH = 201,
    TIMEOUT = 250,
    BUSY = 251,
    TOO_MANY_CONNECTIONS = 106,
    AUTH_FAIL = 101,
    ALREADY_CLOSE = 102,
    PERMISSION_DENY = 104,
    SYNTAX_ERROR = 105,
    TOO_MANY_WAL_SENDERS = 107,
    INTERNAL_ERROR = 300,
    INTERRUPTED = 301,
    NOT_SUPPORTED = 302,
    TABLE_NOT_FOUND = 200,
    CONSTRAINT_VIOLATION = 202,
    DATA_TYPE_ERROR = 203,
    DATA_VALUE_ERROR = 204,
    UNKNOWN_ERROR = 500
} HoloErrCode;

bool is_dirty_data_error(HoloErrCode);
HoloErrCode get_errcode_from_pg_res(PGresult*);

#endif