#ifndef _DEFS_H_
#define _DEFS_H_

#ifdef  __cplusplus
# define __HOLO_CLIENT_BEGIN_DECLS  extern "C" {
# define __HOLO_CLIENT_END_DECLS    }
#else
# define __HOLO_CLIENT_BEGIN_DECLS
# define __HOLO_CLIENT_END_DECLS
#endif

/*
 * Oids of holo-client-c support types
 * copy from pg_type_d.h
 */
#define HOLO_TYPE_BOOL          16
#define HOLO_TYPE_INT2          21
#define HOLO_TYPE_INT4          23
#define HOLO_TYPE_INT8          20
#define HOLO_TYPE_FLOAT4        700
#define HOLO_TYPE_FLOAT8        701
#define HOLO_TYPE_NUMERIC       1700
#define HOLO_TYPE_BYTEA         17
#define HOLO_TYPE_CHAR          18
#define HOLO_TYPE_TEXT          25
#define HOLO_TYPE_VARCHAR       1043
#define HOLO_TYPE_DATE          1082
#define HOLO_TYPE_TIMESTAMP     1114
#define HOLO_TYPE_TIMESTAMPTZ   1184
#define HOLO_TYPE_JSON          114
#define HOLO_TYPE_JSONB         3802
#define HOLO_TYPE_BOOL_ARRAY    1000
#define HOLO_TYPE_INT2_ARRAY    1005
#define HOLO_TYPE_INT4_ARRAY    1007
#define HOLO_TYPE_INT8_ARRAY    1016
#define HOLO_TYPE_FLOAT4_ARRAY  1021
#define HOLO_TYPE_FLOAT8_ARRAY  1022
#define HOLO_TYPE_TEXT_ARRAY    1009
#define HOLO_TYPE_NUMERIC_ARRAY 1231
#define HOLO_TYPE_VARCHAR_ARRAY 1015
#define HOLO_TYPE_BYTEA_ARRAY   1001
#define HOLO_TYPE_CHAR_ARRAY    1002
#define HOLO_TYPE_JSON_ARRAY    199
#define HOLO_TYPE_JSONB_ARRAY   3807
#define HOLO_TYPE_DATE_ARRAY    1182
#define HOLO_TYPE_TIMESTAMP_ARRAY   1115
#define HOLO_TYPE_TIMESTAMPTZ_ARRAY 1185
#endif