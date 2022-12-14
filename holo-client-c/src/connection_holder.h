#ifndef _CONNECTION_HOLDER_H_
#define _CONNECTION_HOLDER_H_

#include "pthread.h"
#include "libpq-fe.h"
#include "utils.h"
#include "holo_config.h"
#include "table_schema.h"
#include "action.h"
#include "ilist.h"
#include "batch.h"
#include "sql_builder.h"
#include "metrics.h"
#include <stdbool.h>
#include "lp_map.h"

typedef struct _ConnectionHolder ConnectionHolder;
typedef ActionStatus (*ActionHandler)(ConnectionHolder*, Action*);

typedef struct _HoloVersion
{
    int majorVersion;
    int minorVersion;
    int fixVersion;
} HoloVersion;

typedef struct _ConnectionHolder {
    PGconn* conn;
    char* connInfo;
    HoloVersion* holoVersion;

    int retryCount;
    long long retrySleepStepMs;
    long long retrySleepInitMs;
    long long lastActiveTs;

    dlist_head prepareList;
    int prepareCount;

    dlist_head sqlCache;
    int insertSqlCount;

    dlist_head getSqlCache;
    int getSqlCount;

    ExceptionHandler handleExceptionByUser;

    MetricsInWorker* metrics;
    LPMap* map;
} ConnectionHolder;

ConnectionHolder* holo_client_new_connection_holder(HoloConfig);
ActionStatus connection_holder_do_action(ConnectionHolder*, Action*, ActionHandler);
extern PGresult* connection_holder_exec_params(ConnectionHolder*, const char*, int, const Oid*, const char* const*, const int*, const int*, int);
extern PGresult* connection_holder_exec_params_with_retry(ConnectionHolder*, const char*, int, const Oid*, const char* const*, const int*, const int*, int);
extern void connection_holder_exec_func_with_retry(ConnectionHolder*, SqlFunction, void*, void**);
void connection_holder_close_conn(ConnectionHolder*);

bool is_batch_support_unnest(ConnectionHolder*, Batch*);
SqlCache* connection_holder_get_or_create_sql_cache_with_batch(ConnectionHolder*, Batch*, int);
SqlCache* connection_holder_get_or_create_get_sql_cache(ConnectionHolder*, TableSchema*, int);
#endif