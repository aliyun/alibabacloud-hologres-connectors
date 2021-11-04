#include "connection_holder.h"
#include "logger.h"
#include "table_schema.h"
#include "table_schema_private.h"
#include "utils.h"
#include "unistd.h"
#include "ilist.h"
#include "sql_builder.h"
#include "holo_client.h"

typedef struct _PrepareItem {
    dlist_node list_node;
    const char* command;
} PrepareItem;

PrepareItem* create_prepare_item(const char* command) {
    PrepareItem* item = MALLOC(1, PrepareItem);
    item->command = command;
    return item;
}

void clear_prepare_list(ConnectionHolder* connHolder){
    dlist_mutable_iter miter;
    PrepareItem* prepareItem;
    dlist_foreach_modify(miter, &(connHolder->prepareList)) {
        prepareItem = dlist_container(PrepareItem, list_node, miter.cur);
        dlist_delete(miter.cur);
        FREE(prepareItem);
    }
    connHolder->prepareCount = 0;
}

typedef struct _InsertSqlItem {
    dlist_node list_node;
    Batch* batchWithoutRecords;
    SqlCache* sqlCache;
} InsertSqlItem;

typedef struct _GetSqlItem {
    dlist_node list_node;
    int numRecords;
    SqlCache* sqlCache;
    TableSchema* schema;
} GetSqlItem;

InsertSqlItem* create_sql_cache_item(Batch* batch, int nRecords, SqlCache* sqlCache) {
    InsertSqlItem* item = MALLOC(1, InsertSqlItem);
    item->batchWithoutRecords = holo_client_clone_batch_without_records(batch);
    if (nRecords != 0) item->batchWithoutRecords->nRecords = nRecords;
    item->sqlCache = sqlCache;
    return item;
}

GetSqlItem* create_get_sql_cache_item(int nRecords, SqlCache* sqlCache, TableSchema* schema) {
    GetSqlItem* item = MALLOC(1, GetSqlItem);
    item->numRecords = nRecords;
    item->sqlCache = sqlCache;
    item->schema = schema;
    return item;
}

void destroy_sql_cache(ConnectionHolder* connHolder){
    connHolder->insertSqlCount = 0;
    dlist_mutable_iter miter;
    InsertSqlItem* insertSqlItem;
    dlist_foreach_modify(miter, &(connHolder->sqlCache)) {
        insertSqlItem = dlist_container(InsertSqlItem, list_node, miter.cur);
        holo_client_destroy_batch(insertSqlItem->batchWithoutRecords);
        FREE(insertSqlItem->sqlCache->command);
        FREE(insertSqlItem->sqlCache->paramTypes);  //释放了整个mPool
        // FREE(insertSqlItem->sqlCache->paramFormats);
        // FREE(insertSqlItem->sqlCache->paramLengths);
        FREE(insertSqlItem->sqlCache);
        dlist_delete(miter.cur);
        FREE(insertSqlItem);
    }
}

void destroy_get_sql_cache(ConnectionHolder* connHolder) {
    connHolder->getSqlCount = 0;
    dlist_mutable_iter miter;
    GetSqlItem* getSqlItem;
    dlist_foreach_modify(miter, &(connHolder->getSqlCache)) {
        getSqlItem = dlist_container(GetSqlItem, list_node, miter.cur);
        FREE(getSqlItem->sqlCache->command);
        FREE(getSqlItem->sqlCache->paramTypes);  //释放了整个mPool
        // FREE(insertSqlItem->sqlCache->paramFormats);
        // FREE(insertSqlItem->sqlCache->paramLengths);
        FREE(getSqlItem->sqlCache);
        dlist_delete(miter.cur);
        FREE(getSqlItem);
    }
}

ConnectionHolder* holo_client_new_connection_holder(HoloConfig config){
    ConnectionHolder* connHolder = MALLOC(1, ConnectionHolder);
    connHolder->conn = NULL;
    connHolder->connInfo = config.connInfo;
    connHolder->retryCount = config.retryCount;
    connHolder->retrySleepStepMs = config.retrySleepStepMs;
    connHolder->retrySleepInitMs = config.retrySleepInitMs;
    connHolder->lastActiveTs = get_time_usec();
    connHolder->prepareCount = 0;
    dlist_init(&(connHolder->prepareList));
    connHolder->insertSqlCount = 0;
    dlist_init(&(connHolder->sqlCache));
    connHolder->getSqlCount = 0;
    dlist_init(&(connHolder->getSqlCache));
    connHolder->handleExceptionByUser = config.exceptionHandler;
    return connHolder;
}

extern PGresult *connection_holder_exec_params(ConnectionHolder *connHolder,
	const char *command,
	int nParams,
	const Oid *paramTypes,
	const char *const *paramValues,
	const int *paramLengths,
	const int *paramFormats,
	int resultFormat
){
    PGresult *res;
    char* stmtName;
    bool prepared = false;
    dlist_mutable_iter miter;
    PrepareItem* prepareItem;
    const char* preparedCommand;
    int preparedCount = 0;
    dlist_foreach_modify(miter, &(connHolder->prepareList)) {
        prepareItem = dlist_container(PrepareItem, list_node, miter.cur);
        preparedCommand = prepareItem->command;
        if (strcmp(preparedCommand, command) == 0){
            prepared = true;
            stmtName = itoa(preparedCount);
            break;
        }
        preparedCount++;
    }
    if (!prepared){
        stmtName = itoa(connHolder->prepareCount++);
        res = PQprepare(connHolder->conn, stmtName, command, nParams, paramTypes);
        if (PQresultStatus(res) != PGRES_COMMAND_OK){
            LOG_WARN("Prepare command failed.");
            return res;
        }
        //LOG_DEBUG("Prepared successfully. Statement name: %s", stmtName);
        dlist_push_tail(&(connHolder->prepareList), &(create_prepare_item(command)->list_node));
        PQclear(res);
    }
    long before = current_time_ms();
    res = PQexecPrepared(connHolder->conn, stmtName, nParams, paramValues, paramLengths, paramFormats, resultFormat);
    metrics_histogram_update(connHolder->metrics->execPreparedTime, current_time_ms() - before);
    if (PQresultStatus(res) != PGRES_COMMAND_OK && PQresultStatus(res) != PGRES_TUPLES_OK){
        LOG_WARN("Exec prepared command failed. Statement name: %s", stmtName);
    }
    //else LOG_DEBUG("Exec prepared successfully. Statement name: %s", stmtName);
    FREE(stmtName);
    return res;
}

bool conneciton_holder_connect_or_reset_db(ConnectionHolder* connHolder){
    if (connHolder->conn != NULL && PQstatus(connHolder->conn) == CONNECTION_OK) return true;
    if (connHolder->conn == NULL) {
        LOG_INFO("Connection creating...");
        connHolder->conn = PQconnectdb(connHolder->connInfo);
        clear_prepare_list(connHolder);
        
        if (PQstatus(connHolder->conn) != CONNECTION_OK) return false;
    }
    if (connHolder->conn != NULL && PQstatus(connHolder->conn) != CONNECTION_OK){
        PQreset(connHolder->conn);
        clear_prepare_list(connHolder);
    }
    if (connHolder->conn == NULL || PQstatus(connHolder->conn) != CONNECTION_OK) return false;
    PGresult* res;
    res = PQexec(connHolder->conn, "set hg_experimental_enable_fixed_dispatcher = on");
    PQclear(res);
    res = PQexec(connHolder->conn, "set hg_experimental_enable_fixed_dispatcher_for_multi_values = on");
    PQclear(res);
    res = PQexec(connHolder->conn, "set hg_experimental_enable_fixed_dispatcher_for_update = on");
    PQclear(res);
    res = PQexec(connHolder->conn, "set hg_experimental_enable_fixed_dispatcher_for_delete = on");
    PQclear(res);
    return true;
}

bool need_retry(char* errorMessage){
    if (strstr(errorMessage, "onnection") != NULL) {
        LOG_WARN("Connection Error.");
        return true;
    }
    if (strstr(errorMessage, "nvalid table id") != NULL ||
    strstr(errorMessage, "efresh meta timeout") != NULL ||
    strstr(errorMessage, "ismatches the version of the table") != NULL){
        LOG_WARN("META NOT MATCH. Retrying...");
        return true;
    }
    if (strstr(errorMessage, "ot allowed in readonly mode") != NULL){
        LOG_WARN("READ ONLY. Retrying...");
        return true;
    }
    if (strstr(errorMessage, "does not exist") != NULL && strstr(errorMessage, "relation") != NULL){
        LOG_WARN("TABLE NOT FOUND. Retrying...");
        return true;
    }
    return false;
}

extern PGresult *connection_holder_exec_params_with_retry(ConnectionHolder *connHolder,
	const char *command,
	int nParams,
	const Oid *paramTypes,
	const char *const *paramValues,
	const int *paramLengths,
	const int *paramFormats,
	int resultFormat
){
    PGresult *res = NULL;
    for (int i = 0;i < connHolder->retryCount; ++i){
        bool needRetry = false;
        if (!conneciton_holder_connect_or_reset_db(connHolder)){
            LOG_WARN("Connection Error.");
            LOG_WARN("%s", PQerrorMessage(connHolder->conn));
            if (strstr(PQerrorMessage(connHolder->conn), "Invalid username") != NULL || strstr(PQerrorMessage(connHolder->conn), "incorrect password") != NULL) {
                LOG_ERROR("AUTH FAIL. No retry.");
                connection_holder_close_conn(connHolder);
                return NULL;  //建立连接失败则返回NULL
            }
            needRetry = true;
        }
        else {
            if (res != NULL) PQclear(res);  //只保留最后一个res
            res = connection_holder_exec_params(connHolder, command, nParams, paramTypes, paramValues, paramLengths, paramFormats, resultFormat);
            if (PQresultStatus(res) == PGRES_COMMAND_OK || PQresultStatus(res) == PGRES_TUPLES_OK) return res;
            needRetry = need_retry(PQresultErrorMessage(res));
        }
        if (!needRetry) break;
        long long sleepTime = connHolder->retrySleepStepMs * i + connHolder->retrySleepInitMs;
        LOG_WARN("Execute sql failed, try again [%d/%d], sleepMs = %lldms", i + 1, connHolder->retryCount, sleepTime);
        if (i + 1 < connHolder->retryCount) usleep(sleepTime * 1000);
    }
    if (res != NULL) LOG_ERROR("%s", PQresultErrorMessage(res));
    return res;
}

ActionStatus connection_holder_do_action(ConnectionHolder* connHolder, Action* action, ActionHandler handle){
    return handle(connHolder, action);
}

void connection_holder_close_conn(ConnectionHolder* connHolder){
    clear_prepare_list(connHolder);
    destroy_sql_cache(connHolder);
    destroy_get_sql_cache(connHolder);
    if (connHolder->conn == NULL) return;
    PQfinish(connHolder->conn);
    connHolder->conn = NULL;
    LOG_INFO("Connection closed.");
}

SqlCache* connection_holder_get_or_create_sql_cache_with_batch(ConnectionHolder* connHolder, Batch* batch, int nRecords){
    SqlCache* sqlCache;
    bool cached = false;
    dlist_mutable_iter miter;
    InsertSqlItem* insertSqlItem;
    Batch* cachedBatch;

    dlist_foreach_modify(miter, &(connHolder->sqlCache)) {
        insertSqlItem = dlist_container(InsertSqlItem, list_node, miter.cur);
        cachedBatch = insertSqlItem->batchWithoutRecords;
        if (batch_matches(cachedBatch, batch, nRecords)){
            cached = true;
            sqlCache = insertSqlItem->sqlCache;
            //LOG_DEBUG("Cached sql found.");
            break;
        }
    }
    if (!cached){
        //LOG_DEBUG("Cached sql not found.");
        sqlCache = MALLOC(1, SqlCache);
        switch (batch->mode)
        {
        case PUT:
            sqlCache->command = build_insert_sql_with_batch(batch, nRecords);
            break;
        case DELETE:
            sqlCache->command = build_delete_sql_with_batch(batch, nRecords);
            break;
        }
        if (nRecords == 0) nRecords = batch->nRecords;
        int nParams = nRecords * batch->nValues;
        void* mPool = MALLOC(nParams * (sizeof(Oid) + 2 * sizeof(int)), char);
        sqlCache->paramTypes = mPool;
        sqlCache->paramFormats = mPool + nParams * sizeof(Oid);
        sqlCache->paramLengths = mPool + nParams * (sizeof(Oid) + sizeof(int));
        int count = -1;
        for (int i = 0;i < nRecords;i++){
            for (int j = 0;j < batch->schema->nColumns;j++){
                if (!batch->valuesSet[j]) continue;
                sqlCache->paramTypes[++count] = batch->schema->columns[j].type;
                sqlCache->paramFormats[count] = batch->valueFormats[j];
                sqlCache->paramLengths[count] = batch->valueLengths[j];
            }
        }
        dlist_push_tail(&(connHolder->sqlCache), &(create_sql_cache_item(batch, nRecords, sqlCache)->list_node));
    }
    return sqlCache;
}

SqlCache* connection_holder_get_or_create_get_sql_cache(ConnectionHolder* connHolder, TableSchema* schema, int nRecords) {
    SqlCache* sqlCache;
    bool cached = false;
    dlist_mutable_iter miter;
    GetSqlItem* getSqlItem;

    dlist_foreach_modify(miter, &(connHolder->getSqlCache)) {
        getSqlItem = dlist_container(GetSqlItem, list_node, miter.cur);
        if (getSqlItem->schema == schema && getSqlItem->numRecords == nRecords){
            cached = true;
            sqlCache = getSqlItem->sqlCache;
            //LOG_DEBUG("Cached sql found.");
            break;
        }
    }
    if (!cached){
        //LOG_DEBUG("Cached sql not found.");
        sqlCache = MALLOC(1, SqlCache);
        sqlCache->command = build_get_sql(schema, nRecords);
        int nParams = nRecords * schema->nPrimaryKeys;
        void* mPool = MALLOC(nParams * (sizeof(Oid) + 2 * sizeof(int)), char);
        sqlCache->paramTypes = mPool;
        sqlCache->paramFormats = mPool + nParams * sizeof(Oid);
        sqlCache->paramLengths = mPool + nParams * (sizeof(Oid) + sizeof(int));
        int count = -1;
        for (int i = 0;i < nRecords;i++){
            for (int j = 0;j < schema->nPrimaryKeys;j++){
                int col = schema->primaryKeys[j];
                sqlCache->paramTypes[++count] = schema->columns[col].type;
            }
        }
        dlist_push_tail(&(connHolder->getSqlCache), &(create_get_sql_cache_item(nRecords, sqlCache, schema)->list_node));
    }
    return sqlCache;
}

extern void connection_holder_exec_func_with_retry(ConnectionHolder* connHolder, SqlFunction sqlFunction, void* arg, void** retAddr) {
    for (int i = 0;i < connHolder->retryCount; ++i){
        bool needRetry = false;
        if (!conneciton_holder_connect_or_reset_db(connHolder)){
            LOG_WARN("Connection Error.");
            LOG_WARN("%s", PQerrorMessage(connHolder->conn));
            if (strstr(PQerrorMessage(connHolder->conn), "Invalid username") != NULL || strstr(PQerrorMessage(connHolder->conn), "incorrect password") != NULL) {
                LOG_ERROR("AUTH FAIL. No retry.");
                connection_holder_close_conn(connHolder);
                *retAddr = NULL;
                return;  //建立连接失败则返回NULL
            }
            needRetry = true;
        }
        else {
            *retAddr = sqlFunction(connHolder->conn, arg);
        }
        if (!needRetry) break;
        long long sleepTime = connHolder->retrySleepStepMs * i + connHolder->retrySleepInitMs;
        LOG_WARN("Execute sql failed, try again [%d/%d], sleepMs = %lldms", i + 1, connHolder->retryCount, sleepTime);
        if (i + 1 < connHolder->retryCount) usleep(sleepTime * 1000);
    }
    return;
}
