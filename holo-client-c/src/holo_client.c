#include "holo_client_private.h"
#include "holo_config_private.h"
#include "utils.h"
#include "logger_private.h"
#include "unistd.h"

#define INT32_STRING_MAX_LENGTH 12

int check_mutation(HoloMutation mutation);
int check_partition(HoloClient* client, HoloMutation mutation);
int check_get(HoloGet get);

HoloClient* holo_client_new_client(HoloConfig config) {
    if (!holo_config_is_valid(&config)){
        LOG_ERROR("Holo config is invalid.");
        return NULL;
    }
    log_holo_config(&config);
    HoloClient* client = MALLOC(1, HoloClient);
    HoloWorkerPool* executionPool = NULL;
    client->isEmbeddedPool = false;
    if (config.useFixedFe) {
        client->workerPool = holo_client_new_worker_pool(config, false, config.connectionSizeWhenUseFixedFe);
        client->fixedPool = holo_client_new_worker_pool(config, true, config.threadSize);
        executionPool = client->fixedPool;
        holo_client_start_worker_pool(client->fixedPool);
    } else {
        client->workerPool = holo_client_new_worker_pool(config, false, config.threadSize);
        client->fixedPool = NULL;
        executionPool = client->workerPool;
    }
    client->directCollector = holo_client_new_direct_collector();
    client->mutationCollector = holo_client_new_mutation_collector(executionPool, config);
    client->getCollector = holo_client_new_get_collector(executionPool, config.readBatchSize);
    client->config = config;
    holo_client_start_worker_pool(client->workerPool);
    holo_client_start_watch_direct_collector(client->directCollector, client->workerPool);
    holo_client_start_watch_mutation_collector(client->mutationCollector);
    holo_client_start_watch_get_collector(client->getCollector);
    return client;
}

HoloClient* holo_client_new_client_with_workerpool(HoloConfig config, HoloWorkerPool* pool) {
    if (pool == NULL) {
        LOG_ERROR("Worker pool is NULL.");
        return NULL;
    }
    if (!holo_config_is_valid(&config)){
        LOG_ERROR("Holo config invalid.");
        return NULL;
    }
    log_holo_config(&config);
    if (pool->status == 2) {
        LOG_WARN("worker pool already stopped, will create a new one");
        return holo_client_new_client(config);
    } else if (pool->status == 0) {
        holo_client_start_worker_pool(pool);
    }
    HoloClient* client = MALLOC(1, HoloClient);
    if (config.writeBatchTotalByteSize == -1) config.writeBatchTotalByteSize = config.shardCollectorSize * config.writeBatchByteSize;
    client->isEmbeddedPool = true;
    client->workerPool = pool;
    client->directCollector = holo_client_new_direct_collector();
    client->mutationCollector = holo_client_new_mutation_collector(client->workerPool, config);
    client->getCollector = holo_client_new_get_collector(client->workerPool, config.readBatchSize);
    client->config = config;
    holo_client_start_watch_direct_collector(client->directCollector, client->workerPool);
    holo_client_start_watch_mutation_collector(client->mutationCollector);
    holo_client_start_watch_get_collector(client->getCollector);
    return client;
}

int holo_client_close_client(HoloClient* client) {
    if (client == NULL) {
        LOG_ERROR("Holo client is NULL.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    LOG_INFO("Holo client closing...");
    holo_client_stop_watch_direct_collector(client->directCollector);
    holo_client_destroy_direct_collector(client->directCollector);
    holo_client_stop_watch_mutation_collector(client->mutationCollector);
    holo_client_destroy_mutation_collector(client->mutationCollector);
    holo_client_stop_watch_get_collector(client->getCollector);
    holo_client_destroy_get_collector(client->getCollector);
    if (!client->isEmbeddedPool) {
        holo_client_stop_worker_pool(client->workerPool);
        holo_client_close_worker_pool(client->workerPool);
    }
    if (client->config.useFixedFe) {
        holo_client_stop_worker_pool(client->fixedPool);
        holo_client_close_worker_pool(client->fixedPool);
    }
    FREE(client->config.connInfo);
    FREE(client);
    client = NULL;
    return HOLO_CLIENT_RET_OK;
}

HoloTableSchema* holo_client_get_tableschema_by_tablename(HoloClient* client, HoloTableName name, bool withCache, char** errMsgAddr) {
    LOG_DEBUG("get table schema by table name:%s", name.fullName);
    Meta meta = NULL;
    HoloTableSchema* schema = NULL;
    if(withCache) {
        schema = find_tableschema_in_metacache(client->workerPool->metaCache, name);
        if (schema != NULL) {
            return schema;
        }
    }
    meta = holo_client_new_meta_request(name);
    holo_client_add_meta_request_to_direct_collector(client->directCollector, meta);
    schema = get_future_result(meta->future);
    // 如果errMsgAddr == NULL，说明用户不想要errMsg
    // 如果future->errMsg == NULL，说明worker没有拿到errMsg
    if (errMsgAddr != NULL && meta->future->errMsg != NULL) {
        *errMsgAddr = deep_copy_string(meta->future->errMsg);
    }
    holo_client_destroy_meta_request(meta);
    if (schema != NULL) {
        add_tableschema_to_metacache(client->workerPool->metaCache, schema);
    } else {
        LOG_ERROR("Get table schema failed for table \"%s\".", name.tableName);
        return NULL;
    }
    return schema;
}

HoloTableSchema* holo_client_get_tableschema(HoloClient* client, const char* schemaName, const char* tableName, bool withCache) {
    HoloTableSchema* ret = holo_client_get_tableschema_with_errmsg(client, schemaName, tableName, withCache, NULL);
    return ret;
}

HoloTableSchema* holo_client_get_tableschema_with_errmsg(HoloClient* client, const char* schemaName, const char* tableName, bool withCache, char** errMsgAddr) {
    if (client == NULL) {
        LOG_ERROR("Holo client is NULL.");
        return NULL;
    }
    HoloTableName name;
    if (tableName == NULL){
        LOG_ERROR("Table name in table name not set.");
        return NULL;
    }
    name.tableName = (char*)tableName;
    if (schemaName == NULL){
        name.schemaName = "public";
        LOG_WARN("Schema name in table \"%s\" not set. Set to \"public\".", name.tableName);
    }
    else name.schemaName = (char*)schemaName;

    name.fullName = quote_table_name(name.schemaName, name.tableName);
    HoloTableSchema* ret = holo_client_get_tableschema_by_tablename(client, name, withCache, errMsgAddr);
    FREE(name.fullName);
    return ret;
}

int holo_client_submit_with_attachments(HoloClient* client, HoloMutation mutation, int64_t sequence, int64_t timestamp) {
    if (client == NULL) {
        LOG_ERROR("Holo client is NULL.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (mutation == NULL || mutation->record == NULL) {
        LOG_ERROR("HoloMutation is NULL.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    mutation->record->sequence = sequence;
    mutation->record->timestamp = timestamp;
    return holo_client_submit(client, mutation);
}

int holo_client_submit(HoloClient* client, HoloMutation mutation) {
    int ret = holo_client_submit_with_errmsg(client, mutation, NULL);
    return ret;
}

int holo_client_submit_with_errmsg(HoloClient* client, HoloMutation mutation, char** errMsgAddr){
    if (client == NULL) {
        LOG_ERROR("Holo client is NULL.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    mutation->writeMode = client->config.writeMode;  //所有mutation request的write mode都和config保持一致
    int ret = HOLO_CLIENT_RET_OK;

    ret = check_mutation(mutation);
    if (ret != HOLO_CLIENT_RET_OK) {
        client->config.exceptionHandler(mutation->record, "Submit request failed: check_mutation failed.", client->config.exceptionHandlerParam);
        holo_client_destroy_mutation_request(mutation);
        return ret;
    }

    ret = check_partition(client, mutation);
    if (ret != HOLO_CLIENT_RET_OK) {
        client->config.exceptionHandler(mutation->record, "Submit request failed: check_partition failed.", client->config.exceptionHandlerParam);
        holo_client_destroy_mutation_request(mutation);
        return ret;
    }

    ret = normalize_mutation_request(mutation); //若非UPDATE，则给未设置的列填入默认值或NULL
    if (ret != HOLO_CLIENT_RET_OK) {
        client->config.exceptionHandler(mutation->record, "Submit request failed: normalize_mutation_request failed.", client->config.exceptionHandlerParam);
        holo_client_destroy_mutation_request(mutation);
        return ret;
    }

    mutation->byteSize = sizeof(HoloMutationRequest) + mutation->record->byteSize;
    return holo_client_add_request_to_mutation_collector(client->mutationCollector, mutation, errMsgAddr);
}

void* holo_client_sql(HoloClient* client, Sql sql) {
    void* retVal = NULL;
    holo_client_add_sql_request_to_direct_collector(client->directCollector, sql);
    retVal = get_future_result(sql->future);
    holo_client_destroy_sql_request(sql);
    return retVal;
}

int holo_client_flush_client(HoloClient* client) {
    int ret = holo_client_flush_client_with_errmsg(client, NULL);
    return ret;
}

int holo_client_flush_client_with_errmsg(HoloClient* client, char** errMsgAddr) {
    if (client == NULL) {
        LOG_ERROR("Holo client is NULL.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    return holo_client_flush_mutation_collector(client->mutationCollector, errMsgAddr);
}

int holo_client_get(HoloClient* client, HoloGet get) {
    if (client == NULL) {
        LOG_ERROR("Holo client is NULL.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (get == NULL) {
        LOG_ERROR("HoloGet is NULL.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    int ret = check_get(get);
    if(ret != HOLO_CLIENT_RET_OK) {
        // client->config.exceptionHandler(get->record, "Submit get request failed.", client->config.exceptionHandlerParam);
        // holo_client_destroy_get_request(get);
        return ret;
    }
    holo_client_add_request_to_get_collector(client->getCollector, get);
    return HOLO_CLIENT_RET_OK;
}

HoloRecord* holo_client_get_record(const HoloGet get) {
    if (get == NULL) {
        LOG_ERROR("HoloGet is NULL.");
        return NULL;
    }
    if (!get->submitted) {
        LOG_ERROR("Not yet submitted!");
        return NULL;
    }
    return (HoloRecord*)get_future_result(get->future);
}

char* holo_client_get_record_val(const HoloRecord* record, int colIndex) {
    if(colIndex < 0 || colIndex >= record->schema->nColumns) {
        LOG_ERROR("Invalid column index %d for table \"%s\"", colIndex, record->schema->tableName->tableName);
        return NULL;
    }
    return record->values[colIndex];
}

int check_mutation(HoloMutation mutation) {
    HoloTableSchema* schema = NULL;
    int index;
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation request cannot be null!");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    schema = mutation->record->schema;
    for (int i = 0; i < schema->nPrimaryKeys; i++) {
        index = schema->primaryKeys[i];
        if (!mutation->record->valuesSet[index] || (mutation->record->values[index] == NULL && schema->columns[index].defaultValue == NULL)) {
            LOG_ERROR("HoloMutation request primary key cannot be null for table \"%s\"!", mutation->record->schema->tableName->tableName);
            return HOLO_CLIENT_PK_IS_NULL;
        }
    }
    index = schema->partitionColumn;
    if (index > -1 && (!mutation->record->valuesSet[index] || mutation->record->values[index] == NULL)) {
        LOG_ERROR("HoloMutation request partition key cannot be null for table \"%s\"!", mutation->record->schema->tableName->tableName);
        return HOLO_CLIENT_PARTITION_IS_NULL;
    }
    if (mutation->mode == DELETE && schema->nPrimaryKeys == 0) {
        LOG_ERROR("Delete request table must have primary key for table \"%s\"!", mutation->record->schema->tableName->tableName);
        return HOLO_CLIENT_DELETE_NO_PK_TABLE;
    }
    return HOLO_CLIENT_RET_OK;
}

static int get_int_partition_value_string(char* intValueStr, const char* intPartitionValue) {
    char* copyValue = MALLOC(4, char);
    memcpy(copyValue, intPartitionValue, 4);
    endian_swap(copyValue, 4);
    snprintf(intValueStr, INT32_STRING_MAX_LENGTH, "%d", *(int32_t*)copyValue);
    endian_swap(copyValue, 4);
    FREE(copyValue);
    return HOLO_CLIENT_RET_OK;
}

void* find_partition_table_name(PGconn* conn, void* arg) {
    HoloRecord* record = arg;
    const char* findPartitionSql = "with inh as (SELECT i.inhrelid, i.inhparent FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_catalog.pg_inherits i on c.oid=i.inhparent where n.nspname = $1 and c.relname= $2) select n.nspname as schema_name, c.relname as table_name, inh.inhrelid, inh.inhparent, p.partstrat, pg_get_expr(c.relpartbound, c.oid, true) as part_expr, p.partdefid, p.partnatts, p.partattrs from inh join pg_catalog.pg_class c on inh.inhrelid = c.oid join pg_catalog.pg_namespace n on c.relnamespace = n.oid join pg_partitioned_table p on p.partrelid = inh.inhparent where pg_get_expr(c.relpartbound, c.oid, true) = $3 limit 1";
    const char* schemaName = record->schema->tableName->schemaName;
    const char* tableName = record->schema->tableName->tableName;
    char* partitionInfo = NULL;
    PGresult* res = NULL;
    HoloTableName* name = NULL;
    char intValStr[INT32_STRING_MAX_LENGTH];
    if (record->valueFormats[record->schema->partitionColumn] == 1) {
        //int
        get_int_partition_value_string(intValStr, record->values[record->schema->partitionColumn]);
        int length = strlen(intValStr) + 17;
        partitionInfo = MALLOC(length, char);
        snprintf(partitionInfo, length, "FOR VALUES IN (%s)", intValStr);
    } else {
        //text or varchar
        int length = strlen(record->values[record->schema->partitionColumn]) + 19;
        partitionInfo = MALLOC(length, char);
        snprintf(partitionInfo, length, "FOR VALUES IN ('%s')", record->values[record->schema->partitionColumn]);
    }
    const char* params[3] = {schemaName, tableName, partitionInfo};
    res = PQexecParams(conn, findPartitionSql, 3, NULL, params, NULL, NULL, 0);
    if (PQntuples(res) == 0) {
        PQclear(res);
        FREE(partitionInfo);
        return NULL;
    }
    name = MALLOC(1, HoloTableName);
    name->schemaName = deep_copy_string(PQgetvalue(res, 0, 0));
    name->tableName = deep_copy_string(PQgetvalue(res, 0, 1));
    name->fullName = NULL;
    PQclear(res);
    FREE(partitionInfo);
    return name;
}

void* retry_create_partition_child_table(PGconn* conn, void* arg) {
    int retry = 0;
    HoloRecord* record = arg;
    HoloTableSchema* parentSchema = record->schema;
    char* partitionValue = NULL;
    char intValStr[INT32_STRING_MAX_LENGTH];
    if (record->valueFormats[parentSchema->partitionColumn] == 1) {
        get_int_partition_value_string(intValStr, record->values[record->schema->partitionColumn]);
        partitionValue = intValStr;
    } else {
        partitionValue = record->values[record->schema->partitionColumn];
    }
    int maxTableNameLength = strlen(parentSchema->tableName->tableName) + strlen(partitionValue) + 22;
    int maxSqlLength = 2 * strlen(parentSchema->tableName->schemaName) + strlen(parentSchema->tableName->tableName) + maxTableNameLength + strlen(partitionValue) + 57;
    char* tableName = MALLOC(maxTableNameLength, char);
    char* sql = MALLOC(maxSqlLength, char);
    Oid type = parentSchema->columns[parentSchema->partitionColumn].type;
    PGresult* res = NULL;
    char* errorMsg = NULL;
    bool continueRetry = true;
    snprintf(tableName, maxTableNameLength, "%s_%s", parentSchema->tableName->tableName, partitionValue);
    while(continueRetry) {
        if (retry > 0) {
            snprintf(tableName, maxTableNameLength, "%s_%s_%ld", parentSchema->tableName->tableName, partitionValue, current_time_ms());
        }
        // 这里的表名要用quote_identifier包一下，value要用quote_literal_cstr包一下
        char* childTableName = quote_table_name(parentSchema->tableName->schemaName, tableName);
        char* parentTableName = quote_table_name(parentSchema->tableName->schemaName, parentSchema->tableName->tableName);
        char* quotedPartitionValue = quote_literal_cstr(partitionValue);
        if (type == HOLO_TYPE_TEXT || type == HOLO_TYPE_VARCHAR) {
            //text or varchar
            snprintf(sql, maxSqlLength, "CREATE TABLE %s PARTITION OF %s FOR VALUES IN (%s)", childTableName, parentTableName, quotedPartitionValue);
        } else {
            snprintf(sql, maxSqlLength, "CREATE TABLE %s PARTITION OF %s FOR VALUES IN (%s)", childTableName, parentTableName, partitionValue);
        }
        FREE(childTableName);
        FREE(parentTableName);
        FREE(quotedPartitionValue);
        res = PQexec(conn, sql);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            errorMsg = PQresultErrorMessage(res);
            if (strstr(errorMsg, "already exists") != NULL || retry < 20) {
                //try with another table name
                sleep(3);
            } else {
                //not retry 
                FREE(tableName);
                tableName = NULL;
                continueRetry = false;
            }
        } else {
            // create table success
            continueRetry = false;
        }
        PQclear(res);
        retry++;
    }
    FREE(sql);
    return tableName;
}

int check_partition_schema(HoloTableSchema* parentSchema, HoloTableSchema* partitionSchema) {
    if (parentSchema->nColumns != partitionSchema->nColumns || parentSchema->nPrimaryKeys != partitionSchema->nPrimaryKeys) {
        return HOLO_CLIENT_RET_FAIL;
    }
    for (int i = 0; i < partitionSchema->nColumns; i++) {
        HoloColumn parentColumn = parentSchema->columns[i];
        HoloColumn partitionColumn = partitionSchema->columns[i];
        if (!compare_strings(parentColumn.name, partitionColumn.name)
            || !compare_strings(parentColumn.defaultValue, partitionColumn.defaultValue)
            || parentColumn.type != partitionColumn.type
            || parentColumn.isPrimaryKey != partitionColumn.isPrimaryKey
            || parentColumn.nullable != partitionColumn.nullable) {
            return HOLO_CLIENT_RET_FAIL;
        }
    }

    return HOLO_CLIENT_RET_OK;
}

int check_partition(HoloClient* client, HoloMutation mutation) {
    char* partitionValue = NULL;
    HoloTableSchema* parentSchema = NULL;
    HoloTableSchema* partitionSchema = NULL;
    HoloTableName* name = NULL;
    Sql sql = NULL;
    char* partition = NULL;
    char intValStr[INT32_STRING_MAX_LENGTH];
    if (mutation->record->schema->partitionColumn == -1) return HOLO_CLIENT_RET_OK;
    parentSchema = mutation->record->schema;
    if (mutation->record->valueFormats[parentSchema->partitionColumn] == 1) {
        get_int_partition_value_string(intValStr, mutation->record->values[parentSchema->partitionColumn]);
        partitionValue = intValStr;
    } else {
        partitionValue = mutation->record->values[parentSchema->partitionColumn];
    }
    partitionSchema = meta_cache_find_partition(client->workerPool->metaCache, parentSchema, partitionValue);
    if (partitionSchema != NULL) {
        mutation->record->schema = partitionSchema;
        return HOLO_CLIENT_RET_OK;
    }
    sql = holo_client_new_sql_request(find_partition_table_name, mutation->record);
    name = holo_client_sql(client, sql);
    if (name != NULL) {
        partitionSchema = holo_client_get_tableschema(client, name->schemaName, name->tableName, false);
        if (check_partition_schema(parentSchema, partitionSchema) != HOLO_CLIENT_RET_OK) {
            FREE(name->schemaName);
            FREE(name->tableName);
            FREE(name);
            return HOLO_CLIENT_PARTITION_META_CHANGE;
        }
        mutation->record->schema = partitionSchema;
        meta_cache_add_partition(client->workerPool->metaCache, parentSchema, partitionSchema, partitionValue);
        FREE(name->schemaName);
        FREE(name->tableName);
        FREE(name);
        return HOLO_CLIENT_RET_OK;
    }
    if (client->config.dynamicPartition && mutation->mode != DELETE) {
        LOG_WARN("Partition %s does not exist in table %s", partitionValue, parentSchema->tableName->fullName);
        LOG_INFO("Creating partition child table for value %s in table %s", partitionValue, parentSchema->tableName->fullName);
        sql = holo_client_new_sql_request(retry_create_partition_child_table, mutation->record);
        partition = holo_client_sql(client, sql);
        if (partition != NULL) {
            LOG_INFO("Created partition %s", partition);
            partitionSchema = holo_client_get_tableschema(client, parentSchema->tableName->schemaName, partition, false);
            mutation->record->schema = partitionSchema;
            meta_cache_add_partition(client->workerPool->metaCache, parentSchema, partitionSchema, partitionValue);
            FREE(partition);
            return HOLO_CLIENT_RET_OK;
        }
        LOG_ERROR("Failed creating partition child table for value %s in table %s", partitionValue, parentSchema->tableName->fullName);
    } else {
        LOG_ERROR("Partition %s does not exist in table %s", partitionValue, parentSchema->tableName->fullName);
    }
    return HOLO_CLIENT_PARTITION_NOT_EXIST;
}

int check_get(HoloGet get) {
    HoloTableSchema* schema = NULL;
    int index;
    if (get == NULL) {
        LOG_ERROR("HoloGet request cannot be null for table \"%s\"!", get->record->schema->tableName->tableName);
        return HOLO_CLIENT_INVALID_PARAM;
    }
    schema = get->record->schema;
    for (int i = 0; i < schema->nPrimaryKeys; i++) {
        index = schema->primaryKeys[i];
        if (!get->record->valuesSet[index] || get->record->values[index] == NULL) {
            LOG_ERROR("HoloGet request primary key cannot be null for table \"%s\"!", get->record->schema->tableName->tableName);
            return HOLO_CLIENT_PK_IS_NULL;
        }
    }
    return HOLO_CLIENT_RET_OK;
}

const char* holo_client_get_errmsg_with_errcode(int errCode) {
    const char* ret = NULL;
    switch (errCode)
    {
    case HOLO_CLIENT_RET_OK:
        ret = "Success";
        break;
    case HOLO_CLIENT_RET_FAIL:
        ret = "Internal error";
        break;
    case HOLO_CLIENT_FLUSH_FAIL:
        ret = "Flush failed";
        break;
    case HOLO_CLIENT_INVALID_PARAM:
        ret = "Invalid parameter";
        break;
    case HOLO_CLIENT_INVALID_COL_IDX:
        ret = "Column index exceeds column number";
        break;
    case HOLO_CLIENT_INVALID_COL_NAME:
        ret = "Column does not exist";
        break;
    case HOLO_CLIENT_TYPE_NOT_MATCH:
        ret = "Column type not match";
        break;
    case HOLO_CLIENT_COL_ALREADY_SET:
        ret = "Column already set value";
        break;
    case HOLO_CLIENT_NOT_NULL_BUT_SET_NULL:
        ret = "Column can not be null but set null";
        break;
    case HOLO_CLIENT_COL_NOT_PK:
        ret = "Column is not primary key of table in get request";
        break;
    case HOLO_CLIENT_CHECK_CONSTRAINT_FAIL:
        ret = "Check constraint failed";
        break;
    case HOLO_CLIENT_PK_IS_NULL:
        ret = "Primary key in request cannot be null";
        break;
    case HOLO_CLIENT_PARTITION_IS_NULL:
        ret = "Partition key in request cannot be null";
        break;
    case HOLO_CLIENT_DELETE_NO_PK_TABLE:
        ret = "Table must have primary key in delete request";
        break;
    case HOLO_CLIENT_PARTITION_NOT_EXIST:
        ret = "Partition does not exist";
        break;
    case HOLO_CLIENT_PK_NOT_SET_IN_DELETE:
        ret = "Primary key not set in delete request";
        break;
    case HOLO_CLIENT_NOT_NULL_BUT_NOT_SET:
        ret = "Column can not be null but not set";
        break;
    case HOLO_CLIENT_PARTITION_META_CHANGE:
        ret = "Partition table meta changed";
        break;
    case HOLO_CLIENT_EXCEED_MAX_BYTE:
        ret = "Batch exceeds writeBatchByteSize and autoFlush is disabled";
        break;
    case HOLO_CLIENT_EXCEED_MAX_NUM:
        ret = "Batch exceeds batchSize and autoFlush is disabled";
        break;
    case HOLO_CLIENT_EXCEED_MAX_INTERVAL:
        ret = "Exceeds writeMaxInterval and autoFlush is disabled";
        break;
    case HOLO_CLIENT_EXCEED_MAX_TOTAL_BYTE:
        ret = "Exceeds writeBatchTotalByteSize and autoFlush is disabled";
        break;
    default:
        ret = "Unknown error";
        break;
    }
    return ret;
}