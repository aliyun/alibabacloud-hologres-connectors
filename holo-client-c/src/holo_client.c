#include "holo_client_private.h"
#include "holo_config_private.h"
#include "utils.h"
#include "logger.h"
#include "unistd.h"

bool check_mutation(Mutation mutation);
bool check_partition(HoloClient* client, Mutation mutation);
bool check_get(Get get);

HoloClient* holo_client_new_client(HoloConfig config) {
    //holo_client_logger_open();
    if (!holo_config_is_valid(&config)){
        LOG_ERROR("Holo config invalid.");
        return NULL;
    }
    log_holo_config(&config);
    HoloClient* client = MALLOC(1, HoloClient);
    client->workerPool = holo_client_new_worker_pool(config);
    client->isEmbeddedPool = false;
    client->directCollector = holo_client_new_direct_collector();
    client->mutationCollector = holo_client_new_mutation_collector(client->workerPool, config);
    client->getCollector = holo_client_new_get_collector(client->workerPool, config.readBatchSize);
    client->config = config;
    holo_client_start_worker_pool(client->workerPool);
    holo_client_start_watch_direct_collector(client->directCollector, client->workerPool);
    holo_client_start_watch_mutation_collector(client->mutationCollector);
    holo_client_start_watch_get_collector(client->getCollector);
    return client;
}

HoloClient* holo_client_new_client_with_workerpool(HoloConfig config, WorkerPool* pool) {
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
        return -1;
    }
    LOG_INFO("Closing...");
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
    FREE(client->config.connInfo);
    FREE(client);
    client = NULL;
    return 0;
}

TableSchema* holo_client_get_tableschema_by_tablename(HoloClient* client, TableName name, bool withCache) {
    Meta meta;
    TableSchema* schema;
    if(withCache) {
        schema = find_tableschema_in_metacache(client->workerPool->metaCache, name);
        if (schema != NULL) {
            return schema;
        }
    }
    meta = holo_client_new_meta_request(name);
    holo_client_add_meta_request_to_direct_collector(client->directCollector, meta);
    schema = get_future_result(meta->future);
    holo_client_destroy_meta_request(meta);
    if (schema != NULL) {
        add_tableschema_to_metacache(client->workerPool->metaCache, schema);
    } else {
        LOG_ERROR("Get table schema failed.");
        return NULL;
    }
    return schema;
}

TableSchema* holo_client_get_tableschema(HoloClient* client, const char* schemaName, const char* tableName, bool withCache) {
    if (client == NULL) {
        LOG_ERROR("Holo client is NULL.");
        return NULL;
    }
    TableName name;
    if (tableName == NULL){
        LOG_ERROR("Table name in table name not set.");
        return NULL;
    }
    name.tableName = (char*)tableName;
    if (schemaName == NULL){
        name.schemaName = "public";
        LOG_WARN("Schema name in table name not set. Set to \"public\".");
    }
    else name.schemaName = (char*)schemaName;

    name.fullName = MALLOC(strlen(name.tableName) + strlen(name.schemaName) + 6, char);
    int position = 0;
    strncpy(name.fullName + position, "\"", 2);
    position++;
    strncpy(name.fullName + position, name.schemaName, strlen(name.schemaName));
    position += strlen(name.schemaName);
    strncpy(name.fullName + position, "\".\"", 4);
    position += 3;
    strncpy(name.fullName + position, name.tableName, strlen(name.tableName));
    position += strlen(name.tableName);
    strncpy(name.fullName + position, "\"", 2);
    TableSchema* ret = holo_client_get_tableschema_by_tablename(client, name, withCache);
    FREE(name.fullName);
    return ret;
}

int holo_client_submit(HoloClient* client, Mutation mutation){
    if (client == NULL) {
        LOG_ERROR("Holo client is NULL.");
        return -1;
    }
    if (mutation == NULL) {
        LOG_ERROR("Mutation is NULL.");
        return -1;
    }
    mutation->writeMode = client->config.writeMode;  //所有mutation request的write mode都和config保持一致
    if (!check_mutation(mutation) || !check_partition(client, mutation) || !normalize_mutation_request(mutation)){  //若非UPDATE，则给未设置的列填入默认值或NULL
        client->config.exceptionHandler(mutation->record, "Submit request failed.");
        holo_client_destroy_mutation_request(mutation);
        return -1;
    }
    mutation->byteSize = sizeof(MutationRequest) + mutation->record->byteSize;
    holo_client_add_request_to_mutation_collector(client->mutationCollector, mutation);
    return 0;
}

void* holo_client_sql(HoloClient* client, Sql sql) {
    void* retVal;
    holo_client_add_sql_request_to_direct_collector(client->directCollector, sql);
    retVal = get_future_result(sql->future);
    holo_client_destroy_sql_request(sql);
    return retVal;
}

int holo_client_flush_client(HoloClient* client) {
    if (client == NULL) {
        LOG_ERROR("Holo client is NULL.");
        return -1;
    }
    holo_client_flush_mutation_collector(client->mutationCollector);
    return 0;
}

int holo_client_get(HoloClient* client, Get get) {
    if (client == NULL) {
        LOG_ERROR("Holo client is NULL.");
        return -1;
    }
    if (get == NULL) {
        LOG_ERROR("Get is NULL.");
        return -1;
    }
    if(!check_get(get)) {
        // client->config.exceptionHandler(get->record, "Submit get request failed.");
        // holo_client_destroy_get_request(get);
        return -1;
    }
    holo_client_add_request_to_get_collector(client->getCollector, get);
    return 0;
}

Record* holo_client_get_record(Get get) {
    if (get == NULL) {
        LOG_ERROR("Get is NULL.");
        return NULL;
    }
    if (!get->submitted) {
        LOG_ERROR("Not yet submitted!");
        return NULL;
    }
    return (Record*)get_future_result(get->future);
}

char* holo_client_get_record_val(Record* record, int colIndex) {
    if(colIndex < 0 || colIndex >= record->schema->nColumns) {
        LOG_ERROR("Invalid column index %d", colIndex);
        return NULL;
    }
    return record->values[colIndex];
}

void holo_client_logger_open() {
    logger_open();
}
void holo_client_logger_close() {
    logger_close();
}

bool check_mutation(Mutation mutation) {
    TableSchema* schema;
    int index;
    if (mutation == NULL) {
        LOG_ERROR("Mutation request cannot be null!");
        return false;
    }
    schema = mutation->record->schema;
    for (int i = 0; i < schema->nPrimaryKeys; i++) {
        index = schema->primaryKeys[i];
        if (!mutation->record->valuesSet[index] || (mutation->record->values[index] == NULL && schema->columns[index].defaultValue == NULL)) {
            LOG_ERROR("Mutation request primary key cannot be null!");
            return false;
        }
    }
    index = schema->partitionColumn;
    if (index > -1 && (!mutation->record->valuesSet[index] || mutation->record->values[index] == NULL)) {
        LOG_ERROR("Mutation Request partition key cannot be null!");
        return false;
    }
    if (mutation->mode == DELETE && schema->nPrimaryKeys == 0) {
        LOG_ERROR("Delete request table must have primary key!");
        return false;
    }
    return true;
}

void* find_partition_table_name(PGconn* conn, void* arg) {
    Record* record = arg;
    const char* findPartitionSql = "with inh as (SELECT i.inhrelid, i.inhparent FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_catalog.pg_inherits i on c.oid=i.inhparent where n.nspname = $1 and c.relname= $2) select n.nspname as schema_name, c.relname as table_name, inh.inhrelid, inh.inhparent, p.partstrat, pg_get_expr(c.relpartbound, c.oid, true) as part_expr, p.partdefid, p.partnatts, p.partattrs from inh join pg_catalog.pg_class c on inh.inhrelid = c.oid join pg_catalog.pg_namespace n on c.relnamespace = n.oid join pg_partitioned_table p on p.partrelid = inh.inhparent where pg_get_expr(c.relpartbound, c.oid, true) = $3 limit 1";
    const char* schemaName = record->schema->tableName->schemaName;
    const char* tableName = record->schema->tableName->tableName;
    int length = strlen(record->values[record->schema->partitionColumn]) + 19;
    char* partitionInfo = MALLOC(length, char);
    const char* params[3] = {schemaName, tableName, partitionInfo};
    PGresult* res;
    TableName* name;
    Oid type = record->schema->columns[record->schema->partitionColumn].type;
    if (type == 25 || type == 1043) {
        //text or varchar
        snprintf(partitionInfo, length, "FOR VALUES IN ('%s')", record->values[record->schema->partitionColumn]);
    } else {
        snprintf(partitionInfo, length, "FOR VALUES IN (%s)", record->values[record->schema->partitionColumn]);
    }
    res = PQexecParams(conn, findPartitionSql, 3, NULL, params, NULL, NULL, 0);
    if (PQntuples(res) == 0) {
        PQclear(res);
        FREE(partitionInfo);
        return NULL;
    }
    name = MALLOC(1, TableName);
    name->schemaName = deep_copy_string(PQgetvalue(res, 0, 0));
    name->tableName = deep_copy_string(PQgetvalue(res, 0, 1));
    name->fullName = NULL;
    PQclear(res);
    FREE(partitionInfo);
    return name;
}

void* retry_create_partition_child_table(PGconn* conn, void* arg) {
    int retry = 0;
    Record* record = arg;
    TableSchema* parentSchema = record->schema;
    char intVal[12];
    char* partitionValue = record->values[record->schema->partitionColumn];
    if (record->valueFormats[parentSchema->partitionColumn] == 1) {
        endian_swap(partitionValue, 4);
        snprintf(intVal, 12, "%d", *(int32_t*)partitionValue);
        endian_swap(partitionValue, 4);
        partitionValue = intVal;
    }
    int maxTableNameLength = strlen(parentSchema->tableName->tableName) + strlen(partitionValue) + 22;
    int maxSqlLength = 2 * strlen(parentSchema->tableName->schemaName) + strlen(parentSchema->tableName->tableName) + maxTableNameLength + strlen(partitionValue) + 57;
    char* tableName = MALLOC(maxTableNameLength, char);
    char* sql = MALLOC(maxSqlLength, char);
    Oid type = parentSchema->columns[parentSchema->partitionColumn].type;
    PGresult* res;
    char* errorMsg;
    bool continueRetry = true;
    snprintf(tableName, maxTableNameLength, "%s_%s", parentSchema->tableName->tableName, partitionValue);
    while(continueRetry) {
        if (retry > 0) {
            snprintf(tableName, maxTableNameLength, "%s_%s_%ld", parentSchema->tableName->tableName, partitionValue, current_time_ms());
        }
        if (type == 25 || type == 1043) {
            //text or varchar
            snprintf(sql, maxSqlLength, "CREATE TABLE \"%s\".\"%s\" PARTITION OF \"%s\".\"%s\" FOR VALUES IN ('%s')", parentSchema->tableName->schemaName, tableName
            , parentSchema->tableName->schemaName, parentSchema->tableName->tableName, partitionValue);
        } else {
            snprintf(sql, maxSqlLength, "CREATE TABLE \"%s\".\"%s\" PARTITION OF \"%s\".\"%s\" FOR VALUES IN (%s)", parentSchema->tableName->schemaName, tableName
            , parentSchema->tableName->schemaName, parentSchema->tableName->tableName, partitionValue);
        }
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

bool check_partition(HoloClient* client, Mutation mutation) {
    char* partitionValue;
    TableSchema* parentSchema;
    TableSchema* partitionSchema;
    TableName* name;
    Sql sql;
    char* partition;
    char intVal[12];
    if (mutation->record->schema->partitionColumn == -1) return true;
    parentSchema = mutation->record->schema;
    partitionValue = mutation->record->values[parentSchema->partitionColumn];
    if (mutation->record->valueFormats[parentSchema->partitionColumn] == 1) {
        endian_swap(partitionValue, 4);
        snprintf(intVal, 12, "%d", *(int32_t*)partitionValue);
        endian_swap(partitionValue, 4);
        partitionValue = intVal;
    }
    partitionSchema = meta_cache_find_partition(client->workerPool->metaCache, parentSchema, partitionValue);
    if (partitionSchema != NULL) {
        mutation->record->schema = partitionSchema;
        return true;
    }
    sql = holo_client_new_sql_request(find_partition_table_name, mutation->record);
    name = holo_client_sql(client, sql);
    if (name != NULL) {
        partitionSchema = holo_client_get_tableschema(client, name->schemaName, name->tableName, false);
        mutation->record->schema = partitionSchema;
        meta_cache_add_partition(client->workerPool->metaCache, parentSchema, partitionSchema, partitionValue);
        FREE(name->schemaName);
        FREE(name->tableName);
        FREE(name);
        return true;
    }
    if (client->config.dynamicPartition) {
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
            return true;
        }
        LOG_ERROR("Failed creating partition child table for value %s in table %s", partitionValue, parentSchema->tableName->fullName);
    } else {
        LOG_ERROR("Partition %s does not exist in table %s", partitionValue, parentSchema->tableName->fullName);
    }
    return false;
}

bool check_get(Get get) {
    TableSchema* schema;
    int index;
    if (get == NULL) {
        LOG_ERROR("Get request cannot be null!");
        return false;
    }
    schema = get->record->schema;
    for (int i = 0; i < schema->nPrimaryKeys; i++) {
        index = schema->primaryKeys[i];
        if (!get->record->valuesSet[index] || get->record->values[index] == NULL) {
            LOG_ERROR("Get request primary key cannot be null!");
            return false;
        }
    }
    return true;
}