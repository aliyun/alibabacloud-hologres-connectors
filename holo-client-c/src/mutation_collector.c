#include "mutation_collector.h"
#include "utils.h"
#include "action.h"
#include "logger.h"
#include "murmur3.h"

ShardCollector* holo_client_new_shard_collector(HoloWorkerPool* pool, int batchSize, long writeMaxIntervalMs, bool hasPK, long maxByteSize) {
    ShardCollector* collector = MALLOC(1, ShardCollector);
    collector->map = holo_client_new_mutation_map(batchSize);
    collector->mutex = MALLOC(1, pthread_mutex_t);
    collector->numRequests = 0;
    collector->pool = pool;
    collector->startTime = current_time_ms();;
    collector->batchSize = batchSize;
    collector->writeMaxIntervalMs = writeMaxIntervalMs;
    collector->activeAction = NULL;
    pthread_mutex_init(collector->mutex, NULL);
    collector->hasPK = hasPK;
    collector->maxByteSize = maxByteSize;
    collector->byteSize = 0;
    return collector;
}

bool shard_collector_should_flush(ShardCollector* collector) {
    if (collector->numRequests == 0) return false;
    if (collector->numRequests == collector->batchSize) return true;
    if (current_time_ms() - collector->startTime > collector->writeMaxIntervalMs) return true;
    if (collector->byteSize > collector->maxByteSize) return true;
    if ((current_time_ms() - collector->startTime > collector->writeMaxIntervalMs / 2) && ((collector->numRequests & (collector->numRequests - 1)) == 0)) return true;
    if (collector->byteSize > collector->maxByteSize / 2 && ((collector->numRequests & (collector->numRequests - 1)) == 0)) return true;
    return false;
}

long holo_client_add_request_to_shard_collector(ShardCollector* collector, HoloMutation mutation) {
    long before, after;
    pthread_mutex_lock(collector->mutex);
    if (collector->numRequests == 0) collector->startTime = current_time_ms();
    before = collector->byteSize;
    mutation_map_add(collector->map, mutation, collector->hasPK);
    collector->numRequests = collector->map->size;
    collector->byteSize = collector->map->byteSize;
    if (shard_collector_should_flush(collector)) {
        if (current_time_ms() - collector->startTime > collector->writeMaxIntervalMs) metrics_meter_mark(collector->pool->metrics->timeoutFlush, 1);
        else metrics_meter_mark(collector->pool->metrics->timeoutFlush, 0);
        do_flush_shard_collector(collector);
    }
    after = collector->byteSize;
    pthread_mutex_unlock(collector->mutex);
    return after - before;
}

void shard_collector_clear_mutation_action(ShardCollector* collector) {
    if (collector->activeAction != NULL && collector->activeAction->future->completed == false) {
        get_future_result(collector->activeAction->future);
    }
    if (collector->activeAction != NULL) {
        holo_client_destroy_mutation_action(collector->activeAction);
        collector->activeAction = NULL;
    }
}

void holo_client_destroy_shard_collector(ShardCollector* collector) {
    holo_client_flush_shard_collector(collector);
    pthread_mutex_destroy(collector->mutex);
    holo_client_destroy_mutation_map(collector->map);
    FREE(collector->mutex);
    FREE(collector);
    collector = NULL;
}

void do_flush_shard_collector(ShardCollector* collector) {
    if (collector->numRequests == 0) return;
    MutationAction* action = NULL;
    HoloMutation mutation = NULL;
    //LOG_DEBUG("starting flush shard collector"); 
    action = holo_client_new_mutation_action();
    action->numRequests = collector->numRequests;
    for (int i = 0; i < collector->map->maxSize; i++) {
        mutation = collector->map->mutations[i];
        if (mutation != NULL) {
            dlist_push_tail(&(action->requests), &(create_mutation_item(mutation)->list_node));
        }
        collector->map->mutations[i] = NULL;
    }
    metrics_histogram_update(collector->pool->metrics->gatherTime, current_time_ms() - collector->startTime);
    long before = current_time_ms();
    shard_collector_clear_mutation_action(collector);  //阻塞等待上一个action完成
    metrics_histogram_update(collector->pool->metrics->getFutureTime, current_time_ms() - before);
    before = current_time_ms();
    holo_client_submit_action_to_worker_pool(collector->pool, (Action*)action);
    metrics_histogram_update(collector->pool->metrics->submitActionTime, current_time_ms() - before);
    collector->numRequests = 0;
    collector->map->size = 0;
    collector->byteSize = 0;
    collector->map->byteSize = 0;
    collector->activeAction = action;
    collector->startTime = current_time_ms();
}

void holo_client_flush_shard_collector(ShardCollector* collector) {
    pthread_mutex_lock(collector->mutex);
    do_flush_shard_collector(collector);
    pthread_mutex_unlock(collector->mutex);
}

void holo_client_try_flush_shard_collector(ShardCollector* collector) {
    if (shard_collector_should_flush(collector)) {
        pthread_mutex_lock(collector->mutex);
        if (shard_collector_should_flush(collector)) {
            if (current_time_ms() - collector->startTime > collector->writeMaxIntervalMs) metrics_meter_mark(collector->pool->metrics->timeoutFlush, 1);
            else metrics_meter_mark(collector->pool->metrics->timeoutFlush, 0);
            do_flush_shard_collector(collector);
        }
        pthread_mutex_unlock(collector->mutex);
    }
}

TableCollector* holo_client_new_table_collector(HoloTableSchema* schema, HoloWorkerPool* pool, int batchSize, long writeMaxIntervalMs, long maxByteSize) {
    TableCollector* collector = MALLOC(1, TableCollector);
    collector->schema = schema;
    collector->numRequests = 0;
    collector->pool = pool;
    collector->nShardCollectors = pool->config.shardCollectorSize;
    collector->shardCollectors = MALLOC(collector->nShardCollectors, ShardCollector*);
    collector->hasPK = table_has_pk(schema);
    collector->maxByteSize = maxByteSize;
    collector->byteSize = 0;
    for (int i = 0;i < collector->nShardCollectors;i++){
        collector->shardCollectors[i] = holo_client_new_shard_collector(pool, batchSize, writeMaxIntervalMs, collector->hasPK, collector->maxByteSize);
    }
    return collector;
}

int shard_hash(HoloRecord* record, int nShards){
    unsigned raw = 0;
    bool first = true;
    for (int i = 0;i < record->schema->nDistributionKeys;i++){
        int index = record->schema->distributionKeys[i];
        char* value = record->values[index];
        int length = record->valueLengths[index];
        if (first){
            MurmurHash3_x86_32(value, length, 104729, &raw);
            first = false;
        }
        else{
            unsigned t = 0;
            MurmurHash3_x86_32(value, length, 104729, &t);
            raw ^= t;
        }
    }
    int hash = raw % ((unsigned)65536);
    int base = 65536 / nShards;
    int remain = 65536 % nShards;
    int pivot = (base + 1) * remain;
    int index = 0;
    if (hash < pivot) index = hash / (base + 1);
    else index = (hash - pivot) / base + remain;
    return index;
}

long holo_client_add_request_to_table_collector(TableCollector* collector, HoloMutation mutation) {
    int shard = shard_hash(mutation->record, collector->nShardCollectors);
    long byteChange = holo_client_add_request_to_shard_collector(collector->shardCollectors[shard],  mutation);
    collector->byteSize += byteChange;
    return byteChange;
}

void holo_client_flush_table_collector(TableCollector* collector) {
    for (int i = 0; i < collector->nShardCollectors; i++) {
        holo_client_flush_shard_collector(collector->shardCollectors[i]);
    }
    collector->byteSize = 0;
}

void clear_table_collector_actions(TableCollector* collector){
    for (int i = 0; i < collector->nShardCollectors; i++) {
        shard_collector_clear_mutation_action(collector->shardCollectors[i]);
    }
}

void holo_client_try_flush_table_collector(TableCollector* collector) {
    long byteBefore, byteAfter;
    for (int i = 0; i < collector->nShardCollectors; i++) {
        byteBefore = collector->shardCollectors[i]->byteSize;
        holo_client_try_flush_shard_collector(collector->shardCollectors[i]);
        byteAfter = collector->shardCollectors[i]->byteSize;
        if (byteAfter != byteBefore) collector->byteSize += (byteAfter - byteBefore);
    }
}

void holo_client_destroy_table_collector(TableCollector* collector) {
    for (int i = 0; i < collector->nShardCollectors; i++) {
        holo_client_destroy_shard_collector(collector->shardCollectors[i]);
    }
    FREE(collector->shardCollectors);
    FREE(collector);
}

MutationCollector* holo_client_new_mutation_collector(HoloWorkerPool* pool, HoloConfig config) {
    MutationCollector* collector = MALLOC(1, MutationCollector);
    collector->pool = pool;
    dlist_init(&(collector->tableCollectors));
    collector->numTables = 0;
    collector->autoFlush = MALLOC(1, pthread_t);
    collector->rwLock = MALLOC(1, pthread_rwlock_t);
    pthread_rwlock_init(collector->rwLock, NULL);
    collector->batchSize = config.batchSize;
    collector->writeMaxIntervalMs = config.writeMaxIntervalMs;
    collector->status = 0;
    collector->byteSize = 0;
    collector->maxByteSize = config.writeBatchByteSize;
    collector->maxTotalByteSize = config.writeBatchTotalByteSize;
    return collector;
}

void* watch_mutation_collector_run(void* mutationCollector) {
    int waitInMs = 1000;
    MutationCollector* collector = mutationCollector;
    HoloWorkerPool* pool = collector->pool;
    dlist_iter iter;
    TableCollectorItem* item;
    pthread_mutex_t fakeMutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t fakeCond = PTHREAD_COND_INITIALIZER;
    long byteBefore, byteAfter;

    while(collector->status == 1) {
        metrics_try_gather_and_show(pool->metrics);
        pthread_rwlock_rdlock(collector->rwLock);
        dlist_foreach(iter, &(collector->tableCollectors)) {
            item = dlist_container(TableCollectorItem, list_node, iter.cur);
            byteBefore = item->tableCollector->byteSize;
            holo_client_try_flush_table_collector(item->tableCollector);
            byteAfter = item->tableCollector->byteSize;
            if (byteAfter != byteBefore) collector->byteSize += (byteAfter - byteBefore);
        }
        pthread_rwlock_unlock(collector->rwLock);

        //wait for waitinMs
        struct timespec outTime = get_out_time(waitInMs);
        pthread_mutex_lock(&fakeMutex);
        pthread_cond_timedwait(&fakeCond, &fakeMutex, &outTime);
        pthread_mutex_unlock(&fakeMutex);
    }
    collector->status = 3;
    return NULL;
}

int holo_client_start_watch_mutation_collector(MutationCollector* collector) {
    int rc;
    collector->status = 1;
    rc = pthread_create(collector->autoFlush, NULL, watch_mutation_collector_run, collector);
    if (rc != 0) {
        collector->status = 4;
        LOG_ERROR("start mutation collector failed with error code %d", rc);
    }
    LOG_DEBUG("start watch mutation collector");
    return rc;
}

TableCollectorItem* create_table_collector_item(TableCollector* tableCollector) {
    TableCollectorItem* item = MALLOC(1, TableCollectorItem);
    item->tableCollector = tableCollector;
    return item;
}

TableCollector* find_table_collector(MutationCollector* collector, HoloTableSchema* schema) {
    TableCollector* tableCollector = NULL;
    dlist_iter iter;
    TableCollectorItem* item;
    dlist_foreach(iter, &(collector->tableCollectors)) {
        item = dlist_container(TableCollectorItem, list_node, iter.cur);
        if (item->tableCollector->schema->tableId == schema->tableId) {
            tableCollector = item->tableCollector;
            break;
        }
    }
    return tableCollector;
}

TableCollector* find_or_create_table_collector(MutationCollector* collector, HoloTableSchema* schema) {
    TableCollector* tableCollector = NULL;
    pthread_rwlock_rdlock(collector->rwLock);
    tableCollector = find_table_collector(collector, schema);
    pthread_rwlock_unlock(collector->rwLock);
    if (tableCollector != NULL) return tableCollector;
    pthread_rwlock_wrlock(collector->rwLock);
    tableCollector = find_table_collector(collector, schema);
    if (tableCollector == NULL) {
        //create new table collector
        tableCollector = holo_client_new_table_collector(schema, collector->pool, collector->batchSize, collector->writeMaxIntervalMs, collector->maxByteSize);
        dlist_push_head(&(collector->tableCollectors), &(create_table_collector_item(tableCollector)->list_node));
        collector->numTables++;
    }
    pthread_rwlock_unlock(collector->rwLock);
    return tableCollector;
}


void holo_client_add_request_to_mutation_collector(MutationCollector* collector, HoloMutation mutation) {
    TableCollector* tableCollector = find_or_create_table_collector(collector, mutation->record->schema);
    long byteChange = holo_client_add_request_to_table_collector(tableCollector, mutation);
    collector->byteSize += byteChange;
    if (collector->byteSize > collector->maxTotalByteSize) holo_client_flush_mutation_collector(collector);
}

int holo_client_stop_watch_mutation_collector(MutationCollector* collector) {
    int rc;
    collector->status = 2;
    rc = pthread_join(*collector->autoFlush, NULL);
    collector->status = 3;
    return rc;
}

void holo_client_destroy_mutation_collector(MutationCollector* collector) {
    TableCollectorItem* item;
    TableCollector* tableCollector = NULL;
    dlist_mutable_iter miter;
    dlist_foreach_modify(miter, &(collector->tableCollectors)) {
        item = dlist_container(TableCollectorItem, list_node, miter.cur);
        tableCollector = item->tableCollector;
        dlist_delete(miter.cur);
        holo_client_destroy_table_collector(tableCollector);
        FREE(item);
    }
    pthread_rwlock_destroy(collector->rwLock);
    FREE(collector->autoFlush);
    FREE(collector->rwLock);
    FREE(collector);
    collector = NULL;
}

void holo_client_flush_mutation_collector(MutationCollector* collector) {
    dlist_iter iter;
    TableCollectorItem* item;
    pthread_rwlock_wrlock(collector->rwLock);
    dlist_foreach(iter, &(collector->tableCollectors)) {
        item = dlist_container(TableCollectorItem, list_node, iter.cur);
        holo_client_flush_table_collector(item->tableCollector);
    }
    dlist_foreach(iter, &(collector->tableCollectors)) {
        item = dlist_container(TableCollectorItem, list_node, iter.cur);
        clear_table_collector_actions(item->tableCollector);
    }
    collector->byteSize = 0;
    pthread_rwlock_unlock(collector->rwLock);
}