#include "get_collector.h"
#include "utils.h"
#include "action.h"
#include "logger.h"

TableGetCollector* holo_client_new_table_get_collector(TableSchema* schema, WorkerPool* pool, int batchSize, pthread_cond_t* signal) {
    TableGetCollector* collector = MALLOC(1, TableGetCollector);
    collector->schema = schema;
    collector->pool = pool;
    collector->numRequests = 0;
    collector->batchSize = batchSize;
    collector->mutex = MALLOC(1, pthread_mutex_t);
    pthread_mutex_init(collector->mutex, NULL);
    collector->requests = MALLOC(batchSize, Get);
    collector->signal = signal;
    return collector;
}

void holo_client_destroy_table_get_collector(TableGetCollector* collector) {
    flush_table_get_collector(collector);
    pthread_mutex_destroy(collector->mutex);
    FREE(collector->mutex);
    FREE(collector->requests);
    FREE(collector);
    collector = NULL;
}

GetAction* do_flush_table_get_collector(TableGetCollector* collector) {
    GetAction* action;
    if (collector->numRequests == 0) return NULL;
    action = holo_client_new_get_action();
    for (int i = 0; i < collector->numRequests; i++) {
        get_action_add_request(action, collector->requests[i]);
        collector->requests[i] = NULL;
    }
    collector->numRequests = 0;
    action->schema = collector->schema;
    // LOG_DEBUG("num request: %d", action->numRequests);
    return action;
    // submit action to worker pool
    // holo_client_submit_action_to_worker_pool(collector->pool, (Action*)action);
}

void flush_table_get_collector(TableGetCollector* collector) {
    GetAction* action;
    if (collector->numRequests == 0) return;
    pthread_mutex_lock(collector->mutex);
    action = do_flush_table_get_collector(collector);
    pthread_mutex_unlock(collector->mutex);
    if (action != NULL) holo_client_submit_action_to_worker_pool(collector->pool, (Action*)action);
}

void table_get_collector_add_request(TableGetCollector* collector, Get get) {
    GetAction* action = NULL;
    pthread_mutex_lock(collector->mutex);
    collector->requests[collector->numRequests] = get;
    collector->numRequests++;
    get->submitted = true;
    if (collector->numRequests == collector->batchSize) {
        action = do_flush_table_get_collector(collector);
    } else {
        pthread_cond_signal(collector->signal);
    }
    pthread_mutex_unlock(collector->mutex);
    if (action != NULL) holo_client_submit_action_to_worker_pool(collector->pool, (Action*)action);
}

GetCollector* holo_client_new_get_collector(WorkerPool* pool, int batchSize) {
    GetCollector* collector = MALLOC(1, GetCollector);
    dlist_init(&(collector->tableCollectors));
    collector->numTables = 0;
    collector->actionWatcherThread = MALLOC(1, pthread_t);
    collector->mutex = MALLOC(1, pthread_mutex_t);
    collector->cond = MALLOC(1, pthread_cond_t);
    pthread_mutex_init(collector->mutex, NULL);
    pthread_cond_init(collector->cond, NULL); 
    collector->status = 0;
    collector->pool = pool;
    collector->batchSize = batchSize;
    return collector;
}

void holo_client_destroy_get_collector(GetCollector* collector) {
    TableGetCollectorItem* item;
    TableGetCollector* tableCollector;
    dlist_mutable_iter miter;
    dlist_foreach_modify(miter, &(collector->tableCollectors)) {
        item = dlist_container(TableGetCollectorItem, list_node, miter.cur);
        tableCollector = item->tableGetCollector;
        dlist_delete(miter.cur);
        holo_client_destroy_table_get_collector(tableCollector);
        FREE(item);
    }
    pthread_mutex_destroy(collector->mutex);
    pthread_cond_destroy(collector->cond);
    FREE(collector->actionWatcherThread);
    FREE(collector->mutex);
    FREE(collector->cond);
    FREE(collector);
    collector = NULL;
}

void holo_client_do_flush_get_collector(GetCollector* collector) {
    dlist_iter iter;
    TableGetCollector* tableCollector;
    dlist_foreach(iter, &(collector->tableCollectors)) {
        tableCollector = dlist_container(TableGetCollectorItem, list_node, iter.cur)->tableGetCollector;
        flush_table_get_collector(tableCollector);
    }
}

void* watch_get_collector_run(void* argsPtr) {
    GetCollector* collector = argsPtr;
    struct timespec out_time;
    pthread_mutex_lock(collector->mutex);
    while (collector->status == 1) {
        holo_client_do_flush_get_collector(collector);
        out_time = get_out_time(2000);
        pthread_cond_timedwait(collector->cond, collector->mutex, &out_time);
    }
    collector->status = 3;
    pthread_mutex_unlock(collector->mutex);
    return NULL;
}

int holo_client_start_watch_get_collector(GetCollector* collector) {
    int rc;
    collector->status = 1;
    rc = pthread_create(collector->actionWatcherThread, NULL, watch_get_collector_run, collector);
    if (rc != 0) {
        collector->status = 4;
        LOG_ERROR("start get collector failed with error code %d", rc);
    }
    LOG_INFO("start watch get collector");
    return rc;
}

int holo_client_stop_watch_get_collector(GetCollector* collector) {
    int rc;
    collector->status = 2;
    pthread_cond_signal(collector->cond);
    rc = pthread_join(*collector->actionWatcherThread, NULL);
    collector->status = 3;
    return rc;
}

TableGetCollectorItem* create_table_get_collector_item(TableGetCollector* tableCollector) {
    TableGetCollectorItem* item = MALLOC(1, TableGetCollectorItem);
    item->tableGetCollector = tableCollector;
    return item;
}

TableGetCollector* find_table_get_collector(GetCollector* collector, TableSchema* schema) {
    TableGetCollector* tableCollector = NULL;
    dlist_iter iter;
    TableGetCollectorItem* item;
    dlist_foreach(iter, &(collector->tableCollectors)) {
        item = dlist_container(TableGetCollectorItem, list_node, iter.cur);
        if (item->tableGetCollector->schema->tableId == schema->tableId) { 
            tableCollector = item->tableGetCollector;
            break;
        }
    }
    return tableCollector;
}

TableGetCollector* find_or_create_table_get_collector(GetCollector* collector, TableSchema* schema) {
    TableGetCollector* tableCollector;
    tableCollector = find_table_get_collector(collector, schema);
    if (tableCollector != NULL) return tableCollector;
    pthread_mutex_lock(collector->mutex);
    tableCollector = find_table_get_collector(collector, schema);
    if (tableCollector == NULL) {
        //create new table get collector
        tableCollector = holo_client_new_table_get_collector(schema, collector->pool, collector->batchSize, collector->cond);
        dlist_push_head(&(collector->tableCollectors), &(create_table_get_collector_item(tableCollector)->list_node));
        collector->numTables++;
    }
    pthread_mutex_unlock(collector->mutex);
    return tableCollector;
}

void holo_client_add_request_to_get_collector(GetCollector* collector, Get get) {
    TableGetCollector* tableCollector = find_or_create_table_get_collector(collector, get->record->schema);
    table_get_collector_add_request(tableCollector, get);
}