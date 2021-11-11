#include "direct_collector.h"
#include "utils.h"
#include "logger.h"

typedef struct _DCArgs {
    DirectCollector* collector;
    WorkerPool* pool;
} DCArgs;

DirectCollector* holo_client_new_direct_collector() {
    DirectCollector* collector = MALLOC(1, DirectCollector);
    collector->actionWatcherThread = MALLOC(1, pthread_t);
    collector->mutex = MALLOC(1, pthread_mutex_t);
    collector->cond = MALLOC(1, pthread_cond_t);
    pthread_mutex_init(collector->mutex, NULL);
    pthread_cond_init(collector->cond, NULL); 
    collector->status = 0;
    collector->numActions = 0;
    dlist_init(&(collector->actionsToDo));
    return collector;
}

void* watch_direct_collector_run(void* argsPtr) {
    DCArgs* args = argsPtr;
    DirectCollector* collector = args->collector;
    WorkerPool* pool = args->pool;
    dlist_mutable_iter miter;
    ActionItem* actionItem;
    Action* action;
    pthread_mutex_lock(collector->mutex);
    while(collector->status == 1 || collector->numActions > 0) {
        dlist_foreach_modify(miter, &(collector->actionsToDo)) {
            actionItem = dlist_container(ActionItem, list_node, miter.cur);
            action = actionItem->action;
            holo_client_submit_action_to_worker_pool(pool, action);
            dlist_delete(miter.cur);
            collector->numActions--;
            FREE(actionItem);
        }
        if (collector->status != 1) {
            break;
        }
        pthread_cond_wait(collector->cond,  collector->mutex);
    }
    collector->status = 3;
    pthread_mutex_unlock(collector->mutex);
    FREE(args);
    return NULL;
}

int holo_client_start_watch_direct_collector(DirectCollector* collector, WorkerPool* pool) {
    int rc;
    DCArgs* args = MALLOC(1, DCArgs);
    args->collector = collector;
    args->pool = pool;
    collector->status = 1;
    rc = pthread_create(collector->actionWatcherThread, NULL, watch_direct_collector_run, args);
    if (rc != 0) {
        collector->status = 4;
        LOG_ERROR("start direct collector failed with error code %d", rc);
    }
    return rc;
}

void holo_client_add_meta_request_to_direct_collector(DirectCollector* collector, Meta meta) {
    MetaAction* action;
    pthread_mutex_lock(collector->mutex);
    action = holo_client_new_meta_action(meta);
    dlist_push_tail(&(collector->actionsToDo), &(create_action_item((Action*)action)->list_node));
    collector->numActions++;
    pthread_cond_signal(collector->cond);
    pthread_mutex_unlock(collector->mutex);
}

void holo_client_add_sql_request_to_direct_collector(DirectCollector* collector, Sql sql) {
    SqlAction* action;
    pthread_mutex_lock(collector->mutex);
    action = holo_client_new_sql_action(sql);
    dlist_push_tail(&(collector->actionsToDo), &(create_action_item((Action*)action)->list_node));
    collector->numActions++;
    pthread_cond_signal(collector->cond);
    pthread_mutex_unlock(collector->mutex);
}

int holo_client_stop_watch_direct_collector(DirectCollector* collector) {
    int rc;
    pthread_mutex_lock(collector->mutex);
    collector->status = 2;
    pthread_cond_signal(collector->cond);
    pthread_mutex_unlock(collector->mutex);
    rc = pthread_join(*collector->actionWatcherThread, NULL);
    collector->status = 3;
    return rc;
}

void holo_client_destroy_direct_collector(DirectCollector* collector) {
    pthread_mutex_destroy(collector->mutex);
    pthread_cond_destroy(collector->cond);
    FREE(collector->actionWatcherThread);
    FREE(collector->mutex);
    FREE(collector->cond);
    FREE(collector);
    collector = NULL;
}