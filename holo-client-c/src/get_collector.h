#ifndef _GET_COLLECTOR_H_
#define _GET_COLLECTOR_H_

#include <pthread.h>
#include "worker_pool_private.h"
#include "request_private.h"

typedef struct _TableGetCollector {
    TableSchema* schema;
    WorkerPool* pool;
    int numRequests;
    int batchSize;
    pthread_mutex_t* mutex;
    Get* requests;
    pthread_cond_t* signal;
} TableGetCollector;

TableGetCollector* holo_client_new_table_get_collector(TableSchema* schema, WorkerPool* pool, int batchSize, pthread_cond_t* signal);
void holo_client_destroy_table_get_collector(TableGetCollector*);
GetAction* do_flush_table_get_collector(TableGetCollector*);
void flush_table_get_collector(TableGetCollector*);
void table_get_collector_add_request(TableGetCollector*, Get);

typedef struct _TableGetCollectorItem {
    dlist_node list_node;
    TableGetCollector* tableGetCollector;
} TableGetCollectorItem;

typedef struct _GetCollector {
    dlist_head tableCollectors;
    int numTables;
    pthread_t* actionWatcherThread;
    pthread_mutex_t* mutex;
    pthread_cond_t* cond;
    int status; //0:ready 1:started 2:stopping 3:stopped 4:error
    WorkerPool* pool;
    int batchSize;
} GetCollector;

GetCollector* holo_client_new_get_collector(WorkerPool* pool, int batchSize);
int holo_client_start_watch_get_collector(GetCollector*);
void holo_client_add_request_to_get_collector(GetCollector*, Get);
int holo_client_stop_watch_get_collector(GetCollector*);
void holo_client_destroy_get_collector(GetCollector*);
void holo_client_do_flush_get_collector(GetCollector*);

#endif