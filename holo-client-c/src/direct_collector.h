#ifndef _DIRECT_COLLECTOR_H_
#define _DIRECT_COLLECTOR_H_
//ActionWatcher 只要ActionQueue不为空且有空余worker就提交
//ActionQueue 所有要执行的action

#include <pthread.h>
#include "request_private.h"
#include "action.h"
#include "ilist.h"
#include "worker_pool_private.h"


typedef struct _DirectCollector {
    dlist_head actionsToDo;
    int numActions;
    pthread_t* actionWatcherThread;
    pthread_mutex_t* mutex;
    pthread_cond_t* cond;
    int status; //0:ready 1:started 2:stopping 3:stopped 4:error
} DirectCollector;

DirectCollector* holo_client_new_direct_collector();
int holo_client_start_watch_direct_collector(DirectCollector*, HoloWorkerPool*);
void holo_client_add_meta_request_to_direct_collector(DirectCollector*, Meta);
void holo_client_add_sql_request_to_direct_collector(DirectCollector*, Sql);
int holo_client_stop_watch_direct_collector(DirectCollector*);
void holo_client_destroy_direct_collector(DirectCollector*);

#endif