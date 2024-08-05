#ifndef _HOLO_CLIENT_PRIVATE_H_
#define _HOLO_CLIENT_PRIVATE_H_

#include "../include/holo_client.h"
#include "direct_collector.h"
#include "mutation_collector.h"
#include "get_collector.h"
#include <stdbool.h>

struct _HoloClient {
    HoloWorkerPool *workerPool;
    HoloWorkerPool *fixedPool;
    bool isEmbeddedPool;
    DirectCollector* directCollector;
    MutationCollector* mutationCollector;
    GetCollector* getCollector;
    HoloConfig config;
};

HoloTableSchema* holo_client_get_tableschema_by_tablename(HoloClient* client, HoloTableName name, bool withCache, char** errMsgAddr);

void* holo_client_sql(HoloClient*, Sql);

void holo_client_ensure_pool_open(HoloClient*); //open a new workerpool if doesn't have one

#endif