#ifndef _WORKER_POOL_PRIVATE_H_
#define _WORKER_POOL_PRIVATE_H_

#include "worker.h"
#include "meta_cache.h"
#include "metrics.h"
#include "../include/worker_pool.h"

struct _HoloWorkerPool {
    Worker** workers;
    int numWorkers;
    MetaCache* metaCache;
    HoloConfig config;
    int status; //0:ready 1:started 2:stopped
    Metrics* metrics;
    pthread_mutex_t* idleMutex;
    pthread_cond_t* idleCond;
};

bool holo_client_submit_action_to_worker_pool(HoloWorkerPool*, Action*);

#endif