#include "worker_pool_private.h"
#include "utils.h"
#include "holo_config_private.h"
#include "logger.h"

HoloWorkerPool* holo_client_new_worker_pool(HoloConfig config, bool isFixedFe, int threadSize) {
    if (!holo_config_is_valid(&config)){
        LOG_ERROR("Holo config invalid.");
        return NULL;
    }
    int i;
    HoloWorkerPool* pool = MALLOC(1, HoloWorkerPool);
    pool->config = config;
    if (isFixedFe) {
        pool->config.connInfo = generate_fixed_fe_conn_info(config.connInfo);
    } else {
        pool->config.connInfo = deep_copy_string(config.connInfo);
    }
    pool->numWorkers = threadSize;
    pool->workers = MALLOC(pool->numWorkers, Worker*);
    pool->status = 0;
    pool->metaCache = holo_client_new_metacache();
    pool->metrics = holo_client_new_metrics(pool->numWorkers, config.reportInterval);
    pool->idleMutex = MALLOC(1, pthread_mutex_t);
    pool->idleCond = MALLOC(1, pthread_cond_t);
    pthread_mutex_init(pool->idleMutex, NULL);
    pthread_cond_init(pool->idleCond, NULL);
    for (i = 0; i < pool->numWorkers; i++) {
        pool->workers[i] = holo_client_new_worker(config, i, isFixedFe);
        pool->metrics->metricsList[i] = pool->workers[i]->metrics;
        pool->workers[i]->idleMutex = pool->idleMutex;
        pool->workers[i]->idleCond = pool->idleCond;
    }
    return pool;
} 

int holo_client_start_worker_pool(HoloWorkerPool* pool) {
    if (pool == NULL) {
        LOG_ERROR("Worker pool is NULL.");
        return -1;
    }
    int i, numFail;
    if (pool->status != 0) {
        return -1;
    }
    pool->status = 1;
    numFail = 0;
    for (i = 0; i < pool->numWorkers; i++) {
        if (holo_client_start_worker(pool->workers[i]) != 0) {
            numFail++;
        }
    }
    return numFail;
}

int holo_client_worker_pool_status(const HoloWorkerPool* pool) {
    if (pool == NULL) {
        LOG_ERROR("Worker pool is NULL.");
        return -1;
    }
    return pool->status;
}

int holo_client_stop_worker_pool(HoloWorkerPool* pool) {
    if (pool == NULL) {
        LOG_ERROR("Worker pool is NULL.");
        return -1;
    }
    int i;
    if (pool->status == 0) {
        pool->status = 2;
        return -1;
    }
    if (pool->status == 2) {
        return 0;
    }
    pool->status = 2;
    metrics_gather_and_show(pool->metrics);
    for (i = 0; i < pool->numWorkers; i++) {
        holo_client_stop_worker(pool->workers[i]);
    }
    return 0;
}

int holo_client_close_worker_pool(HoloWorkerPool* pool) {
    if (pool == NULL) {
        LOG_WARN("Worker pool is NULL.");
        return -1;
    }
    int i;
    if (pool->status != 2) {
        holo_client_stop_worker_pool(pool);
    }
    for (i = 0; i < pool->numWorkers; i++) {
        holo_client_close_worker(pool->workers[i]);
    }
    holo_client_destroy_metacache(pool->metaCache);
    holo_client_destroy_metrics(pool->metrics);
    FREE(pool->workers);
    FREE(pool->config.connInfo);
    FREE(pool->idleMutex);
    FREE(pool->idleCond);
    FREE(pool);
    pool = NULL;
    return 0;
}

bool holo_client_submit_action_to_worker_pool(HoloWorkerPool* pool, Action* action) {
    if (action->type == 1) metrics_histogram_update(pool->metrics->actionSize, ((MutationAction*)action)->numRequests);
    if (action->type == 3) metrics_histogram_update(pool->metrics->actionSize, ((GetAction*)action)->numRequests);
    pthread_mutex_lock(pool->idleMutex);
    while (1){
        for (int i = 0; i < pool->numWorkers; i++) {
            if (holo_client_try_submit_action_to_worker(pool->workers[i], action)) {
                pthread_mutex_unlock(pool->idleMutex);
                // if (action->type == 1) LOG_DEBUG("Submitted mutation action to worker %d, numRequests: %d", i, ((MutationAction*)action)->numRequests);
                // else LOG_DEBUG("Submitted action to worker %d", i);
                return true;
            }
        }
        struct timespec outTime = get_out_time(1000);
        pthread_cond_timedwait(pool->idleCond, pool->idleMutex, &outTime);
    }
    pthread_mutex_unlock(pool->idleMutex);
    return false;
}