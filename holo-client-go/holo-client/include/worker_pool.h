#ifndef _WORKER_POOL_H_
#define _WORKER_POOL_H_

#include "holo_config.h"

struct _WorkerPool;
typedef struct _WorkerPool WorkerPool;

WorkerPool* holo_client_new_worker_pool(HoloConfig);
int holo_client_start_worker_pool(WorkerPool*);
int holo_client_worker_pool_status(WorkerPool*);
int holo_client_stop_worker_pool(WorkerPool*);
int holo_client_close_worker_pool(WorkerPool*);

#endif