#ifndef _WORKER_POOL_H_
#define _WORKER_POOL_H_

#include "defs.h"
#include "holo_config.h"

__HOLO_CLIENT_BEGIN_DECLS

struct _HoloWorkerPool;
typedef struct _HoloWorkerPool HoloWorkerPool;

HoloWorkerPool* holo_client_new_worker_pool(HoloConfig, bool, int);
int holo_client_start_worker_pool(HoloWorkerPool*);
int holo_client_worker_pool_status(const HoloWorkerPool*);
int holo_client_stop_worker_pool(HoloWorkerPool*);
int holo_client_close_worker_pool(HoloWorkerPool*);

__HOLO_CLIENT_END_DECLS

#endif