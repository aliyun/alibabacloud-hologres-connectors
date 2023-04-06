#ifndef _WORKER_H_
#define _WORKER_H_

#include <libpq-fe.h>
#include <pthread.h>
#include <stdbool.h>
#include "../include/holo_client.h"
#include "action.h"
#include "connection_holder.h"
#include "metrics.h"
#include "lp_map.h"

typedef struct _Worker Worker;

typedef struct _Worker {
    ConnectionHolder *connHolder;
    HoloConfig config;
    int status; //0: initialized 1: started 2: stopping 3: stopped 4: error
    int index;
    Action* action; //action the worker is currently working on
    pthread_t* thread;
    pthread_mutex_t* mutex;
    pthread_cond_t* cond;
    MetricsInWorker* metrics;
    long lastUpdateTime;
    pthread_mutex_t* idleMutex;
    pthread_cond_t* idleCond;
    LPMap* map;
} Worker;

Worker* holo_client_new_worker(HoloConfig, int, bool);
int holo_client_start_worker(Worker*);
int holo_client_stop_worker(Worker*);
void holo_client_close_worker(Worker*);

bool holo_client_try_submit_action_to_worker(Worker*, Action*);

ActionStatus handle_meta_action(ConnectionHolder* , Action*);
ActionStatus handle_mutation_action(ConnectionHolder* , Action*);
ActionStatus handle_sql_action(ConnectionHolder* , Action*);
ActionStatus handle_get_action(ConnectionHolder*, Action*);

void worker_abort_action(Worker*);
#endif