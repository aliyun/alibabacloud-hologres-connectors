#ifndef _MUTATION_COLLECTOR_H_
#define _MUTATION_COLLECTOR_H_
//AutoFlush request数大于batchSize或者等待时间大于writeMaxIntervalMs就自动flush

#include <pthread.h>
#include "request_private.h"
#include "action.h"
#include "ilist.h"
#include "worker_pool_private.h"
#include "mutation_map.h"

typedef struct _ShardCollector {
    HoloWorkerPool* pool;
    MutationMap* map;
    int numRequests;
    pthread_mutex_t* mutex;
    long startTime;
    int batchSize;
    long writeMaxIntervalMs;
    MutationAction* activeAction;
    bool hasPK;
    long byteSize;
    long maxByteSize;
    bool enableAutoFlush;
} ShardCollector;

ShardCollector* holo_client_new_shard_collector(HoloWorkerPool*, int, long, bool, long, bool);
int holo_client_add_request_to_shard_collector(ShardCollector*, HoloMutation, long*, char**);
void holo_client_destroy_shard_collector(ShardCollector*);
int holo_client_flush_shard_collector(ShardCollector*, char**);
void holo_client_try_flush_shard_collector(ShardCollector*);
int do_flush_shard_collector(ShardCollector*, char** errMsgAddr);

typedef struct _TableCollector {
    HoloTableSchema* schema;
    HoloWorkerPool* pool;
    ShardCollector** shardCollectors;
    int nShardCollectors;
    int numRequests;
    bool hasPK;
    long byteSize;
    long maxByteSize;
    bool enableAutoFlush;
} TableCollector;

TableCollector* holo_client_new_table_collector(HoloTableSchema*, HoloWorkerPool*, int, long, long, bool);
int holo_client_add_request_to_table_collector(TableCollector*, HoloMutation, long*, char**);
void holo_client_destroy_table_collector(TableCollector*);
int holo_client_flush_table_collector(TableCollector*, char**);
void holo_client_try_flush_table_collector(TableCollector*);

typedef struct _TableCollectorItem {
    dlist_node list_node;
    TableCollector* tableCollector;
} TableCollectorItem;

typedef struct _MutationCollector {
    HoloWorkerPool* pool;
    dlist_head tableCollectors;
    int numTables;
    pthread_t* autoFlush;
    pthread_rwlock_t* rwLock;
    int batchSize;
    long writeMaxIntervalMs;
    int status; //0:ready 1:started 2:stopping 3:stopped 4:error
    long byteSize;
    long maxByteSize;
    long maxTotalByteSize;
    bool enableAutoFlush;
} MutationCollector;

MutationCollector* holo_client_new_mutation_collector(HoloWorkerPool*, HoloConfig);
int holo_client_start_watch_mutation_collector(MutationCollector*);
int holo_client_add_request_to_mutation_collector(MutationCollector*, HoloMutation, char**);
int holo_client_stop_watch_mutation_collector(MutationCollector*);
void holo_client_destroy_mutation_collector(MutationCollector*);
int holo_client_flush_mutation_collector(MutationCollector*, char**);

#endif