#ifndef _META_CACHE_H_
#define _META_CACHE_H_

#include "../include/table_schema.h"
#include <pthread.h>
#include <time.h>
#include "ilist.h"

typedef struct _MetaCache {
    dlist_head schemaList;
    dlist_head parentList;
    pthread_rwlock_t* rwlock;
} MetaCache;

typedef struct _SchemaItem {
    dlist_node list_node;
    TableSchema* schema;
    time_t age;
} SchemaItem;

typedef struct _ParentItem {
    dlist_node list_node;
    TableSchema** parent;
    dlist_head partitions;
} ParentItem;

typedef struct _PartitionItem {
    dlist_node list_node;
    char* value;
    TableSchema** partition;
} PartitionItem;

MetaCache* holo_client_new_metacache();
void holo_client_destroy_metacache(MetaCache*);
void clear_all_contents(MetaCache*);

TableSchema* find_tableschema_in_metacache(MetaCache*, TableName);
void add_tableschema_to_metacache(MetaCache*, TableSchema*);
TableSchema* meta_cache_find_partition(MetaCache*, TableSchema*, char*);
void meta_cache_add_partition(MetaCache*, TableSchema*, TableSchema*, char*);

#endif