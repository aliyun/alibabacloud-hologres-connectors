#include "meta_cache.h"
#include "utils.h"
#include "table_schema_private.h"
#include "logger_private.h"

SchemaItem* create_schema_item(HoloTableSchema* schema) {
    SchemaItem* item = MALLOC(1, SchemaItem);
    item->schema = schema;
    return item;
}

ParentItem* create_parent_item(HoloTableSchema** parent) {
    ParentItem* item = MALLOC(1, ParentItem);
    item->parent = parent;
    dlist_init(&(item->partitions));
    return item;
}

PartitionItem* create_partition_item(HoloTableSchema** partition, char* value) {
    PartitionItem* item = MALLOC(1, PartitionItem);
    item->partition = partition;
    item->value = deep_copy_string(value);
    return item;
}

MetaCache* holo_client_new_metacache() {
    MetaCache* cache = MALLOC(1, MetaCache);
    dlist_init(&(cache->schemaList));
    dlist_init(&(cache->parentList));
    cache->rwlock = MALLOC(1, pthread_rwlock_t);
    pthread_rwlock_init(cache->rwlock, NULL);
    return cache;
}

void clear_all_contents(MetaCache* cache) {
    SchemaItem* schemaItem;
    ParentItem* parentItem;
    PartitionItem* partitionItem;
    HoloTableSchema* schema = NULL;
    dlist_mutable_iter miter, miter2;
    dlist_foreach_modify(miter, &(cache->parentList)) {
        parentItem = dlist_container(ParentItem, list_node, miter.cur);
        dlist_foreach_modify(miter2, &(parentItem->partitions)) {
            partitionItem = dlist_container(PartitionItem, list_node, miter2.cur); 
            dlist_delete(miter2.cur);
            FREE(partitionItem->value);
            FREE(partitionItem); 
        }
        dlist_delete(miter.cur);
        FREE(parentItem);
    }
    dlist_foreach_modify(miter, &(cache->schemaList)) {
        schemaItem = dlist_container(SchemaItem, list_node, miter.cur);
        schema = schemaItem->schema;
        dlist_delete(miter.cur);
        holo_client_destroy_tableschema(schema);
        FREE(schemaItem);
    }
}

void holo_client_destroy_metacache(MetaCache* cache) {
    clear_all_contents(cache);
    pthread_rwlock_destroy(cache->rwlock);
    FREE(cache->rwlock);
    FREE(cache);
    cache = NULL;
}

HoloTableSchema* find_tableschema_in_metacache(MetaCache* cache, HoloTableName name) {
    HoloTableSchema* schema = NULL;
    dlist_iter	iter;
    SchemaItem* schemaItem;
    pthread_rwlock_rdlock(cache->rwlock);
    dlist_foreach(iter, &cache->schemaList) {
        schemaItem = dlist_container(SchemaItem, list_node, iter.cur);
        if (strcmp(name.fullName, schemaItem->schema->tableName->fullName) == 0) {
            schema = schemaItem->schema;
            break;
        }
    }
    pthread_rwlock_unlock(cache->rwlock);
    return schema;
}

void add_tableschema_to_metacache(MetaCache* cache, HoloTableSchema* schema) {
    dlist_iter	iter;
    SchemaItem* schemaItem;
    bool updated = false;
    pthread_rwlock_wrlock(cache->rwlock); 
    dlist_foreach(iter, &cache->schemaList) {
        schemaItem = dlist_container(SchemaItem, list_node, iter.cur);
        if (strcmp(schema->tableName->fullName, schemaItem->schema->tableName->fullName) == 0) {
            holo_client_destroy_tableschema(schemaItem->schema);
            schemaItem->schema = schema;
            updated = true;
            break;
        }
    }
    if(!updated) {
        schemaItem = create_schema_item(schema);
        dlist_push_tail(&(cache->schemaList), &(schemaItem->list_node));
        if (schema->partitionColumn > -1) {
            //parent table
            dlist_push_tail(&(cache->parentList), &(create_parent_item(&(schemaItem->schema))->list_node));
        }
    }
    pthread_rwlock_unlock(cache->rwlock);
}

HoloTableSchema* meta_cache_find_partition(MetaCache* cache, HoloTableSchema* schema, char* value) {
    dlist_iter	iter, iter2;
    HoloTableSchema* partition = NULL;
    ParentItem* parentItem;
    PartitionItem* partitionItem;
    pthread_rwlock_rdlock(cache->rwlock);
    dlist_foreach(iter, &cache->parentList) {
        parentItem = dlist_container(ParentItem, list_node, iter.cur);
        if (*(parentItem->parent) == schema) {
            dlist_foreach(iter2, &parentItem->partitions) {
                partitionItem = dlist_container(PartitionItem, list_node, iter2.cur);
                if (strcmp(partitionItem->value, value) == 0) {
                    partition = *(partitionItem->partition);
                    break;
                }
            }
            break;
        }
    }
    pthread_rwlock_unlock(cache->rwlock);
    return partition;
}

void meta_cache_add_partition(MetaCache* cache, HoloTableSchema* parent, HoloTableSchema* partition, char* value) {
    dlist_iter	iter;
    ParentItem* parentItem = NULL;
    SchemaItem* schemaItem = NULL;
    pthread_rwlock_wrlock(cache->rwlock); 
    dlist_foreach(iter, &cache->parentList) {
        parentItem = dlist_container(ParentItem, list_node, iter.cur);
        if (*(parentItem->parent) == parent) {
            break;
        }
    }
    dlist_foreach(iter, &cache->schemaList) {
        schemaItem = dlist_container(SchemaItem, list_node, iter.cur);
        if (schemaItem->schema == partition) {
            break;
        }
    }
    if (parentItem != NULL && schemaItem != NULL) {
        dlist_push_tail(&(parentItem->partitions), &(create_partition_item(&(schemaItem->schema), value)->list_node));
    }
    pthread_rwlock_unlock(cache->rwlock);
}