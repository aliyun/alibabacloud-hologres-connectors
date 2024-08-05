#include "mutation_map.h"
#include "logger_private.h"

MutationMap* holo_client_new_mutation_map(int size) {
    MutationMap* map = MALLOC(1, MutationMap);
    map->maxSize = size * 2;
    map->size = 0;
    map->byteSize = 0;
    map->mutations = MALLOC(map->maxSize, HoloMutation);
    for (int i = 0; i < map->maxSize; i++) {
        map->mutations[i] = NULL;
    }
    return map;
}

void holo_client_destroy_mutation_map(MutationMap* map) {
    FREE(map->mutations);
    FREE(map);
    map = NULL;
} 

int mutation_hash_code(HoloMutation mutation, int size) {
    return record_pk_hash_code(mutation->record, size);
}

void mutation_map_add(MutationMap* map, HoloMutation mutation, bool hasPK) {
    int M = map->maxSize;
    int index;
    if (!hasPK) {
        map->mutations[map->size] = mutation;
        map->size++;
        map->byteSize += mutation->byteSize;
        return;
    }
    index = mutation_hash_code(mutation, map->maxSize);
    for(int i = 0; i < map->maxSize; i++) {
        if(map->mutations[index] == NULL) {
            map->mutations[index] = mutation;
            map->size++;
            map->byteSize += mutation->byteSize;
            return;
        }
        if(has_same_pk(mutation->record, map->mutations[index]->record)) {
            map->byteSize -= map->mutations[index]->byteSize;
            map->mutations[index] = mutation_request_merge(map->mutations[index], mutation);
            map->byteSize += map->mutations[index]->byteSize;
            return;
        }
        index = (index + 1) % M;
    }
}

HoloMutation mutation_map_find_origin(MutationMap* map, HoloMutation mutation) {
    int index = mutation_hash_code(mutation, map->maxSize);
    int M = map->maxSize;
    for(int i = 0; i < map->maxSize; i++) {
        if(map->mutations[index] == NULL) {
            return NULL;
        }
        if(has_same_pk(mutation->record, map->mutations[index]->record)) {
            return map->mutations[index];
        }
        index = (index + 1) % M;
    }
    return NULL;
}