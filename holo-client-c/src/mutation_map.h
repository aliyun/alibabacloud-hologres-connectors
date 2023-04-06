#ifndef _MUTATION_MAP_H_
#define _MUTATION_MAP_H_

#include "request_private.h"
#include "murmur3.h"
#include "ilist.h"
#include "utils.h"

//linear probing
typedef struct _MutationMap {
    int maxSize;
    int size;
    long byteSize;
    HoloMutation* mutations;
} MutationMap;

MutationMap* holo_client_new_mutation_map(int size);

void holo_client_destroy_mutation_map(MutationMap* map);

int mutation_hash_code(HoloMutation mutation, int size);

void mutation_map_add(MutationMap* map, HoloMutation mutation, bool hasPK);

HoloMutation mutation_map_find_origin(MutationMap* map, HoloMutation mutation);

#endif