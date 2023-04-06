#ifndef _LP_MAP_H_
#define _LP_MAP_H_

#include "murmur3.h"
#include "utils.h"

typedef bool (*ValueComparer)(void*, void*);

typedef struct _LPMap {
    int maxSize;
    int size;
    void** values;
} LPMap;

LPMap* holo_client_new_lp_map(int size);

void holo_client_destroy_lp_map(LPMap* map);

void holo_client_clear_lp_map(LPMap* map);

// void lp_map_add(LPMap* map, int index, void* value, ValueComparer comparer);

#endif