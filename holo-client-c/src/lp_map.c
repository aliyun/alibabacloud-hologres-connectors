#include "lp_map.h"

LPMap* holo_client_new_lp_map(int size) {
    LPMap* map = MALLOC(1, LPMap);
    map->maxSize = size * 2;
    map->size = 0;
    map->values = MALLOC(map->maxSize, void*);
    for (int i = 0; i < map->maxSize; i++) {
        map->values[i] = NULL;
    }
    return map;
}

void holo_client_destroy_lp_map(LPMap* map) {
    FREE(map->values);
    FREE(map);
    map = NULL;
}

void holo_client_clear_lp_map(LPMap* map) {
    for (int i = 0; i < map->maxSize; i++) {
        map->values[i] = NULL;
    }
}

// void lp_map_add(LPMap* map, int index, void* value, ValueComparer equals) {
//     int M = map->maxSize;
//     for(int i = 0; i < M; i++) {
//         if(map->values[index] == NULL) {
//             map->values[index] = value;
//             map->size++;
//             return;
//         }
//         if(equals(value, map->values[index])) {
//             return;
//         }
//         index = (index + 1) % M;
//     }
// }
