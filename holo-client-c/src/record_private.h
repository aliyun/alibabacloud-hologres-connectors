#ifndef _RECORD_PRIVATE_H_
#define _RECORD_PRIVATE_H_

#include "table_schema_private.h"
#include "ilist.h"
#include "../include/record.h"

struct _HoloRecord{
    HoloTableSchema *schema;
    char **values;
    bool *valuesSet;
    int *valueLengths;
    int *valueFormats;
    int nValues;
    int byteSize;
};

HoloRecord* holo_client_new_record(HoloTableSchema*);
void holo_client_destroy_record(HoloRecord*);
bool record_conflict(HoloRecord*, HoloRecord*);
void* new_record_val(HoloRecord* , int);
void revoke_record_val(void*, HoloRecord*, int);
void destroy_record_val(HoloRecord*, int);

typedef struct _RecordItem {
    dlist_node list_node;
    HoloRecord* record;
} RecordItem;

RecordItem* create_record_item(HoloRecord*);

bool has_same_pk(HoloRecord*, HoloRecord*);
int record_pk_hash_code(HoloRecord* record, int size);

#endif