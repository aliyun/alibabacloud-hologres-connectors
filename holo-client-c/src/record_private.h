#ifndef _RECORD_PRIVATE_H_
#define _RECORD_PRIVATE_H_

#include "table_schema_private.h"
#include "ilist.h"
#include "../include/record.h"

struct _Record{
    TableSchema *schema;
    char **values;
    bool *valuesSet;
    int *valueLengths;
    int *valueFormats;
    int nValues;
    int byteSize;
};

Record* holo_client_new_record(TableSchema*);
void holo_client_destroy_record(Record*);
bool record_conflict(Record*, Record*);
void* new_record_val(Record* , int);
void revoke_record_val(void*, Record*, int);
void destroy_record_val(Record*, int);

typedef struct _RecordItem {
    dlist_node list_node;
    Record* record;
} RecordItem;

RecordItem* create_record_item(Record*);

bool has_same_pk(Record*, Record*);
int record_pk_hash_code(Record* record, int size);

#endif