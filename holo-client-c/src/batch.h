#ifndef _BATCH_H_
#define _BATCH_H_

#include "table_schema.h"
#include "ilist.h"
#include "stdbool.h"
#include "record.h"
#include "utils.h"
#include "request_private.h"
#include "holo_config.h"

typedef struct _Batch{
    TableSchema *schema;
    bool* valuesSet;
    int* valueFormats;
    int* valueLengths;
    int nValues;
    dlist_head recordList;
    int nRecords;
    HoloMutationMode mode;
    HoloWriteMode writeMode;
} Batch;

typedef struct _BatchItem {
    dlist_node list_node;
    Batch* batch;
} BatchItem;

BatchItem* create_batch_item(Batch*);

Batch* holo_client_new_batch_with_record(Record*);
Batch* holo_client_new_batch_with_mutation_request(Mutation);
Batch* holo_client_clone_batch_without_records(Batch*);
void holo_client_destroy_batch(Batch*);
bool batch_can_apply_normalized_record(Batch*, Record*);
bool batch_try_apply_normalized_record(Batch*, Record*);
bool batch_can_apply_update_record(Batch*, Record*);
bool batch_try_apply_update_record(Batch*, Record*);
bool batch_can_apply_mutation_request(Batch*, Mutation);
bool batch_try_apply_mutation_request(Batch*, Mutation);
bool batch_matches(Batch*, Batch*, int);

#endif