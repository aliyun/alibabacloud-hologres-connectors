#include "batch.h"
#include "ilist.h"
#include "logger.h"
#include "request_private.h"
#include "record_private.h"

BatchItem* create_batch_item(Batch* batch){
    BatchItem* item = MALLOC(1, BatchItem);
    item->batch = batch;
    return item;
}

Batch* holo_client_new_batch_with_record(HoloRecord* record){
    Batch* ret = MALLOC(1, Batch);
    ret->schema = record->schema;
    void* mPool = MALLOC(ret->schema->nColumns * (sizeof(bool) + 2 * sizeof(int)), char);
    ret->valuesSet = mPool;
    ret->valueFormats = mPool + ret->schema->nColumns * sizeof(bool);
    ret->valueLengths = mPool + ret->schema->nColumns * (sizeof(bool) + sizeof(int));
    for (int i = 0;i < ret->schema->nColumns;i++){
        ret->valuesSet[i] = record->valuesSet[i];
        ret->valueFormats[i] = record->valueFormats[i];
        ret->valueLengths[i] = record->valueLengths[i];
    }
    ret->nValues = record->nValues;
    dlist_init(&(ret->recordList));
    dlist_push_tail(&(ret->recordList), &(create_record_item(record)->list_node));
    ret->nRecords = 1;
    return ret;
}

Batch* holo_client_new_batch_with_mutation_request(HoloMutation mutation){
    Batch* ret = holo_client_new_batch_with_record(mutation->record);
    ret->mode = mutation->mode;
    ret->writeMode = mutation->writeMode;
    ret->isSupportUnnest = false;
    return ret;
}

Batch* holo_client_clone_batch_without_records(Batch* batch){
    Batch* ret = MALLOC(1, Batch);
    ret->mode = batch->mode;
    ret->schema = batch->schema;
    ret->writeMode = batch->writeMode;
    ret->isSupportUnnest = batch->isSupportUnnest;
    void* mPool = MALLOC(ret->schema->nColumns * (sizeof(bool) + 2 * sizeof(int)), char);
    ret->valuesSet = mPool;
    ret->valueFormats = mPool + ret->schema->nColumns * sizeof(bool);
    ret->valueLengths = mPool + ret->schema->nColumns * (sizeof(bool) + sizeof(int));
    for (int i = 0;i < ret->schema->nColumns;i++){
        ret->valuesSet[i] = batch->valuesSet[i];
        ret->valueFormats[i] = batch->valueFormats[i];
    }
    ret->nValues = batch->nValues;
    dlist_init(&(ret->recordList));
    ret->nRecords = batch->nRecords;
    return ret;
}

void holo_client_destroy_batch(Batch* batch){
    FREE(batch->valuesSet);  //释放了整个mPool
    // FREE(batch->valueFormats);
    // FREE(batch->valueLengths);
    dlist_mutable_iter miter;
    RecordItem* recordItem;
    dlist_foreach_modify(miter, &(batch->recordList)) {
        recordItem = dlist_container(RecordItem, list_node, miter.cur);
        dlist_delete(miter.cur);
        FREE(recordItem);
    }
    FREE(batch);
}

bool batch_can_apply_normalized_record(Batch* batch, HoloRecord* record){
    if (record->schema != batch->schema) return false;
    for (int i = 0;i < record->schema->nColumns;i++){
        if (record->valueFormats[i] != batch->valueFormats[i]) return false;
    }
    return true;
}

bool batch_try_apply_normalized_record(Batch* batch, HoloRecord* record){
    if (!batch_can_apply_normalized_record(batch, record)) return false;
    dlist_push_tail(&(batch->recordList), &(create_record_item(record)->list_node));
    batch->nRecords += 1;
    return true;
}

bool batch_can_apply_update_record(Batch* batch, HoloRecord* record){
    if (record->schema != batch->schema) return false;
    if (record->nValues != batch->nValues) return false;
    for (int i = 0;i < batch->schema->nColumns;i++) {
        if (record->valuesSet[i] != batch->valuesSet[i]) return false;
        if (!record->valuesSet[i]) continue;
        if (record->valueFormats[i] != batch->valueFormats[i]) return false;
    }
    return true;
}

bool batch_try_apply_update_record(Batch* batch, HoloRecord* record){
    if (!batch_can_apply_update_record(batch, record)) return false;
    dlist_push_tail(&(batch->recordList), &(create_record_item(record)->list_node));
    batch->nRecords += 1;
    return true;
}

bool batch_can_apply_mutation_request(Batch* batch, HoloMutation mutation){
    if (mutation->mode != batch->mode) return false;
    if (mutation->writeMode != batch->writeMode) return false;
    return true;
}

bool batch_try_apply_mutation_request(Batch* batch, HoloMutation mutation){
    if (!batch_can_apply_mutation_request(batch, mutation)) return false;
    if (batch->mode == PUT && batch->writeMode == INSERT_OR_UPDATE){
        if (!batch_try_apply_update_record(batch, mutation->record)) return false;
    }
    else if (!batch_try_apply_normalized_record(batch, mutation->record)) return false;
    return true;
}

bool batch_matches(Batch* a, Batch* b, int nRecords){
    if (a->mode != b->mode) return false;
    if (a->writeMode != b->writeMode) return false;
    if (a->isSupportUnnest != b->isSupportUnnest) return false;
    if (a->schema != b->schema) return false;
    if (a->nValues != b->nValues) return false;
    if (a->nRecords != (nRecords == 0 ? b->nRecords : nRecords)) return false;
    for (int i = 0;i < a->schema->nColumns;i++) {
        if (a->valuesSet[i] != b->valuesSet[i]) return false;
        if (!a->valuesSet[i]) continue;
        if (a->valueFormats[i] != b->valueFormats[i]) return false;
    }
    return true;
}