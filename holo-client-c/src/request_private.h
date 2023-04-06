#ifndef _REQUEST_PRIVATE_H_
#define _REQUEST_PRIVATE_H_

#include "../include/request.h"
#include "../include/table_schema.h"
#include "../include/holo_config.h"
#include "record_private.h"
#include "future.h"
#include "record.h"
#include "ilist.h"
#include <libpq-fe.h>

typedef struct _MetaRequest {
    HoloTableName tableName;
    Future* future;
} MetaRequest;
typedef MetaRequest* Meta;
Meta holo_client_new_meta_request(HoloTableName);
void holo_client_destroy_meta_request(Meta);

struct _HoloMutationRequest {
    HoloMutationMode mode;
    HoloRecord* record;
    dlist_head attachmentList;
    HoloWriteMode writeMode;
    int byteSize;
};
void holo_client_destroy_mutation_request(HoloMutation);
bool normalize_mutation_request(HoloMutation);
bool mutation_request_conflict(HoloMutation, HoloMutation);
HoloMutation mutation_request_merge(HoloMutation, HoloMutation);

typedef struct _MutationItem {
    dlist_node list_node;
    HoloMutation mutation;
} MutationItem;

MutationItem* create_mutation_item(HoloMutation mutation);

struct _HoloGetRequest {
    HoloRecord* record;
    Future* future;
    bool submitted;
};

typedef struct _GetItem {
    dlist_node list_node;
    HoloGet get;
} GetItem;

GetItem* create_get_item(HoloGet get);
typedef void* (*SqlFunction)(PGconn*, void*);

typedef struct _SqlRequest {
    SqlFunction sqlFunction;
    void* arg;
    Future* future;
} SqlRequest;
typedef SqlRequest* Sql;

Sql holo_client_new_sql_request(SqlFunction, void*);
void holo_client_destroy_sql_request(Sql);

bool set_record_val(HoloRecord* record, int colIndex, char* ptr, int format, int length);

#endif