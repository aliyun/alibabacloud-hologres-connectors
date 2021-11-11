#ifndef _SQL_BUILDER_H_
#define _SQL_BUILDER_H_

#include "batch.h"

typedef struct _SqlCache{
    char* command;
    Oid* paramTypes;
    int* paramFormats;
    int* paramLengths;
} SqlCache;

char* build_insert_sql_with_batch(Batch*, int);
char* build_delete_sql_with_batch(Batch*, int);
char* build_get_sql(TableSchema*, int);

#endif