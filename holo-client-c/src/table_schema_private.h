#ifndef _TABLE_SCHEMA_PRIVATE_H_
#define _TABLE_SCHEMA_PRIVATE_H_

#include "../include/table_schema.h"

struct _HoloTableName {
    char *schemaName; //no "" e.g. = "schemaName"
    char *tableName; //no "" e.g. = "tableName"
    char *fullName; //has "" e.g. = "\"schemaName\".\"tableName\""
};

void holo_client_destroy_tablename(HoloTableName*);

void holo_client_destroy_columns(HoloColumn*, int);

HoloColumn* holo_client_new_columns(int);

HoloTableSchema* holo_client_new_tableschema();
void holo_client_destroy_tableschema(HoloTableSchema*);

int get_colindex_by_colname(HoloTableSchema*, const char* );

bool table_has_pk(HoloTableSchema*);

#endif