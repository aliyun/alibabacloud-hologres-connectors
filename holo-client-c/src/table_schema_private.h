#ifndef _TABLE_SCHEMA_PRIVATE_H_
#define _TABLE_SCHEMA_PRIVATE_H_

#include "../include/table_schema.h"

struct _TableName {
    char *schemaName; //no "" e.g. = "schemaName"
    char *tableName; //no "" e.g. = "tableName"
    char *fullName; //has "" e.g. = "\"schemaName\".\"tableName\""
};

void holo_client_destroy_tablename(TableName*);

void holo_client_destroy_columns(Column*, int);

Column* holo_client_new_columns(int);

TableSchema* holo_client_new_tableschema();
void holo_client_destroy_tableschema(TableSchema*);

int get_colindex_by_colname(TableSchema*, char*);

bool table_has_pk(TableSchema*);

#endif