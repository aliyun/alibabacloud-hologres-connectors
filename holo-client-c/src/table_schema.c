#include "table_schema.h"
#include "table_schema_private.h"
#include "logger_private.h"
#include "utils.h"

void holo_client_destroy_tablename(HoloTableName* tableName) {
    FREE(tableName->fullName);
    tableName->fullName = NULL;
    FREE(tableName->schemaName);
    tableName->schemaName = NULL;
    FREE(tableName->tableName);
    tableName->tableName = NULL;
}

void holo_client_destroy_columns(HoloColumn* columns, int n) {
    if (columns == NULL) {
        return;
    }
    for (int i = 0; i < n; i++) {
        FREE(columns[i].name);
        FREE(columns[i].quoted);
        FREE(columns[i].defaultValue);
    }
    FREE(columns);
    columns = NULL;
}

HoloColumn* holo_client_new_columns(int n) {
    HoloColumn* columns = MALLOC(n, HoloColumn);
    for (int i = 0; i < n; i++) {
        columns[i].name = NULL;
        columns[i].quoted = NULL;
        columns[i].defaultValue = NULL;
    }
    return columns;
}

HoloTableSchema* holo_client_new_tableschema() {
    HoloTableSchema* schema = MALLOC(1, HoloTableSchema);
    schema->tableName = MALLOC(1, HoloTableName);
    schema->tableName->fullName = NULL;
    schema->tableName->schemaName = NULL;
    schema->tableName->tableName = NULL;
    schema->columns = NULL;
    schema->nColumns = 0;
    schema->nDistributionKeys = 0;
    schema->distributionKeys = NULL;
    //schema->dictionaryEncoding = NULL;
    //schema->bitmapIndexKey = NULL;
    //schema->clusteringKey = NULL;
    //schema->segmentKey = NULL;
    schema->primaryKeys = NULL;
    schema->nPrimaryKeys = 0;
    schema->partitionColumn = -1;
    return schema;
}

void holo_client_destroy_tableschema(HoloTableSchema* schema) {
    LOG_DEBUG("destory table schema:%s", schema->tableName->fullName);
    holo_client_destroy_tablename(schema->tableName);
    FREE(schema->tableName);
    holo_client_destroy_columns(schema->columns, schema->nColumns);
    FREE(schema->distributionKeys);
    //FREE(schema->dictionaryEncoding);
    //FREE(schema->bitmapIndexKey);
    //FREE(schema->clusteringKey);
    //FREE(schema->segmentKey);
    FREE(schema->primaryKeys);
    FREE(schema);
    schema = NULL;
}

int get_colindex_by_colname(HoloTableSchema* schema, const char* colName) {
    for (int i = 0; i < schema->nColumns; i++) {
        if (strcmp(colName, schema->columns[i].name) == 0) {
            return i;
        }
    }
    return -1;
}

bool table_has_pk(HoloTableSchema* schema) {
    return (schema->nPrimaryKeys > 0);
}

const char* holo_client_get_column_name(HoloTableSchema* schema, int colIndex) {
    if (schema == NULL || schema->columns == NULL) {
        LOG_ERROR("Invalid table schema!");
        return NULL;
    }
    if (colIndex < 0 || colIndex >= schema->nColumns) {
        LOG_ERROR("Invalid column index!");
        return NULL;
    }
    return schema->columns[colIndex].name;
}

const char* holo_client_get_column_type_name(HoloTableSchema* schema, int colIndex) {
    if (schema == NULL || schema->columns == NULL) {
        LOG_ERROR("Invalid table schema!");
        return NULL;
    }
    if (colIndex < 0 || colIndex >= schema->nColumns) {
        LOG_ERROR("Invalid column index!");
        return NULL;
    }
    return holo_client_get_type_name_with_type_oid(schema->columns[colIndex].type);
}