#ifndef _TABLE_SCHEMA_H_
#define _TABLE_SCHEMA_H_

//#include <libpq-fe.h>
#include <stdbool.h>
#include "defs.h"

__HOLO_CLIENT_BEGIN_DECLS

typedef struct _HoloTableName HoloTableName;

typedef struct _HoloColumn {
    char *name;
    char *quoted;
    unsigned int type;
    bool nullable;
    bool isPrimaryKey;
    char *defaultValue;
} HoloColumn;

typedef struct _HoloTableSchema {
    unsigned int tableId;
    HoloTableName* tableName;  //为了隐藏HoloTableName结构，此处定义为指针
    int nColumns;
    HoloColumn *columns;
    int nDistributionKeys;
    int *distributionKeys; //column index
    //int *dictionaryEncoding;
    //int *bitmapIndexKey;
    //int *clusteringKey;
    //int *segmentKey;
    int nPrimaryKeys;
    int *primaryKeys;
    int partitionColumn;
} HoloTableSchema;

//通过HoloColumn的type字段（无符号整型数字），可以获取HoloColumn的类型（字符串）
const char* holo_client_get_type_name_with_type_oid(unsigned int typeOid);

__HOLO_CLIENT_END_DECLS

#endif