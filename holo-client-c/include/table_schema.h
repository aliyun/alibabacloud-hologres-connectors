#ifndef _TABLE_SCHEMA_H_
#define _TABLE_SCHEMA_H_

//#include <libpq-fe.h>
#include <stdbool.h>

typedef struct _TableName TableName;

typedef struct _Column {
    char *name;
    char *quoted;
    unsigned int type;
    bool nullable;
    bool isPrimaryKey;
    char *defaultValue;
} Column;

typedef struct _TableSchema {
    unsigned int tableId;
    TableName* tableName;  //为了隐藏TableName结构，此处定义为指针
    int nColumns;
    Column *columns;
    int nDistributionKeys;
    int *distributionKeys; //column index
    //int *dictionaryEncoding;
    //int *bitmapIndexKey;
    //int *clusteringKey;
    //int *segmentKey;
    int nPrimaryKeys;
    int *primaryKeys;
    int partitionColumn;
} TableSchema;

#endif