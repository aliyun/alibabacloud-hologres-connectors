#include "sql_builder.h"
#include "logger_private.h"
#include "unistd.h"

const char* holo_client_get_type_name_with_type_oid(unsigned int typeOid) {
    const char* ret = NULL;
    switch (typeOid)
    {
    case HOLO_TYPE_BOOL:
        ret = "bool";
        break;
    case HOLO_TYPE_INT8:
        ret = "int8";
        break;
    case HOLO_TYPE_INT4:
        ret = "int4";
        break;
    case HOLO_TYPE_INT2:
        ret = "smallint";
        break;
    case HOLO_TYPE_FLOAT4:
        ret = "float4";
        break;
    case HOLO_TYPE_FLOAT8:
        ret = "float8";
        break;
    case HOLO_TYPE_CHAR:
        ret = "char";
        break;
    case HOLO_TYPE_VARCHAR:
        ret = "varchar";
        break;
    case HOLO_TYPE_TEXT:
        ret = "text";
        break;
    case HOLO_TYPE_BYTEA:
        ret = "bytea";
        break;
    case HOLO_TYPE_JSON:
        ret = "json";
        break;
    case HOLO_TYPE_JSONB:
        ret = "jsonb";
        break;
    case HOLO_TYPE_TIMESTAMP:
        ret = "timestamp";
        break;
    case HOLO_TYPE_TIMESTAMPTZ:
        ret = "timestamptz";
        break;
    case HOLO_TYPE_DATE:
        ret = "date";
        break;
    case HOLO_TYPE_NUMERIC:
        ret = "numeric";
        break;
    default:
        LOG_ERROR("Unsupported type oid: %d.", typeOid);
        break;
    }
    return ret;
}

char* build_unnest_insert_sql_with_batch(Batch* batch){
    if (batch->mode != PUT) return NULL;
    int length = 12; // "INSERT INTO "
    length += strlen(batch->schema->tableName->fullName);
    length += 2; // " ("
    for (int i = 0; i < batch->schema->nColumns; i++) {
        if (!batch->valuesSet[i]) continue;
        length += strlen(batch->schema->columns[i].quoted) + 1;
    }
    length += 8; // " SELECT "
    int count = 0;
    for (int i = 0; i < batch->schema->nColumns; i++) {
        if (!batch->valuesSet[i]) continue;
        length += 13 + strlen(holo_client_get_type_name_with_type_oid(batch->schema->columns[i].type)); // "unnest(::<typeName>[]),"
        length += 1 + len_of_int(++count); // "$<count>"
    }
    if (batch->schema->nPrimaryKeys != 0) {
        length += 17; //"on conflict ("," do "
        for (int i = 0;i < batch->schema->nPrimaryKeys;i++){
            length += strlen(batch->schema->columns[batch->schema->primaryKeys[i]].quoted) + 1; //"<pk>,"
        }
        if (batch->writeMode == INSERT_OR_IGNORE){
            length += 7; // "nothing"
        }
        else {
            length += 11; // "update set " 
            for (int i = 0;i < batch->schema->nColumns;i++){
                if (!batch->valuesSet[i]) continue;
                length += strlen(batch->schema->columns[i].quoted) * 2 + 10 + 1; //"<column>=excluded.<column>,"
            }
            length -= 1;// 最后减一个","
        }
    }
    char* sql = MALLOC(length + 1, char);
    strncpy(sql, "INSERT INTO ", 13);
    int position = 12;
    count = 0;
    strncpy(sql + position, batch->schema->tableName->fullName, strlen(batch->schema->tableName->fullName));
    position += strlen(batch->schema->tableName->fullName);
    strncpy(sql + position, " (", 3);
    position += 2;
    for (int i = 0;i < batch->schema->nColumns;i++){
        if (!batch->valuesSet[i]) continue;
        if (count > 0) {
            strncpy(sql + position, ",", 2);
            position += 1;
        }
        strncpy(sql + position, batch->schema->columns[i].quoted, strlen(batch->schema->columns[i].quoted));
        position += strlen(batch->schema->columns[i].quoted);
        count ++;
    }
    strncpy(sql + position, ") SELECT ", 10);
    position += 9;
    count = 0;
    for (int i = 0;i < batch->schema->nColumns;i++){
        if (!batch->valuesSet[i]) continue;
        if (count > 0) {
            strncpy(sql + position, ",", 2);
            position += 1;
        }
        strncpy(sql + position, "unnest($", 9);
        position += 8;
        char* number = itoa(++count);
        strncpy(sql + position, number, len_of_int(count));
        position += len_of_int(count);
        FREE(number);
        strncpy(sql + position, "::", 3);
        position += 2;
        const char* typeName = holo_client_get_type_name_with_type_oid(batch->schema->columns[i].type);
        strncpy(sql + position, typeName, strlen(typeName));
        position += strlen(typeName);
        strncpy(sql + position, "[])", 4);
        position += 3;
    }
    if (batch->schema->nPrimaryKeys != 0) {
        strncpy(sql + position, " on conflict (", 15);
        position += 14;
        bool first = true;
        for (int i = 0;i < batch->schema->nPrimaryKeys;i++){
            int index = batch->schema->primaryKeys[i];
            if (first) first = false;
            else {
                strncpy(sql + position, ",", 2);
                position += 1;
            }
            strncpy(sql + position, batch->schema->columns[index].quoted, strlen(batch->schema->columns[index].quoted));
            position += strlen(batch->schema->columns[index].quoted);
        }
        strncpy(sql + position, ") do ", 6);
        position += 5;
        if (batch->writeMode == INSERT_OR_IGNORE){
            strncpy(sql + position, "nothing", 8);
            position += 7;
        }
        else {  //若为INSERT_OR_REPLACE，则record已被normalize
            strncpy(sql + position, "update set ", 12);
            position += 11;
            first = true;
            for (int i = 0;i < batch->schema->nColumns;i++){
                if (!batch->valuesSet[i]) continue;
                if (first) first = false;
                else {
                    strncpy(sql + position, ",", 2);
                    position += 1;
                }
                strncpy(sql + position, batch->schema->columns[i].quoted, strlen(batch->schema->columns[i].quoted));
                position += strlen(batch->schema->columns[i].quoted);
                strncpy(sql + position, "=excluded.", 11);
                position += 10;
                strncpy(sql + position, batch->schema->columns[i].quoted, strlen(batch->schema->columns[i].quoted) + 1);
                position += strlen(batch->schema->columns[i].quoted);
            }
        }
    }
    // LOG_DEBUG("unnest sql is %s", sql);
    return sql;
}

char* build_insert_sql_with_batch(Batch* batch, int nRecords){
    if (batch->mode != PUT) return NULL;
    if (nRecords == 0) nRecords = batch->nRecords;
    int length = 23 - (2 + nRecords);
    length += strlen(batch->schema->tableName->fullName);
    for (int i = 0;i < batch->schema->nColumns;i++){
        if (!batch->valuesSet[i]) continue;
        length += strlen(batch->schema->columns[i].quoted) + 1;
    }
    int count = 0;
    for (int i = 0;i < nRecords;i++){
        for (int j = 0;j < batch->nValues;j++){
            length += 2 + len_of_int(++count);
        }
        length += 3;
    }
    if (batch->schema->nPrimaryKeys != 0) {
        length += 19 - 1;
        for (int i = 0;i < batch->schema->nPrimaryKeys;i++){
            length += strlen(batch->schema->columns[batch->schema->primaryKeys[i]].quoted) + 1;
        }
        if (batch->writeMode == INSERT_OR_IGNORE){
            length += 7;
        }
        else {
            length += 11 -1;
            for (int i = 0;i < batch->schema->nColumns;i++){
                if (!batch->valuesSet[i]) continue;
                length += strlen(batch->schema->columns[i].quoted) * 2 + 10 + 1;
            }
        }
    }

    char* sql = MALLOC(length + 1, char);
    strncpy(sql, "INSERT INTO ", 13);
    int position = 12;
    count = 0;
    strncpy(sql + position, batch->schema->tableName->fullName, strlen(batch->schema->tableName->fullName));
    position += strlen(batch->schema->tableName->fullName);
    strncpy(sql + position, " (", 3);
    position += 2;
    for (int i = 0;i < batch->schema->nColumns;i++){
        if (!batch->valuesSet[i]) continue;
        if (count > 0) {
            strncpy(sql + position, ",", 2);
            position += 1;
        }
        strncpy(sql + position, batch->schema->columns[i].quoted, strlen(batch->schema->columns[i].quoted));
        position += strlen(batch->schema->columns[i].quoted);
        count ++;
    }
    strncpy(sql + position, ") VALUES ", 10);
    position += 9;
    count = 0;
    for (int j = 0; j < nRecords;j++){
        if (count > 0) {
            strncpy(sql + position, ",", 2);
            position += 1;
        }
        strncpy(sql + position, "(", 2);
        position += 1;
        int count_inside = 0;
        for (int i = 0;i < batch->schema->nColumns;i++){
            if (!batch->valuesSet[i]) continue;
            if (count_inside > 0) {
                strncpy(sql + position, ",", 2);
                position += 1;
            }
            strncpy(sql + position, "$", 2);
            position += 1;
            char* number = itoa(++count);
            count_inside += 1;
            strncpy(sql + position, number, len_of_int(count));
            position += len_of_int(count);
            FREE(number);
        }
        strncpy(sql + position, ")", 2);
        position += 1;
    }
    if (batch->schema->nPrimaryKeys != 0) {
        strncpy(sql + position, " on conflict (", 15);
        position += 14;
        bool first = true;
        for (int i = 0;i < batch->schema->nPrimaryKeys;i++){
            int index = batch->schema->primaryKeys[i];
            if (first) first = false;
            else {
                strncpy(sql + position, ",", 2);
                position += 1;
            }
            strncpy(sql + position, batch->schema->columns[index].quoted, strlen(batch->schema->columns[index].quoted));
            position += strlen(batch->schema->columns[index].quoted);
        }
        strncpy(sql + position, ") do ", 6);
        position += 5;
        if (batch->writeMode == INSERT_OR_IGNORE){
            strncpy(sql + position, "nothing", 8);
            position += 7;
        }
        else {  //若为INSERT_OR_REPLACE，则record已被normalize
            strncpy(sql + position, "update set ", 12);
            position += 11;
            first = true;
            for (int i = 0;i < batch->schema->nColumns;i++){
                if (!batch->valuesSet[i]) continue;
                if (first) first = false;
                else {
                    strncpy(sql + position, ",", 2);
                    position += 1;
                }
                strncpy(sql + position, batch->schema->columns[i].quoted, strlen(batch->schema->columns[i].quoted));
                position += strlen(batch->schema->columns[i].quoted);
                strncpy(sql + position, "=excluded.", 11);
                position += 10;
                strncpy(sql + position, batch->schema->columns[i].quoted, strlen(batch->schema->columns[i].quoted) + 1);
                position += strlen(batch->schema->columns[i].quoted);
            }
        }
    }
    // LOG_DEBUG("insert sql is %s", sql);
    return sql;
}

char* build_delete_sql_with_batch(Batch* batch, int nRecords){
    //DELETE FROM test WHERE (id=$1 and id1=$2) or (id=$3 and id1=$4)
    if (batch->mode != DELETE) return NULL;
    if (nRecords == 0) nRecords = batch->nRecords;
    int length = 19 - 4;
    length += strlen(batch->schema->tableName->fullName);
    int singleRecordLength = 2 - 5;
    for (int i = 0;i < batch->schema->nColumns;i++){
        if (!batch->valuesSet[i]) continue;
        singleRecordLength += strlen(batch->schema->columns[i].quoted) + 7;
    }
    length += nRecords * singleRecordLength;
    length += 4 * nRecords;
    int nParams = nRecords * batch->nValues;
    for (int i = 0;i < nParams;) length += 2 + len_of_int(++i);

    char* sql = MALLOC(length + 1, char);
    strncpy(sql, "DELETE FROM ", 13);
    int position = 12;
    int count = 0;
    strncpy(sql + position, batch->schema->tableName->fullName, strlen(batch->schema->tableName->fullName));
    position += strlen(batch->schema->tableName->fullName);
    strncpy(sql + position, " WHERE ", 8);
    position += 7;
    bool first = true;
    for (int i = 0;i < nRecords;i++){
        if (first){
            first = false;
        }
        else {
            strncpy(sql + position, " or ", 5);
            position += 4;
        }
        strncpy(sql + position, "(", 2);
        position++;
        bool firstCol = true;
        for (int j = 0;j < batch->schema->nColumns;j++){
            if (!batch->valuesSet[j]) continue;
            if (firstCol){
                firstCol = false;
            }
            else {
                strncpy(sql + position, " and ", 6);
                position += 5;
            }
            strncpy(sql + position, batch->schema->columns[j].quoted, strlen(batch->schema->columns[j].quoted));
            position += strlen(batch->schema->columns[j].quoted);
            strncpy(sql + position, "=$", 3);
            position += 2;
            char* number = itoa(++count);
            strncpy(sql + position, number, strlen(number));
            position += len_of_int(count);
            FREE(number);
        }
        strncpy(sql + position, ")", 2);
        position++;
    }
    //fprintf(stderr, "Delete SQL: %s\n", sql);
    return sql;
}

char* build_get_sql(HoloTableSchema* schema, int nRecords) {
    //SELECT * FROM xx WHERE (f1=$1 and f2=$2) or (f1=$3 and f2=$4)
    int length = 21 - 4;
    length += strlen(schema->tableName->fullName);
    int singleRecordLength = 2 - 5;
    for (int i = 0;i < schema->nPrimaryKeys;i++){
        singleRecordLength += strlen(schema->columns[schema->primaryKeys[i]].quoted) + 7;
    }
    length += nRecords * singleRecordLength;
    length += 4 * nRecords;
    int nParams = nRecords * schema->nPrimaryKeys;
    for (int i = 0;i < nParams;) length += 2 + len_of_int(++i);

    char* sql = MALLOC(length + 1, char);
    strncpy(sql, "SELECT * FROM ", 15);
    int position = 14;
    int count = 0;
    strncpy(sql + position, schema->tableName->fullName, strlen(schema->tableName->fullName));
    position += strlen(schema->tableName->fullName);
    strncpy(sql + position, " WHERE ", 8);
    position += 7;
    bool first = true;
    for (int i = 0;i < nRecords;i++){
        if (first){
            first = false;
        }
        else {
            strncpy(sql + position, " or ", 5);
            position += 4;
        }
        strncpy(sql + position, "(", 2);
        position++;
        bool firstCol = true;
        for (int j = 0;j < schema->nPrimaryKeys;j++){
            int col = schema->primaryKeys[j];
            if (firstCol){
                firstCol = false;
            }
            else {
                strncpy(sql + position, " and ", 6);
                position += 5;
            }
            strncpy(sql + position, schema->columns[col].quoted, strlen(schema->columns[col].quoted));
            position += strlen(schema->columns[col].quoted);
            strncpy(sql + position, "=$", 3);
            position += 2;
            char* number = itoa(++count);
            strncpy(sql + position, number, len_of_int(count));
            position += len_of_int(count);
            FREE(number);
        }
        strncpy(sql + position, ")", 2);
        position++;
    }
    // LOG_DEBUG("Get SQL: %s", sql);
    return sql;
}