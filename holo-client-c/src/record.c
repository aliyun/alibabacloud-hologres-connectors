#include "record.h"
#include "record_private.h"
#include "logger.h"
#include "utils.h"
#include "murmur3.h"

Record* holo_client_new_record(TableSchema* schema){
    if (schema == NULL) return NULL;
    Record* record = MALLOC(1, Record);
    record->byteSize = sizeof(Record);
    record->schema = schema;
    int mSize = schema->nColumns * (sizeof(char*) + sizeof(bool) + 2 * sizeof(int));
    void* mPool = MALLOC(mSize, char);
    record->values = mPool;
    record->valuesSet = mPool + schema->nColumns * sizeof(char*);
    record->valueLengths = mPool + schema->nColumns * (sizeof(char*) + sizeof(bool));
    record->valueFormats = mPool + schema->nColumns * (sizeof(char*) + sizeof(bool) + sizeof(int));
    record->byteSize += mSize;
    for (int i = 0;i < schema->nColumns;i++){
        record->values[i] = NULL;
        record->valuesSet[i] = false;
        record->valueLengths[i] = 0;
        record->valueFormats[i] = 0;
    }
    record->nValues = 0;
    return record;
}

void holo_client_destroy_record(Record* record){
    if (record == NULL) return;
    for (int i = 0;i < record->schema->nColumns;i++) {
        destroy_record_val(record, i);
    }
    FREE(record->values); //释放了整个mPool
    // FREE(record->valuesSet);
    // FREE(record->valueLengths);
    // FREE(record->valueFormats);
    FREE(record);
}

void* new_record_val(Record* record, int length){
    return MALLOC(length, char);
}

void revoke_record_val(void* addr, Record* record, int length){
    FREE(addr);
}

void destroy_record_val(Record* record, int colIndex){
    void* addr = record->values[colIndex];
    FREE(record->values[colIndex]);
}

RecordItem* create_record_item(Record* record) {
    RecordItem* item = MALLOC(1, RecordItem);
    item->record = record;
    return item;
}

bool has_same_pk(Record* record1, Record* record2) {
    TableSchema* schema;
    if (record1->schema != record2->schema) return false;
    schema = record1->schema;
    for (int i = 0; i < schema->nPrimaryKeys; i++) {
        int index = schema->primaryKeys[i];
        if (record1->valueFormats[index] != record2->valueFormats[index]) {
            LOG_ERROR("Cannot deal with different value formats.");
            return false;
        };
        if (record1->valueFormats[index] == 0 && strcmp(record1->values[index], record2->values[index]) != 0) return false;
        if (record1->valueFormats[index] == 1 && (record1->valueLengths[index] != record2->valueLengths[index] || memcmp(record1->values[index], record2->values[index], record1->valueLengths[index]) != 0)) return false;
    }
    return true;
}

char* holo_client_record_table_name(Record* record) {
    return record->schema->tableName->fullName;
}

int holo_client_record_num_column(Record* record) {
    return record->schema->nColumns;
}

int16_t holo_client_get_record_int16_val_by_colindex(Record* record, int colIndex) {
    int16_t val;
    if (record->schema->columns[colIndex].type != 21 || !record->valuesSet[colIndex]) return 0;
    memcpy(&val, record->values[colIndex], 2);
    endian_swap(&val, 2);
    return val;
}

int32_t holo_client_get_record_int32_val_by_colindex(Record* record, int colIndex) {
    int32_t val;
    if (record->schema->columns[colIndex].type != 23 || !record->valuesSet[colIndex]) return 0;
    memcpy(&val, record->values[colIndex], 4);
    endian_swap(&val, 4);
    return val;
}

int64_t holo_client_get_record_int64_val_by_colindex(Record* record, int colIndex) {
    int64_t val;
    if (!record->valuesSet[colIndex] || !(record->valueFormats[colIndex] == 1) || !(record->valueLengths[colIndex] == 8)) return 0;
    memcpy(&val, record->values[colIndex], 8);
    endian_swap(&val, 8);
    return val;
}

bool holo_client_get_record_bool_val_by_colindex(Record* record, int colIndex) {
    if (record->schema->columns[colIndex].type != 16 || !record->valuesSet[colIndex]) return false;
    if (*(bool*)record->values[colIndex] == 1) return true;
    return false;
}

float holo_client_get_record_float_val_by_colindex(Record* record, int colIndex) {
    float val;
    if (record->schema->columns[colIndex].type != 700 || !record->valuesSet[colIndex]) return 0;
    memcpy(&val, record->values[colIndex], 4);
    endian_swap(&val, 4);
    return val;
}

double holo_client_get_record_double_val_by_colindex(Record* record, int colIndex) {
    double val;
    if (record->schema->columns[colIndex].type != 701 || !record->valuesSet[colIndex]) return 0;
    memcpy(&val, record->values[colIndex], 8);
    endian_swap(&val, 8);
    return val;
}

char* holo_client_get_record_text_val_by_colindex(Record* record, int colIndex) {
    if (record->schema->columns[colIndex].type != 25) return NULL;
    return deep_copy_string(record->values[colIndex]);
}

int32_t* holo_client_get_record_int32_array_val_by_colindex(Record* record, int colIndex, int* numValues) {
    int n;
    int32_t* array;
    char* cur;
    if (record->schema->columns[colIndex].type != 1007 || record->valueFormats[colIndex] == 0 || !record->valuesSet[colIndex] || record->values[colIndex] == NULL) {
        *numValues = -1;
        return NULL;
    }
    n = (record->valueLengths[colIndex] - 20) / 8;
    *numValues = n;
    array = MALLOC(n, int32_t);
    cur = record->values[colIndex] + 24;
    for (int i = 0; i < n; i++) {
        memcpy(&array[i], cur, 4);
        endian_swap(&array[i], 4);
        cur += 8;
    }
    return array;
}
int64_t* holo_client_get_record_int64_array_val_by_colindex(Record* record, int colIndex, int* numValues) {
    int n;
    int64_t* array;
    char* cur;
    if (record->schema->columns[colIndex].type != 1016 || record->valueFormats[colIndex] == 0 || !record->valuesSet[colIndex] || record->values[colIndex] == NULL) {
        *numValues = -1;
        return NULL;
    }
    n = (record->valueLengths[colIndex] - 20) / 12;
    *numValues = n;
    array = MALLOC(n, int64_t);
    cur = record->values[colIndex] + 24;
    for (int i = 0; i < n; i++) {
        memcpy(&array[i], cur, 8);
        endian_swap(&array[i], 8);
        cur += 12;
    }
    return array;
}
bool* holo_client_get_record_bool_array_val_by_colindex(Record* record, int colIndex, int* numValues) {
    int n;
    bool* array;
    char* cur;
    if (record->schema->columns[colIndex].type != 1000 || record->valueFormats[colIndex] == 0 || !record->valuesSet[colIndex] || record->values[colIndex] == NULL) {
        *numValues = -1;
        return NULL;
    }
    n = (record->valueLengths[colIndex] - 20) / 5;
    *numValues = n;
    array = MALLOC(n, bool);
    cur = record->values[colIndex] + 24;
    for (int i = 0; i < n; i++) {
        memcpy(&array[i], cur, 1);
        endian_swap(&array[i], 1);
        cur += 5;
    }
    return array;
}
float* holo_client_get_record_float_array_val_by_colindex(Record* record, int colIndex, int* numValues) {
    int n;
    float* array;
    char* cur;
    if (record->schema->columns[colIndex].type != 1021 || record->valueFormats[colIndex] == 0 || !record->valuesSet[colIndex] || record->values[colIndex] == NULL) {
        *numValues = -1;
        return NULL;
    }
    n = (record->valueLengths[colIndex] - 20) / 8;
    *numValues = n;
    array = MALLOC(n, float);
    cur = record->values[colIndex] + 24;
    for (int i = 0; i < n; i++) {
        memcpy(&array[i], cur, 4);
        endian_swap(&array[i], 4);
        cur += 8;
    }
    return array;
}
double* holo_client_get_record_double_array_val_by_colindex(Record* record, int colIndex, int* numValues) {
    int n;
    double* array;
    char* cur;
    if (record->schema->columns[colIndex].type != 1022 || record->valueFormats[colIndex] == 0 || !record->valuesSet[colIndex] || record->values[colIndex] == NULL) {
        *numValues = -1;
        return NULL;
    }
    n = (record->valueLengths[colIndex] - 20) / 12;
    *numValues = n;
    array = MALLOC(n, double);
    cur = record->values[colIndex] + 24;
    for (int i = 0; i < n; i++) {
        memcpy(&array[i], cur, 8);
        endian_swap(&array[i], 8);
        cur += 12;
    }
    return array;
}
char** holo_client_get_record_text_array_val_by_colindex(Record* record, int colIndex, int* numValues) {
    int n;
    char** array;
    char* cur;
    if (record->schema->columns[colIndex].type != 1009 || record->valueFormats[colIndex] == 0 || !record->valuesSet[colIndex] || record->values[colIndex] == NULL) {
        *numValues = -1;
        return NULL;
    }
    cur = record->values[colIndex] + 12;
    memcpy(&n, cur, 4);
    endian_swap(&n, 4);
    array = MALLOC(n, char*);
    *numValues = n;
    cur += 8;
    for (int i = 0; i < n; i++) {
        int length;
        memcpy(&length, cur, 4);
        endian_swap(&length, 4);
        array[i] = MALLOC(length + 1, char);
        cur += 4;
        memcpy(array[i], cur, length);
        array[i][length] = '\0';
        cur += length;
    }
    return array;
}

void holo_client_destroy_text_array_val(char** val, int n) {
    for (int i = 0; i < n; i++) {
        FREE(val[i]);
    }
    FREE(val);
    val= NULL;
}

char* holo_client_get_record_val_with_text_by_colindex(Record* record, int colIndex) {
    char* res;
    int length;
    if (!record->valuesSet[colIndex] || record->values[colIndex] == NULL) {
        return NULL;
    }
    if (record->valueFormats[colIndex] == 0) {
        return deep_copy_string(record->values[colIndex]);
    }
    switch (record->schema->columns[colIndex].type)
    {
    case 21:
        return int16toa(holo_client_get_record_int16_val_by_colindex(record, colIndex));
    case 23:
        return int32toa(holo_client_get_record_int32_val_by_colindex(record, colIndex));
    case 20:
        return int64toa(holo_client_get_record_int64_val_by_colindex(record, colIndex));
    case 16:
        return btoa(holo_client_get_record_bool_val_by_colindex(record, colIndex));
    case 700:
        return ftoa(holo_client_get_record_float_val_by_colindex(record, colIndex));
    case 701:
        return dtoa(holo_client_get_record_double_val_by_colindex(record, colIndex));
    case 1114:
        return int64toa(holo_client_get_record_int64_val_by_colindex(record, colIndex));
    case 1184:
        return int64toa(holo_client_get_record_int64_val_by_colindex(record, colIndex));
    case 1007: {
        int32_t* int32array = holo_client_get_record_int32_array_val_by_colindex(record, colIndex, &length);
        res = int32_array_toa(int32array, length);
        FREE(int32array);
        return res;}
    case 1016: {
        int64_t* int64array = holo_client_get_record_int64_array_val_by_colindex(record, colIndex, &length);
        res = int64_array_toa(int64array, length);
        FREE(int64array);
        return res;}
    case 1000: {
        bool* boolarray = holo_client_get_record_bool_array_val_by_colindex(record, colIndex, &length);
        res = bool_array_toa(boolarray, length);
        FREE(boolarray);
        return res;}
    case 1021: {
        float* floatarray = holo_client_get_record_float_array_val_by_colindex(record, colIndex, &length);
        res = float_array_toa(floatarray, length);
        FREE(floatarray);
        return res;}
    case 1022: {
        double* doublearray = holo_client_get_record_double_array_val_by_colindex(record, colIndex, &length);
        res = double_array_toa(doublearray, length);
        FREE(doublearray);
        return res;}
    case 1009: {
        char** textarray = holo_client_get_record_text_array_val_by_colindex(record, colIndex, &length);
        res = text_array_toa(textarray, length);
        holo_client_destroy_text_array_val(textarray, length);
        return res;}
    default:
        break;
    }
    return NULL;
}

void holo_client_destroy_val(void* val) {
    FREE(val);
}

int record_pk_hash_code(Record* record, int size) {
    TableSchema* schema = record->schema;
    unsigned raw = 0;
    bool first = true;
    for (int i = 0;i < schema->nPrimaryKeys;i++){
        int index = schema->primaryKeys[i];
        char* value = record->values[index];
        int length = record->valueLengths[index];
        if (first){
            MurmurHash3_x86_32(value, length, 0xf7ca7fd2, &raw);
            first = false;
        }
        else{
            unsigned t = 0;
            MurmurHash3_x86_32(value, length, 0xf7ca7fd2, &t);
            raw ^= t;
        }
    }
    int hash = raw % ((unsigned)65536);
    int base = 65536 / size;
    int remain = 65536 % size;
    int pivot = (base + 1) * remain;
    int index = 0;
    if (hash < pivot) index = hash / (base + 1);
    else index = (hash - pivot) / base + remain;
    return index;
}