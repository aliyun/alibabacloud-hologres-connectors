#include "request_private.h"
#include "utils.h"
#include "logger_private.h"
#include "table_schema.h"
#include "table_schema_private.h"
#include "inttypes.h"

Meta holo_client_new_meta_request(HoloTableName tableName) {
    Meta meta = MALLOC(1, MetaRequest);
    meta->future = create_future();
    meta->tableName.fullName = deep_copy_string(tableName.fullName);
    meta->tableName.schemaName = deep_copy_string(tableName.schemaName);
    meta->tableName.tableName = deep_copy_string(tableName.tableName);
    return meta;
}

void holo_client_destroy_meta_request(Meta meta) {
    FREE(meta->tableName.fullName);
    FREE(meta->tableName.schemaName);
    FREE(meta->tableName.tableName);
    destroy_future(meta->future);
    FREE(meta);
    meta = NULL;
}

HoloMutation holo_client_new_mutation_request(HoloTableSchema* schema) {
    if (schema == NULL){
        LOG_ERROR("Table schema is NULL.");
        return NULL;
    }
    HoloMutation mutation = MALLOC(1, HoloMutationRequest);
    mutation->record = holo_client_new_record(schema);
    dlist_init(&mutation->attachmentList);
    mutation->mode = PUT;
    mutation->writeMode = INSERT_OR_REPLACE;
    return mutation;
}

int holo_client_set_request_mode(HoloMutation mutation, HoloMutationMode mode){
    if (mutation == NULL){
        LOG_ERROR("HoloMutation is NULL.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    mutation->mode = mode;
    return HOLO_CLIENT_RET_OK;
}

int holo_client_mutation_byte_size(const HoloMutation mutation) {
    if (mutation == NULL){
        LOG_ERROR("HoloMutation is NULL.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (mutation->record == NULL){
        LOG_ERROR("HoloMutation has no record.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    return mutation->byteSize;
}

bool set_req_val_by_colindex_is_valid(HoloMutation mutation, int colIndex){
    if (mutation == NULL){
        LOG_ERROR("HoloMutation is NULL.");
        return false;
    }
    if (colIndex < 0 || colIndex >= mutation->record->schema->nColumns) {
        LOG_ERROR("Column index %d exceeds column number for table \"%s\".", colIndex, mutation->record->schema->tableName->tableName);
        return false;
    }
    return true;
}

bool column_type_matches_oid(HoloRecord* record, int colIndex, Oid oid){
    if (record->schema->columns[colIndex].type != oid) {
        LOG_ERROR("Column %d type not match for table \"%s\".", colIndex, record->schema->tableName->tableName);
        return false;
    }
    return true;
}

int set_record_val(HoloRecord* record, int colIndex, char* ptr, int format, int length){
    if (record->valuesSet[colIndex]) {
        LOG_ERROR("Column %d already set for table \"%s\".", colIndex, record->schema->tableName->tableName);
        revoke_record_val(ptr, record, length);
        return HOLO_CLIENT_COL_ALREADY_SET;
    }
    record->values[colIndex] = ptr;
    record->valuesSet[colIndex] = true;
    record->valueFormats[colIndex] = format;
    record->valueLengths[colIndex] = length;
    record->nValues++;
    record->byteSize += length;
    return HOLO_CLIENT_RET_OK;
}

int try_set_null_val(HoloRecord* record, int colIndex){
    if (record->schema->columns[colIndex].nullable == false){
        LOG_ERROR("Column %d can not be null but set null for table \"%s\".", colIndex, record->schema->tableName->tableName);
        return HOLO_CLIENT_NOT_NULL_BUT_SET_NULL;
    }
    return set_record_val(record, colIndex, NULL, 0, 0);
}

int unnest_convert_array_to_text(char** valuePtr, char** values, int* valueLengths, int nValues) {
    int length = 3; // {}和\0
    for (int j = 0; j < nValues; j++) {
        if (values[j] == NULL) {
            length += 4; // NULL
        } else {
            char* pos = values[j];
            int cnt = 0;
            while (pos) {
                pos = strpbrk(pos, "\"\\");
                if (pos) {
                    ++pos;
                    ++cnt;
                }
            }
            length += cnt;
            length += valueLengths[j]-1;
            length += 2; // ""
        }
        if (j < nValues - 1) {
            length += 1; // ,
        }
    }
    char* ptr = MALLOC(length, char);
    int idx = 0;
    ptr[idx++] = '{';
    for (int j = 0; j < nValues; j++) {
        if (values[j] == NULL){
            LOG_DEBUG("Value is NULL in text array values.");
            strncpy(ptr + idx, "NULL", 5);
            idx += 4;
        } else {
            ptr[idx++] = '"';
            if (strpbrk(values[j], "\"\\") == NULL) {
                //没有转义，整块拷贝
                strncpy(ptr + idx, values[j], valueLengths[j]-1);
                idx += valueLengths[j]-1;
            } else {
                //有转义，逐字符拷贝
                for (int k = 0; k < valueLengths[j]-1; k++) {
                    if (values[j][k] == '"' || values[j][k] == '\\') {
                        ptr[idx++] = '\\';
                    }
                    ptr[idx++] = values[j][k];
                }
            }
            ptr[idx++] = '"';
        }
        if (j < nValues - 1) {
            ptr[idx++] = ',';
        }
    }
    ptr[idx++] = '}';
    ptr[idx] = '\0';
    *valuePtr = ptr;
    return length;
}

void unnest_convert_array_to_postgres_binary(char* ptr, char** values, int nValues, int valueLength, int valueType){
    ((int*)ptr)[0] = 1;   //数组维度nDims
    endian_swap(ptr, 4);
    ((int*)ptr)[1] = 0;   //是否存在null值
    endian_swap(ptr + 4, 4);
    ((int*)ptr)[2] = valueType;  //数组元素类型Oid
    endian_swap(ptr + 8, 4);
    ((int*)ptr)[3] = nValues;  //第1维度上的元素数量
    endian_swap(ptr + 12, 4);
    ((int*)ptr)[4] = 1;  //第1维度上的lBound
    endian_swap(ptr + 16, 4);
    char* cur = ptr + 4 * 5;
    for (int i = 0;i < nValues;i++){
        if (values[i] == NULL){
            LOG_DEBUG("Value is NULL in Binary array values.");
            //修改null flag
            ((int*)ptr)[1] = 1;
            endian_swap(ptr + 4, 4);
            //null元素长度为-1
            *((int*)cur) = -1;
            endian_swap(cur, 4);
            cur += 4;
            continue;
        }
        *((int*)cur) = valueLength;  //元素长度
        endian_swap(cur, 4);
        cur += 4;
        memcpy(cur, values[i], valueLength);  //元素值
        // endian_swap(cur, valueLength);
        cur += valueLength;
    }
}

// int4[],int8[],bool[],double[],float[]转binary，这种无需考虑null
void convert_array_to_postgres_binary(char* ptr, const void* values, int nValues, int valueLength, int valueType){
    ((int*)ptr)[0] = 1;   //数组维度nDims
    endian_swap(ptr, 4);
    ((int*)ptr)[1] = 0;   //是否存在null值
    endian_swap(ptr + 4, 4);
    ((int*)ptr)[2] = valueType;  //数组元素类型Oid
    endian_swap(ptr + 8, 4);
    ((int*)ptr)[3] = nValues;  //第1维度上的元素数量
    endian_swap(ptr + 12, 4);
    ((int*)ptr)[4] = 1;  //第1维度上的lBound
    endian_swap(ptr + 16, 4);
    char* cur = ptr + 4 * 5;
    for (int i = 0;i < nValues;i++){
        *((int*)cur) = valueLength;  //元素长度
        endian_swap(cur, 4);
        cur += 4;
        memcpy(cur, values + valueLength * i, valueLength);  //元素值
        endian_swap(cur, valueLength);
        cur += valueLength;
    }
}

void convert_text_array_to_postgres_binary(char* ptr, char** values, int nValues, int valueType){
    ((int*)ptr)[0] = 1;   //数组维度nDims
    endian_swap(ptr, 4);
    ((int*)ptr)[1] = 0;   //是否存在null值
    endian_swap(ptr + 4, 4);
    ((int*)ptr)[2] = valueType;  //数组元素类型Oid
    endian_swap(ptr + 8, 4);
    ((int*)ptr)[3] = nValues;  //第1维度上的元素数量
    endian_swap(ptr + 12, 4);
    ((int*)ptr)[4] = 1;  //第1维度上的lBound
    endian_swap(ptr + 16, 4);
    char* cur = ptr + 4 * 5;
    for (int i = 0;i < nValues;i++){
        if (values[i] == NULL){
            LOG_DEBUG("Value is NULL in text array values.");
            //修改null flag
            ((int*)ptr)[1] = 1;
            endian_swap(ptr + 4, 4);
            //null元素长度为-1
            *((int*)cur) = -1;
            endian_swap(cur, 4);
            cur += 4;
            continue;
        }
        int valueLength = strlen(values[i]);
        *((int*)cur) = valueLength;  //元素长度
        endian_swap(cur, 4);
        cur += 4;
        memcpy(cur, values[i], valueLength);  //元素值
        cur += valueLength;
    }
}

int set_record_val_by_type(HoloRecord* record, int colIndex, const char* str, int len){
    Oid type = record->schema->columns[colIndex].type;
    char* ptr = NULL;
    switch (type){
    case HOLO_TYPE_INT2:
        ptr = (char*)new_record_val(record, 2);
        char* end;
        *(int16_t*)ptr = strtol(str, &end, 10);
        if (*end){
            LOG_ERROR("\"%s\" is not a int16 value.", str);
            revoke_record_val(ptr, record, 2);
            return HOLO_CLIENT_INVALID_PARAM;
        }
        endian_swap(ptr, 2);
        return set_record_val(record, colIndex, ptr, 1, 2);
        break;
    case HOLO_TYPE_INT4:
        ptr = (char*)new_record_val(record, 4);
        *(int32_t*)ptr = strtol(str, &end, 10);
        if (*end){
            LOG_ERROR("\"%s\" is not a int32 value.", str);
            revoke_record_val(ptr, record, 4);
            return HOLO_CLIENT_INVALID_PARAM;
        }
        endian_swap(ptr, 4);
        return set_record_val(record, colIndex, ptr, 1, 4);
        break;
    case HOLO_TYPE_INT8:
        ptr = (char*)new_record_val(record, 8);
        *(int64_t*)ptr = strtol(str, &end, 10);
        if (*end){
            LOG_ERROR("\"%s\" is not a int64 value.", str);
            revoke_record_val(ptr, record, 8);
            return HOLO_CLIENT_INVALID_PARAM;
        }
        endian_swap(ptr, 8);
        return set_record_val(record, colIndex, ptr, 1, 8);
        break;
    case HOLO_TYPE_BOOL:
        ptr = (char*)new_record_val(record, 1);
        char* tmp = MALLOC(len + 1, char);
        deep_copy_string_to(str, tmp, len);
        tmp[len] = '\0';
        to_lower_case(tmp, len);
        if (strncmp(tmp, "true", len + 1) == 0 || strncmp(tmp, "1", len + 1) == 0) *ptr = 1;
        else if (strncmp(tmp, "false", len + 1) == 0 || strncmp(tmp, "0", len + 1) == 0) *ptr = 0;
        else {
            LOG_ERROR("\"%s\" is not a bool value.", str);
            revoke_record_val(ptr, record, 1);
            FREE(tmp);
            return HOLO_CLIENT_INVALID_PARAM;
        }
        FREE(tmp);
        return set_record_val(record, colIndex, ptr, 1, 1);
        break;
    case HOLO_TYPE_FLOAT4:
        ptr = (char*)new_record_val(record, 4);
        *(float*)ptr = strtof(str, &end);
        if (*end){
            LOG_ERROR("\"%s\" is not a float value.", str);
            revoke_record_val(ptr, record, 4);
            return HOLO_CLIENT_INVALID_PARAM;
        }
        endian_swap(ptr, 4);
        return set_record_val(record, colIndex, ptr, 1, 4);
        break;
    case HOLO_TYPE_FLOAT8:
        ptr = (char*)new_record_val(record, 8);
        *(double*)ptr = strtod(str, &end);
        if (*end){
            LOG_ERROR("\"%s\" is not a double value.", str);
            revoke_record_val(ptr, record, 8);
            return HOLO_CLIENT_INVALID_PARAM;
        }
        endian_swap(ptr, 8);
        return set_record_val(record, colIndex, ptr, 1, 8);
        break;
    // TODO: timestamp的字符串解析成数字，统一两种写入方式
    default:
        ptr = (char*)new_record_val(record, len + 1);
        deep_copy_string_to(str, ptr, len);
        // add '\0', PQexecPrepared need it when processing text as parameter
        ptr[len] = '\0';
        return set_record_val(record, colIndex, ptr, 0, len + 1);
        break;
    }
    return HOLO_CLIENT_RET_OK;
}

int holo_client_set_req_val_with_text_by_colindex(HoloMutation mutation, int colIndex, const char* value, int len){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (value == NULL){
        return holo_client_set_req_null_val_by_colindex(mutation, colIndex);
    }
    if (!set_req_val_by_colindex_is_valid(mutation, colIndex)) return HOLO_CLIENT_INVALID_COL_IDX;
    return set_record_val_by_type(mutation->record, colIndex, value, len);
}

int holo_client_set_req_int16_val_by_colindex(HoloMutation mutation, int colIndex, int16_t value){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (!set_req_val_by_colindex_is_valid(mutation, colIndex)) return HOLO_CLIENT_INVALID_COL_IDX;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_INT2)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    int16_t* ptr = new_record_val(mutation->record, 2);
    *ptr = value;
    endian_swap(ptr, 2);
    return set_record_val(mutation->record, colIndex, (char*)ptr, 1, 2);
}

int holo_client_set_req_int32_val_by_colindex(HoloMutation mutation, int colIndex, int32_t value){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (!set_req_val_by_colindex_is_valid(mutation, colIndex)) return HOLO_CLIENT_INVALID_COL_IDX;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_INT4)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    int32_t* ptr = new_record_val(mutation->record, 4);
    *ptr = value;
    endian_swap(ptr, 4);
    return set_record_val(mutation->record, colIndex, (char*)ptr, 1, 4);
}

int holo_client_set_req_int64_val_by_colindex(HoloMutation mutation, int colIndex, int64_t value){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (!set_req_val_by_colindex_is_valid(mutation, colIndex)) return HOLO_CLIENT_INVALID_COL_IDX;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_INT8)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    int64_t* ptr = new_record_val(mutation->record, 8);
    *ptr = value;
    endian_swap(ptr, 8);
    return set_record_val(mutation->record, colIndex, (char*)ptr, 1, 8);
}

int holo_client_set_req_bool_val_by_colindex(HoloMutation mutation, int colIndex, bool value){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (!set_req_val_by_colindex_is_valid(mutation, colIndex)) return HOLO_CLIENT_INVALID_COL_IDX;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_BOOL)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    bool* ptr = new_record_val(mutation->record, 1);
    *ptr = value;
    return set_record_val(mutation->record, colIndex, (char*)ptr, 1, 1);
}

int holo_client_set_req_float_val_by_colindex(HoloMutation mutation, int colIndex, float value){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (!set_req_val_by_colindex_is_valid(mutation, colIndex)) return HOLO_CLIENT_INVALID_COL_IDX;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_FLOAT4)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    float* ptr = new_record_val(mutation->record, 4);
    *ptr = value;
    endian_swap(ptr, 4);
    return set_record_val(mutation->record, colIndex, (char*)ptr, 1, 4);
}

int holo_client_set_req_double_val_by_colindex(HoloMutation mutation, int colIndex, double value){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (!set_req_val_by_colindex_is_valid(mutation, colIndex)) return HOLO_CLIENT_INVALID_COL_IDX;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_FLOAT8)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    double* ptr = new_record_val(mutation->record, 8);
    *ptr = value;
    endian_swap(ptr, 8);
    return set_record_val(mutation->record, colIndex, (char*)ptr, 1, 8);
}

int holo_client_set_req_text_val_by_colindex(HoloMutation mutation, int colIndex, const char *value, int len) {
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (!set_req_val_by_colindex_is_valid(mutation, colIndex)) return HOLO_CLIENT_INVALID_COL_IDX;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_TEXT)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    if (value == NULL) {
        return try_set_null_val(mutation->record, colIndex);
    }
    char* ptr = (char*)new_record_val(mutation->record, len + 1);
    deep_copy_string_to(value, ptr, len);
    // add '\0', PQexecPrepared need it when processing text as parameter
    ptr[len] = '\0';
    return set_record_val(mutation->record, colIndex, ptr, 0, len + 1);
}

int holo_client_set_req_timestamp_val_by_colindex(HoloMutation mutation, int colIndex, int64_t value) {
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (!set_req_val_by_colindex_is_valid(mutation, colIndex)) return HOLO_CLIENT_INVALID_COL_IDX;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_TIMESTAMP)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    int64_t* ptr = MALLOC(1, int64_t);
    *ptr = value;
    endian_swap(ptr, 8);
    return set_record_val(mutation->record, colIndex, (char*)ptr, 1, 8);
}

int holo_client_set_req_timestamptz_val_by_colindex(HoloMutation mutation, int colIndex, int64_t value) {
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (!set_req_val_by_colindex_is_valid(mutation, colIndex)) return HOLO_CLIENT_INVALID_COL_IDX;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_TIMESTAMPTZ)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    int64_t* ptr = MALLOC(1, int64_t);
    *ptr = value;
    endian_swap(ptr, 8);
    return set_record_val(mutation->record, colIndex, (char*)ptr, 1, 8);
}

int holo_client_set_req_int32_array_val_by_colindex(HoloMutation mutation, int colIndex, const int32_t* values, int nValues){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (!set_req_val_by_colindex_is_valid(mutation, colIndex)) return HOLO_CLIENT_INVALID_COL_IDX;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_INT4_ARRAY)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    if (values == NULL) {
        LOG_DEBUG("Values is NULL in array type setting function. Try set NULL value.");
        return try_set_null_val(mutation->record, colIndex);
    }
    int length = 20 + 4 * nValues + 4 * nValues;
    char* ptr = new_record_val(mutation->record, length);
    convert_array_to_postgres_binary(ptr, values, nValues, 4, HOLO_TYPE_INT4);
    return set_record_val(mutation->record, colIndex, ptr, 1, length);
}

int holo_client_set_req_int64_array_val_by_colindex(HoloMutation mutation, int colIndex, const int64_t* values, int nValues){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (!set_req_val_by_colindex_is_valid(mutation, colIndex)) return HOLO_CLIENT_INVALID_COL_IDX;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_INT8_ARRAY)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    if (values == NULL) {
        LOG_DEBUG("Values is NULL in array type setting function. Try set NULL value.");
        return try_set_null_val(mutation->record, colIndex);
    }
    int length = 20 + 4 * nValues + 8 * nValues;
    char* ptr = new_record_val(mutation->record, length);
    convert_array_to_postgres_binary(ptr, values, nValues, 8, HOLO_TYPE_INT8);
    return set_record_val(mutation->record, colIndex, ptr, 1, length);
}

int holo_client_set_req_bool_array_val_by_colindex(HoloMutation mutation, int colIndex, const bool* values, int nValues){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (!set_req_val_by_colindex_is_valid(mutation, colIndex)) return HOLO_CLIENT_INVALID_COL_IDX;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_BOOL_ARRAY)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    if (values == NULL) {
        LOG_DEBUG("Values is NULL in array type setting function. Try set NULL value.");
        return try_set_null_val(mutation->record, colIndex);
    }
    int length = 20 + 4 * nValues + 1 * nValues;
    char* ptr = new_record_val(mutation->record, length);
    convert_array_to_postgres_binary(ptr, values, nValues, 1, HOLO_TYPE_BOOL);
    return set_record_val(mutation->record, colIndex, ptr, 1, length);
}

int holo_client_set_req_float_array_val_by_colindex(HoloMutation mutation, int colIndex, const float* values, int nValues){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (!set_req_val_by_colindex_is_valid(mutation, colIndex)) return HOLO_CLIENT_INVALID_COL_IDX;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_FLOAT4_ARRAY)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    if (values == NULL) {
        LOG_DEBUG("Values is NULL in array type setting function. Try set NULL value.");
        return try_set_null_val(mutation->record, colIndex);
    }
    int length = 20 + 4 * nValues + 4 * nValues;
    char* ptr = new_record_val(mutation->record, length);
    convert_array_to_postgres_binary(ptr, values, nValues, 4, HOLO_TYPE_FLOAT4);
    return set_record_val(mutation->record, colIndex, ptr, 1, length);
}

int holo_client_set_req_double_array_val_by_colindex(HoloMutation mutation, int colIndex, const double* values, int nValues){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (!set_req_val_by_colindex_is_valid(mutation, colIndex)) return HOLO_CLIENT_INVALID_COL_IDX;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_FLOAT8_ARRAY)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    if (values == NULL) {
        LOG_DEBUG("Values is NULL in array type setting function. Try set NULL value.");
        return try_set_null_val(mutation->record, colIndex);
    }
    int length = 20 + 4 * nValues + 8 * nValues;
    char* ptr = new_record_val(mutation->record, length);
    convert_array_to_postgres_binary(ptr, values, nValues, 8, HOLO_TYPE_FLOAT8);
    return set_record_val(mutation->record, colIndex, ptr, 1, length);
}

int holo_client_set_req_text_array_val_by_colindex(HoloMutation mutation, int colIndex, char** values, int nValues){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_TEXT_ARRAY)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    if (values == NULL) {
        LOG_DEBUG("Values is NULL in array type setting function. Try set NULL value.");
        return try_set_null_val(mutation->record, colIndex);
    }
    int length = 20 + 4 * nValues;
    for (int i = 0;i < nValues;i++){
        if (values[i] == NULL) continue;
        length += strlen(values[i]);
    }
    char* ptr = new_record_val(mutation->record, length);
    convert_text_array_to_postgres_binary(ptr, values, nValues, HOLO_TYPE_TEXT);
    return set_record_val(mutation->record, colIndex, ptr, 1, length);
}

int holo_client_set_req_null_val_by_colindex(HoloMutation mutation, int colIndex){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (!set_req_val_by_colindex_is_valid(mutation, colIndex)) return HOLO_CLIENT_INVALID_COL_IDX;
    return try_set_null_val(mutation->record, colIndex);
}

int try_get_colindex_by_colname(HoloMutation mutation, const char* colName) {
    if (mutation == NULL){
        LOG_ERROR("HoloMutation is NULL.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    if (colName == NULL){
        LOG_ERROR("Column name is NULL.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    int colIndex = get_colindex_by_colname(mutation->record->schema, colName);
    if (colIndex < 0) {
        LOG_ERROR("Column \"%s\" does not exist for table \"%s\".", colName, mutation->record->schema->tableName->tableName);
        return HOLO_CLIENT_INVALID_COL_NAME;
    }
    return colIndex;
}

int holo_client_set_req_val_with_text_by_colname(HoloMutation mutation, const char* colName, const char* value, int len){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    int colIndex = try_get_colindex_by_colname(mutation, colName);
    if (colIndex < 0) return colIndex;
    if (value == NULL){
        return holo_client_set_req_null_val_by_colindex(mutation, colIndex);
    }
    return set_record_val_by_type(mutation->record, colIndex, value, len);
}

int holo_client_set_req_int16_val_by_colname(HoloMutation mutation, const char* colName, int16_t value){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    int colIndex = try_get_colindex_by_colname(mutation, colName);
    if (colIndex < 0) return colIndex;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_INT2)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    int16_t* ptr = new_record_val(mutation->record, 2);
    *ptr = value;
    endian_swap(ptr, 2);
    return set_record_val(mutation->record, colIndex, (char*)ptr, 1, 2);
}

int holo_client_set_req_int32_val_by_colname(HoloMutation mutation, const char* colName, int32_t value){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    int colIndex = try_get_colindex_by_colname(mutation, colName);
    if (colIndex < 0) return colIndex;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_INT4)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    int32_t* ptr = new_record_val(mutation->record, 4);
    *ptr = value;
    endian_swap(ptr, 4);
    return set_record_val(mutation->record, colIndex, (char*)ptr, 1, 4);
}

int holo_client_set_req_int64_val_by_colname(HoloMutation mutation, const char* colName, int64_t value){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    int colIndex = try_get_colindex_by_colname(mutation, colName);
    if (colIndex < 0) return colIndex;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_INT8)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    int64_t* ptr = new_record_val(mutation->record, 8);
    *ptr = value;
    endian_swap(ptr, 8);
    return set_record_val(mutation->record, colIndex, (char*)ptr, 1, 8);
}

int holo_client_set_req_bool_val_by_colname(HoloMutation mutation, const char* colName, bool value){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    int colIndex = try_get_colindex_by_colname(mutation, colName);
    if (colIndex < 0) return colIndex;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_BOOL)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    bool* ptr = new_record_val(mutation->record, 1);
    *ptr = value;
    return set_record_val(mutation->record, colIndex, (char*)ptr, 1, 1);
}

int holo_client_set_req_float_val_by_colname(HoloMutation mutation, const char* colName, float value){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    int colIndex = try_get_colindex_by_colname(mutation, colName);
    if (colIndex < 0) return colIndex;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_FLOAT4)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    float* ptr = new_record_val(mutation->record, 4);
    *ptr = value;
    endian_swap(ptr, 4);
    return set_record_val(mutation->record, colIndex, (char*)ptr, 1, 4);
}

int holo_client_set_req_double_val_by_colname(HoloMutation mutation, const char* colName, double value){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    int colIndex = try_get_colindex_by_colname(mutation, colName);
    if (colIndex < 0) return colIndex;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_FLOAT8)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    double* ptr = new_record_val(mutation->record, 8);
    *ptr = value;
    endian_swap(ptr, 8);
    return set_record_val(mutation->record, colIndex, (char*)ptr, 1, 8);
}

int holo_client_set_req_text_val_by_colname(HoloMutation mutation, const char* colName, const char* value, int len) {
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    int colIndex = try_get_colindex_by_colname(mutation, colName);
    if (colIndex < 0) return colIndex;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_TEXT)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    if (value == NULL){
        if (mutation->record->schema->columns[colIndex].nullable == false) {
            LOG_ERROR("Column \"%s\" can not be null but set null for table \"%s\".", colName, mutation->record->schema->tableName->tableName);
            return HOLO_CLIENT_NOT_NULL_BUT_SET_NULL;
        } else {
            return set_record_val(mutation->record, colIndex, NULL, 0, 0);
        }
    }
    char* ptr = (char*)new_record_val(mutation->record, len + 1);
    deep_copy_string_to(value, ptr, len);
    // add '\0', PQexecPrepared need it when processing text as parameter
    ptr[len] = '\0';
    return set_record_val(mutation->record, colIndex, ptr, 0, len + 1);
}

int holo_client_set_req_timestamp_val_by_colname(HoloMutation mutation, const char* colName, int64_t value) {
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    int colIndex = try_get_colindex_by_colname(mutation, colName);
    if (colIndex < 0) return colIndex;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_TIMESTAMP)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    int64_t* ptr = MALLOC(1, int64_t);
    *ptr = value;
    endian_swap(ptr, 8);
    return set_record_val(mutation->record, colIndex, (char*)ptr, 1, 8);
}

int holo_client_set_req_timestamptz_val_by_colname(HoloMutation mutation, const char* colName, int64_t value) {
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    int colIndex = try_get_colindex_by_colname(mutation, colName);
    if (colIndex < 0) return colIndex;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_TIMESTAMPTZ)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    int64_t* ptr = MALLOC(1, int64_t);
    *ptr = value;
    endian_swap(ptr, 8);
    return set_record_val(mutation->record, colIndex, (char*)ptr, 1, 8);
}

int holo_client_set_req_int32_array_val_by_colname(HoloMutation mutation, const char* colName, const int32_t* values, int nValues){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    int colIndex = try_get_colindex_by_colname(mutation, colName);
    if (colIndex < 0) return colIndex;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_INT4_ARRAY)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    if (values == NULL) {
        LOG_DEBUG("Values is NULL in array type setting function. Try set NULL value.");
        return try_set_null_val(mutation->record, colIndex);
    }
    int length = 20 + 4 * nValues + 4 * nValues;
    char* ptr = new_record_val(mutation->record, length);
    convert_array_to_postgres_binary(ptr, values, nValues, 4, HOLO_TYPE_INT4);
    return set_record_val(mutation->record, colIndex, ptr, 1, length);
}


int holo_client_set_req_int64_array_val_by_colname(HoloMutation mutation, const char* colName, const int64_t* values, int nValues){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    int colIndex = try_get_colindex_by_colname(mutation, colName);
    if (colIndex < 0) return colIndex;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_INT8_ARRAY)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    if (values == NULL) {
        LOG_DEBUG("Values is NULL in array type setting function. Try set NULL value.");
        return try_set_null_val(mutation->record, colIndex);
    }
    int length = 20 + 4 * nValues + 8 * nValues;
    char* ptr = new_record_val(mutation->record, length);
    convert_array_to_postgres_binary(ptr, values, nValues, 8, HOLO_TYPE_INT8);
    return set_record_val(mutation->record, colIndex, ptr, 1, length);
}

int holo_client_set_req_bool_array_val_by_colname(HoloMutation mutation, const char* colName, const bool* values, int nValues){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    int colIndex = try_get_colindex_by_colname(mutation, colName);
    if (colIndex < 0) return colIndex;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_BOOL_ARRAY)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    if (values == NULL) {
        LOG_DEBUG("Values is NULL in array type setting function. Try set NULL value.");
        return try_set_null_val(mutation->record, colIndex);
    }
    int length = 20 + 4 * nValues + 1 * nValues;
    char* ptr = new_record_val(mutation->record, length);
    convert_array_to_postgres_binary(ptr, values, nValues, 1, HOLO_TYPE_BOOL);
    return set_record_val(mutation->record, colIndex, ptr, 1, length);
}

int holo_client_set_req_float_array_val_by_colname(HoloMutation mutation, const char* colName, const float* values, int nValues){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    int colIndex = try_get_colindex_by_colname(mutation, colName);
    if (colIndex < 0) return colIndex;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_FLOAT4_ARRAY)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    if (values == NULL) {
        LOG_DEBUG("Values is NULL in array type setting function. Try set NULL value.");
        return try_set_null_val(mutation->record, colIndex);
    }
    int length = 20 + 4 * nValues + 4 * nValues;
    char* ptr = new_record_val(mutation->record, length);
    convert_array_to_postgres_binary(ptr, values, nValues, 4, HOLO_TYPE_FLOAT4);
    return set_record_val(mutation->record, colIndex, ptr, 1, length);
}

int holo_client_set_req_double_array_val_by_colname(HoloMutation mutation, const char* colName, const double* values, int nValues){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    int colIndex = try_get_colindex_by_colname(mutation, colName);
    if (colIndex < 0) return colIndex;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_FLOAT8_ARRAY)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    if (values == NULL) {
        LOG_DEBUG("Values is NULL in array type setting function. Try set NULL value.");
        return try_set_null_val(mutation->record, colIndex);
    }
    int length = 20 + 4 * nValues + 8 * nValues;
    char* ptr = new_record_val(mutation->record, length);
    convert_array_to_postgres_binary(ptr, values, nValues, 8, HOLO_TYPE_FLOAT8);
    return set_record_val(mutation->record, colIndex, ptr, 1, length);
}

int holo_client_set_req_text_array_val_by_colname(HoloMutation mutation, const char* colName, char** values, int nValues){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    int colIndex = try_get_colindex_by_colname(mutation, colName);
    if (colIndex < 0) return colIndex;
    if (!column_type_matches_oid(mutation->record, colIndex, HOLO_TYPE_TEXT_ARRAY)) return HOLO_CLIENT_TYPE_NOT_MATCH;
    if (values == NULL) {
        LOG_DEBUG("Values is NULL in array type setting function. Try set NULL value.");
        return try_set_null_val(mutation->record, colIndex);
    }
    int length = 20 + 4 * nValues;
    for (int i = 0;i < nValues;i++){
        if (values[i] == NULL) continue;
        length += strlen(values[i]);
    }
    char* ptr = new_record_val(mutation->record, length);
    convert_text_array_to_postgres_binary(ptr, values, nValues, HOLO_TYPE_TEXT);
    return set_record_val(mutation->record, colIndex, ptr, 1, length);
}

int holo_client_set_req_null_val_by_colname(HoloMutation mutation, const char* colName){
    if (mutation == NULL) {
        LOG_ERROR("HoloMutation is NULL when set value.");
        return HOLO_CLIENT_INVALID_PARAM;
    }
    int colIndex = try_get_colindex_by_colname(mutation, colName);
    if (colIndex < 0) return colIndex;
    if (mutation->record->schema->columns[colIndex].nullable == false){
        LOG_ERROR("Column \"%s\" can not be null but set null for table \"%s\".", colName, mutation->record->schema->tableName->tableName);
        return HOLO_CLIENT_NOT_NULL_BUT_SET_NULL;
    }
    return set_record_val(mutation->record, colIndex, NULL, 0, 0);
}

void holo_client_destroy_mutation_request(HoloMutation mutation) {
    if (mutation == NULL){
        LOG_ERROR("HoloMutation is NULL.");
        return;
    }
    dlist_mutable_iter miter;
    MutationItem* item;
    dlist_foreach_modify(miter, &(mutation->attachmentList)) {
        item = dlist_container(MutationItem, list_node, miter.cur);
        holo_client_destroy_mutation_request(item->mutation);
        dlist_delete(miter.cur);
        FREE(item);
    }
    holo_client_destroy_record(mutation->record);
    FREE(mutation);
    mutation = NULL;
}

int normalize_mutation_request(HoloMutation mutation){
    if (mutation->mode == DELETE){ 
        for (int i = 0;i < mutation->record->schema->nColumns;i++){
            if (!mutation->record->valuesSet[i]) {
                if (mutation->record->schema->columns[i].isPrimaryKey){
                    LOG_ERROR("Primary key \"%s\" not set in DELETE for table \"%s\".", mutation->record->schema->columns[i].name, mutation->record->schema->tableName->tableName);
                    return HOLO_CLIENT_PK_NOT_SET_IN_DELETE;
                }
                continue;
            }
            if (mutation->record->schema->columns[i].isPrimaryKey) continue;
            mutation->record->valuesSet[i] = false;
            LOG_DEBUG("Column \"%s\" is not primary key but set in table \"%s\". Ignored.", mutation->record->schema->columns[i].name, mutation->record->schema->tableName->tableName);
            mutation->record->nValues--;
        }
        return HOLO_CLIENT_RET_OK;
    }
    if (mutation->mode != PUT) return HOLO_CLIENT_RET_FAIL;
    if (mutation->record->nValues == 0) LOG_WARN("Nothing set in mutation.");
    for (int i = 0;i < mutation->record->schema->nColumns;i++){
        if (mutation->record->valuesSet[i]) continue;
        if (!mutation->record->schema->columns[i].nullable) {
            LOG_ERROR("Column \"%s\" can not be null but not set for table \"%s\".", mutation->record->schema->columns[i].name, mutation->record->schema->tableName->tableName);
            return HOLO_CLIENT_NOT_NULL_BUT_NOT_SET;
        }
        if (mutation->writeMode == INSERT_OR_UPDATE) continue;
        if (mutation->record->schema->columns[i].defaultValue != NULL) holo_client_set_req_val_with_text_by_colindex(mutation, i, mutation->record->schema->columns[i].defaultValue, strlen(mutation->record->schema->columns[i].defaultValue));
        else if (mutation->record->schema->columns[i].nullable) holo_client_set_req_null_val_by_colindex(mutation, i);
    }
    return HOLO_CLIENT_RET_OK;
}

void mutation_add_attachment(HoloMutation m, HoloMutation attachment) {
    dlist_push_tail(&m->attachmentList, &create_mutation_item(attachment)->list_node);
}

void mutation_request_cover(HoloMutation dst, HoloMutation src) {
    //dst.attachmentList = src.attachmentList + src
    MutationItem* item;
    dlist_mutable_iter miter;
    dlist_foreach_modify(miter, &(src->attachmentList)) {
        item = dlist_container(MutationItem, list_node, miter.cur);
        dlist_delete(miter.cur);
        dlist_push_tail(&(dst->attachmentList), &(item->list_node));
    }
    mutation_add_attachment(dst, src);
}

void mutation_request_update(HoloMutation origin, HoloMutation m) {
    //origin 根据m set的value update
    for (int i = 0; i < origin->record->schema->nColumns; i++) {
        if (!origin->record->schema->columns[i].isPrimaryKey && m->record->valuesSet[i]) {
            origin->record->byteSize -= origin->record->valueLengths[i];
            void* newOriginValue = new_record_val(origin->record, m->record->valueLengths[i]);
            void* newMValue = new_record_val(m->record, origin->record->valueLengths[i]);
            memcpy(newOriginValue, m->record->values[i], m->record->valueLengths[i]);
            memcpy(newMValue, origin->record->values[i], origin->record->valueLengths[i]);
            destroy_record_val(origin->record, i);
            destroy_record_val(m->record, i);
            origin->record->values[i] = newOriginValue;
            m->record->values[i] = newMValue;
            origin->record->valueFormats[i] += m->record->valueFormats[i];
            m->record->valueFormats[i] = origin->record->valueFormats[i] - m->record->valueFormats[i];
            origin->record->valueFormats[i] = origin->record->valueFormats[i] - m->record->valueFormats[i];
            origin->record->valueLengths[i] += m->record->valueLengths[i];
            m->record->valueLengths[i] = origin->record->valueLengths[i] - m->record->valueLengths[i];
            origin->record->valueLengths[i] = origin->record->valueLengths[i] - m->record->valueLengths[i];
            if (!origin->record->valuesSet[i]) {
                origin->record->valuesSet[i] = true;
                origin->record->nValues++;
            }
            origin->record->byteSize += origin->record->valueLengths[i];
        }
    }
    origin->byteSize = sizeof(HoloMutationRequest) + origin->record->byteSize;
    holo_client_destroy_mutation_request(m);
}

HoloMutation mutation_request_merge(HoloMutation origin, HoloMutation m){
    if (m->mode == DELETE) {
        // ?? DELETE
        mutation_request_cover(m, origin);
        LOG_DEBUG("Mutation merged by DELETE. seq:%"PRId64",timestamp:%"PRId64".", origin->record->sequence, origin->record->timestamp);
        return m;
    } 
    else if (origin->mode == DELETE) {
        // DELETE INSERT
        m->writeMode = INSERT_OR_REPLACE;
        normalize_mutation_request(m);
        mutation_request_cover(m, origin);
        LOG_DEBUG("DELETE mutation merged by INSERT. seq:%"PRId64",timestamp:%"PRId64".", origin->record->sequence, origin->record->timestamp);
        return m;
    } 
    else {
        //INSERT INSERT
        switch (m->writeMode) {
            case INSERT_OR_IGNORE:
                mutation_request_cover(origin, m);
                LOG_DEBUG("INSERT mutation ignored. seq:%"PRId64",timestamp:%"PRId64".", m->record->sequence, m->record->timestamp);
                return origin;
            case INSERT_OR_REPLACE:
                mutation_request_cover(m, origin);
                LOG_DEBUG("INSERT mutation replaced. seq:%"PRId64",timestamp:%"PRId64".", origin->record->sequence, origin->record->timestamp);
                return m;
            case INSERT_OR_UPDATE:
                mutation_request_update(origin, m);
                LOG_DEBUG("INSERT mutation updated. seq:%"PRId64",timestamp:%"PRId64".", m->record->sequence, m->record->timestamp);
                return origin;
            default:
                LOG_ERROR("Invalid HoloWriteMode"); 
                mutation_request_cover(m, origin);
                LOG_DEBUG("INSERT mutation replaced. seq:%"PRId64",timestamp:%"PRId64".", origin->record->sequence, origin->record->timestamp);
                return m;
        }
    }
}

MutationItem* create_mutation_item(HoloMutation mutation){
    MutationItem* item = MALLOC(1, MutationItem);
    item->mutation = mutation;
    return item;
}

void holo_clilent_set_request_mode(HoloMutation mutation, HoloMutationMode mode) {
    mutation->mode = mode;
}

HoloGet holo_client_new_get_request(HoloTableSchema* schema) {
    if (schema == NULL){
        LOG_ERROR("Table schema is NULL.");
        return NULL;
    }
    if (schema->nPrimaryKeys == 0) {
        LOG_ERROR("Table \"%s\" has no primary key!", schema->tableName->tableName);
        return NULL;
    }
    HoloGet get = MALLOC(1, HoloGetRequest);
    get->record = holo_client_new_record(schema);
    get->future = create_future();
    get->submitted = false;
    return get;
}

bool set_get_val_by_colindex_is_valid(HoloGet get, int colIndex){
    if (get == NULL){
        LOG_ERROR("HoloGet is NULL.");
        return false;
    }
    if (colIndex < 0 || colIndex >= get->record->schema->nColumns) {
        LOG_ERROR("Column index %d exceeds column number for table \"%s\".", colIndex, get->record->schema->tableName->tableName);
        return false;
    }
    return true;
}

bool set_val_already_set(HoloGet get, int colIndex) {
    if (get->record->valuesSet[colIndex]) {
        LOG_ERROR("Column %d already set for table \"%s\".", colIndex, get->record->schema->tableName->tableName);
        return true;
    }
    return false;
}

int holo_client_set_get_val_with_text_by_colindex(HoloGet get, int colIndex, const char* value, int len) {
    if (!set_get_val_by_colindex_is_valid(get, colIndex)) return HOLO_CLIENT_INVALID_COL_IDX;
    if (set_val_already_set(get, colIndex)) return HOLO_CLIENT_COL_ALREADY_SET;
    if (!get->record->schema->columns[colIndex].isPrimaryKey) {
        LOG_ERROR("Index %d is not primary key of table \"%s\"", colIndex, get->record->schema->tableName->tableName);
        return HOLO_CLIENT_COL_NOT_PK;
    }
    char* ptr = (char*)new_record_val(get->record, len + 1);
    deep_copy_string_to(value, ptr, len);
    // add '\0', PQexecPrepared need it when processing text as parameter
    ptr[len] = '\0';
    return set_record_val(get->record, colIndex, ptr, 0, len + 1);
}

void holo_client_destroy_get_request(HoloGet get) {
    if (get == NULL) {
        return;
    }
    HoloRecord* getRes = NULL;
    holo_client_destroy_record(get->record);
    if (get->submitted) {
        //destroy return value
        getRes = get_future_result(get->future);
        holo_client_destroy_record(getRes);
    }
    destroy_future(get->future);
    FREE(get);
    get = NULL;
}

GetItem* create_get_item(HoloGet get) {
    GetItem* item = MALLOC(1, GetItem);
    item->get = get;
    return item;
}

Sql holo_client_new_sql_request(SqlFunction sqlFunction, void* arg) {
    Sql sql = MALLOC(1, SqlRequest);
    sql->sqlFunction = sqlFunction;
    sql->arg = arg;
    sql->future = create_future();
    return sql;
}

void holo_client_destroy_sql_request(Sql sql) {
    destroy_future(sql->future);
    FREE(sql);
    sql = NULL;
}