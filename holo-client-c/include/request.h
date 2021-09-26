#ifndef _REQUEST_H_
#define _REQUEST_H_

#include <stdint.h>
#include "table_schema.h"
#include "record.h"

typedef enum _HoloMutationMode {
    PUT,
    DELETE
} HoloMutationMode;

typedef struct _MutationRequest MutationRequest;
typedef MutationRequest* Mutation;
Mutation holo_client_new_mutation_request(TableSchema*);

int holo_client_set_request_mode(Mutation mutation, HoloMutationMode mode);

//对于smallint, int, bigint, bool, float4, float8类型，在本地将字符串处理为二进制形式，否则直接设置
int holo_client_set_req_val_with_text_by_colname(Mutation mutation, char *colName, char* value); //所有类型
int holo_client_set_req_int16_val_by_colname(Mutation mutation, char *colName, int16_t value);  //smallint
int holo_client_set_req_int32_val_by_colname(Mutation mutation, char *colName, int32_t value);  //int
int holo_client_set_req_int64_val_by_colname(Mutation mutation, char *colName, int64_t value);  //bigint
int holo_client_set_req_bool_val_by_colname(Mutation mutation, char *colName, bool value);      //bool
int holo_client_set_req_float_val_by_colname(Mutation mutation, char *colName, float value);    //real(float4)
int holo_client_set_req_double_val_by_colname(Mutation mutation, char *colName, double value);  //float(float8 double precision)
int holo_client_set_req_text_val_by_colname(Mutation mutation, char *colName, char* value);     //text
int holo_client_set_req_timestamp_val_by_colname(Mutation mutation, char *colName, int64_t value);       //timestamp
int holo_client_set_req_timestamptz_val_by_colname(Mutation mutation, char *colName, int64_t value);     //timestamptz
int holo_client_set_req_int32_array_val_by_colname(Mutation mutation, char *colName, int32_t* values, int nValues);  //int[]
int holo_client_set_req_int64_array_val_by_colname(Mutation mutation, char *colName, int64_t* values, int nValues);  //bigint[]
int holo_client_set_req_bool_array_val_by_colname(Mutation mutation, char *colName, bool* values, int nValues);      //bool[]
int holo_client_set_req_float_array_val_by_colname(Mutation mutation, char *colName, float* values, int nValues);    //float4[]
int holo_client_set_req_double_array_val_by_colname(Mutation mutation, char *colName, double* values, int nValues);  //float8[]
int holo_client_set_req_text_array_val_by_colname(Mutation mutation, char *colName, char** values, int nValues);     //text[]
int holo_client_set_req_null_val_by_colname(Mutation mutation, char *colName);

//对于smallint, int, bigint, bool, float4, float8类型，在本地将字符串处理为二进制形式，否则直接设置
int holo_client_set_req_val_with_text_by_colindex(Mutation mutation, int colIndex, char* value); //所有类型
int holo_client_set_req_int16_val_by_colindex(Mutation mutation, int colIndex, int16_t value);   //smallint
int holo_client_set_req_int32_val_by_colindex(Mutation mutation, int colIndex, int32_t value);   //int
int holo_client_set_req_int64_val_by_colindex(Mutation mutation, int colIndex, int64_t value);   //bigint
int holo_client_set_req_bool_val_by_colindex(Mutation mutation, int colIndex, bool value);       //bool
int holo_client_set_req_float_val_by_colindex(Mutation mutation, int colIndex, float value);     //real(float4)
int holo_client_set_req_double_val_by_colindex(Mutation mutation, int colIndex, double value);   //float(float8 double precision)
int holo_client_set_req_text_val_by_colindex(Mutation mutation, int colIndex, char *value);      //text
int holo_client_set_req_timestamp_val_by_colindex(Mutation mutation, int colIndex, int64_t value);        //timestamp
int holo_client_set_req_timestamptz_val_by_colindex(Mutation mutation, int colIndex, int64_t value);      //timestamptz
int holo_client_set_req_int32_array_val_by_colindex(Mutation mutation, int colIndex, int32_t* values, int nValues);  //int[]
int holo_client_set_req_int64_array_val_by_colindex(Mutation mutation, int colIndex, int64_t* values, int nValues);  //bigint[]
int holo_client_set_req_bool_array_val_by_colindex(Mutation mutation, int colIndex, bool* values, int nValues);      //bool[]
int holo_client_set_req_float_array_val_by_colindex(Mutation mutation, int colIndex, float* values, int nValues);    //float4[]
int holo_client_set_req_double_array_val_by_colindex(Mutation mutation, int colIndex, double* values, int nValues);  //float8[]
int holo_client_set_req_text_array_val_by_colindex(Mutation mutation, int colIndex, char** values, int nValues);     //text[]
int holo_client_set_req_null_val_by_colindex(Mutation mutation, int colIndex);

struct _GetRequest;
typedef struct _GetRequest GetRequest;
typedef GetRequest* Get;
Get holo_client_new_get_request(TableSchema*);
int holo_client_set_get_val_with_text_by_colindex(Get get, int colIndex, char* value);

void holo_client_destroy_get_request(Get); //同时会destroy 得到的result

#endif