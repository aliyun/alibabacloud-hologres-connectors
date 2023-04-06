#ifndef _REQUEST_H_
#define _REQUEST_H_

#include <stdint.h>
#include "defs.h"
#include "table_schema.h"
#include "record.h"

__HOLO_CLIENT_BEGIN_DECLS

typedef enum _HoloMutationMode {
    PUT,
    DELETE
} HoloMutationMode;

typedef struct _HoloMutationRequest HoloMutationRequest;
typedef HoloMutationRequest* HoloMutation;
HoloMutation holo_client_new_mutation_request(HoloTableSchema*);

int holo_client_set_request_mode(HoloMutation mutation, HoloMutationMode mode);
int holo_client_mutation_byte_size(const HoloMutation mutation); //在submit之后调用，否则获取的size不对

//对于smallint, int, bigint, bool, float4, float8类型，在本地将字符串处理为二进制形式，否则直接设置
int holo_client_set_req_val_with_text_by_colname(HoloMutation mutation, const char* colName, const char* value, int len); //所有类型
int holo_client_set_req_int16_val_by_colname(HoloMutation mutation, const char* colName, int16_t value);  //smallint
int holo_client_set_req_int32_val_by_colname(HoloMutation mutation, const char* colName, int32_t value);  //int
int holo_client_set_req_int64_val_by_colname(HoloMutation mutation, const char* colName, int64_t value);  //bigint
int holo_client_set_req_bool_val_by_colname(HoloMutation mutation, const char* colName, bool value);      //bool
int holo_client_set_req_float_val_by_colname(HoloMutation mutation, const char* colName, float value);    //real(float4)
int holo_client_set_req_double_val_by_colname(HoloMutation mutation, const char* colName, double value);  //float(float8 double precision)
int holo_client_set_req_text_val_by_colname(HoloMutation mutation, const char* colName, const char* value, int len);     //text
int holo_client_set_req_timestamp_val_by_colname(HoloMutation mutation, const char* colName, int64_t value);       //timestamp
int holo_client_set_req_timestamptz_val_by_colname(HoloMutation mutation, const char* colName, int64_t value);     //timestamptz
int holo_client_set_req_int32_array_val_by_colname(HoloMutation mutation, const char* colName, const int32_t* values, int nValues);  //int[]
int holo_client_set_req_int64_array_val_by_colname(HoloMutation mutation, const char* colName, const int64_t* values, int nValues);  //bigint[]
int holo_client_set_req_bool_array_val_by_colname(HoloMutation mutation, const char* colName, const bool* values, int nValues);      //bool[]
int holo_client_set_req_float_array_val_by_colname(HoloMutation mutation, const char* colName, const float* values, int nValues);    //float4[]
int holo_client_set_req_double_array_val_by_colname(HoloMutation mutation, const char* colName, const double* values, int nValues);  //float8[]
int holo_client_set_req_text_array_val_by_colname(HoloMutation mutation, const char* colName, char** values, int nValues);     //text[]
int holo_client_set_req_null_val_by_colname(HoloMutation mutation, const char* colName);

//对于smallint, int, bigint, bool, float4, float8类型，在本地将字符串处理为二进制形式，否则直接设置
int holo_client_set_req_val_with_text_by_colindex(HoloMutation mutation, int colIndex, const char* value, int len); //所有类型
int holo_client_set_req_int16_val_by_colindex(HoloMutation mutation, int colIndex, int16_t value);   //smallint
int holo_client_set_req_int32_val_by_colindex(HoloMutation mutation, int colIndex, int32_t value);   //int
int holo_client_set_req_int64_val_by_colindex(HoloMutation mutation, int colIndex, int64_t value);   //bigint
int holo_client_set_req_bool_val_by_colindex(HoloMutation mutation, int colIndex, bool value);       //bool
int holo_client_set_req_float_val_by_colindex(HoloMutation mutation, int colIndex, float value);     //real(float4)
int holo_client_set_req_double_val_by_colindex(HoloMutation mutation, int colIndex, double value);   //float(float8 double precision)
int holo_client_set_req_text_val_by_colindex(HoloMutation mutation, int colIndex, const char* value, int len);      //text
int holo_client_set_req_timestamp_val_by_colindex(HoloMutation mutation, int colIndex, int64_t value);        //timestamp
int holo_client_set_req_timestamptz_val_by_colindex(HoloMutation mutation, int colIndex, int64_t value);      //timestamptz
int holo_client_set_req_int32_array_val_by_colindex(HoloMutation mutation, int colIndex, const int32_t* values, int nValues);  //int[]
int holo_client_set_req_int64_array_val_by_colindex(HoloMutation mutation, int colIndex, const int64_t* values, int nValues);  //bigint[]
int holo_client_set_req_bool_array_val_by_colindex(HoloMutation mutation, int colIndex, const bool* values, int nValues);      //bool[]
int holo_client_set_req_float_array_val_by_colindex(HoloMutation mutation, int colIndex, const float* values, int nValues);    //float4[]
int holo_client_set_req_double_array_val_by_colindex(HoloMutation mutation, int colIndex, const double* values, int nValues);  //float8[]
int holo_client_set_req_text_array_val_by_colindex(HoloMutation mutation, int colIndex, char** values, int nValues);     //text[]
int holo_client_set_req_null_val_by_colindex(HoloMutation mutation, int colIndex);

struct _HoloGetRequest;
typedef struct _HoloGetRequest HoloGetRequest;
typedef HoloGetRequest* HoloGet;
HoloGet holo_client_new_get_request(HoloTableSchema*);
int holo_client_set_get_val_with_text_by_colindex(HoloGet get, int colIndex, const char* value, int len);

void holo_client_destroy_get_request(HoloGet); //同时会destroy 得到的result

__HOLO_CLIENT_END_DECLS

#endif