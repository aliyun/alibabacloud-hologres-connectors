#ifndef _RECORD_H_
#define _RECORD_H_

#include <stdbool.h>
#include <stdint.h>

struct _Record;
typedef struct _Record Record;

char* holo_client_record_table_name(Record*);
int holo_client_record_num_column(Record*);

int16_t holo_client_get_record_int16_val_by_colindex(Record* record, int colIndex); //smallint
int32_t holo_client_get_record_int32_val_by_colindex(Record* record, int colIndex); //int
int64_t holo_client_get_record_int64_val_by_colindex(Record* record, int colIndex); //bigint
bool holo_client_get_record_bool_val_by_colindex(Record* record, int colIndex);     //bool
float holo_client_get_record_float_val_by_colindex(Record* record, int colIndex);   //real(float4)
double holo_client_get_record_double_val_by_colindex(Record* record, int colIndex); //float(float8 double precision)

char* holo_client_get_record_text_val_by_colindex(Record* record, int colIndex);    //text
int32_t* holo_client_get_record_int32_array_val_by_colindex(Record* record, int colIndex, int* numValues);  //int[]
int64_t* holo_client_get_record_int64_array_val_by_colindex(Record* record, int colIndex, int* numValues);  //bigint[]
bool* holo_client_get_record_bool_array_val_by_colindex(Record* record, int colIndex, int* numValues);      //bool[]
float* holo_client_get_record_float_array_val_by_colindex(Record* record, int colIndex, int* numValues);    //float4[]
double* holo_client_get_record_double_array_val_by_colindex(Record* record, int colIndex, int* numValues);  //float8[]

char* holo_client_get_record_val_with_text_by_colindex(Record* record, int colIndex); //所有类型
//所有返回类型为一维指针的（除了int bool float double char**）的返回值都需要destroy
void holo_client_destroy_val(void* val);

char** holo_client_get_record_text_array_val_by_colindex(Record* record, int colIndex, int* numValues);     //text[]
void holo_client_destroy_text_array_val(char** val, int n);



#endif