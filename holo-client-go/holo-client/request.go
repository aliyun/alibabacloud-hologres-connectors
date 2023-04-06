package holoclient

/*
#cgo CFLAGS: -I./include
#cgo LDFLAGS: -L./lib -lholo-client
#include "request.h"
#include "table_schema.h"
#include <stdlib.h>
#include <stdbool.h>
*/
import "C"

import "unsafe"

type HoloMutationMode C.HoloMutationMode

const (
	PUT    HoloMutationMode = C.PUT
	DELETE HoloMutationMode = C.DELETE
)

type HoloTableSchema struct {
	ctableSchema *C.HoloTableSchema
}

func (schema *HoloTableSchema) NumColumns() int {
	res := schema.ctableSchema.nColumns
	return int(res)
}

type mutationRequest struct {
	cmutation C.HoloMutation
}

func NewMutationRequest(schema *HoloTableSchema) *mutationRequest {
	m := new(mutationRequest)
	m.cmutation = C.holo_client_new_mutation_request(schema.ctableSchema)
	return m
}

func (mutation *mutationRequest) SetRequestMode(mode HoloMutationMode) {
	C.holo_client_set_request_mode(mutation.cmutation, C.HoloMutationMode(mode))
}

// Set value by Colindex (recommended)

//所有类型都可以以string类型设置
func (mutation *mutationRequest) SetValWithTextByColIndex(colIndex int, value string, len int) int {
	cvalue := C.CString(value)
	res := C.holo_client_set_req_val_with_text_by_colindex(mutation.cmutation, C.int(colIndex), cvalue, C.int(len))
	C.free(unsafe.Pointer(cvalue))
	return int(res)
}

//smallint
func (mutation *mutationRequest) SetInt16ValByColIndex(colIndex int, value int16) int {
	res := C.holo_client_set_req_int16_val_by_colindex(mutation.cmutation, C.int(colIndex), C.int16_t(value))
	return int(res)
}

//int
func (mutation *mutationRequest) SetInt32ValByColIndex(colIndex int, value int32) int {
	res := C.holo_client_set_req_int32_val_by_colindex(mutation.cmutation, C.int(colIndex), C.int32_t(value))
	return int(res)
}

//bigint
func (mutation *mutationRequest) SetInt64ValByColIndex(colIndex int, value int64) int {
	res := C.holo_client_set_req_int64_val_by_colindex(mutation.cmutation, C.int(colIndex), C.int64_t(value))
	return int(res)
}

//bool
func (mutation *mutationRequest) SetBoolValByColIndex(colIndex int, value bool) int {
	res := C.holo_client_set_req_bool_val_by_colindex(mutation.cmutation, C.int(colIndex), C.bool(value))
	return int(res)
}

//real(float4)
func (mutation *mutationRequest) SetFloat32ValByColIndex(colIndex int, value float32) int {
	res := C.holo_client_set_req_float_val_by_colindex(mutation.cmutation, C.int(colIndex), C.float(value))
	return int(res)
}

//float(float8 double precision)
func (mutation *mutationRequest) SetFloat64ValByColIndex(colIndex int, value float64) int {
	res := C.holo_client_set_req_double_val_by_colindex(mutation.cmutation, C.int(colIndex), C.double(value))
	return int(res)
}

//text
func (mutation *mutationRequest) SetTextValByColIndex(colIndex int, value string, len int) int {
	cvalue := C.CString(value)
	res := C.holo_client_set_req_text_val_by_colindex(mutation.cmutation, C.int(colIndex), cvalue, C.int(len))
	C.free(unsafe.Pointer(cvalue))
	return int(res)
}

//timestamp
func (mutation *mutationRequest) SetTimestampValByColIndex(colIndex int, value int64) int {
	res := C.holo_client_set_req_timestamp_val_by_colindex(mutation.cmutation, C.int(colIndex), C.int64_t(value))
	return int(res)
}

//timestamptz
func (mutation *mutationRequest) SetTimestamptzValByColIndex(colIndex int, value int64) int {
	res := C.holo_client_set_req_timestamptz_val_by_colindex(mutation.cmutation, C.int(colIndex), C.int64_t(value))
	return int(res)
}

//int[]
func (mutation *mutationRequest) SetInt32ArrayValByColIndex(colIndex int, value []int32) int {
	var dataPtr *C.int32_t
	if len(value) == 0 {
		dataPtr = (*C.int32_t)(unsafe.Pointer(&value))
	} else {
		dataPtr = (*C.int32_t)(unsafe.Pointer(&value[0]))
	}
	res := C.holo_client_set_req_int32_array_val_by_colindex(mutation.cmutation, C.int(colIndex), dataPtr, C.int(len(value)))
	return int(res)
}

//bigint[]
func (mutation *mutationRequest) SetInt64ArrayValByColIndex(colIndex int, value []int64) int {
	var dataPtr *C.int64_t
	if len(value) == 0 {
		dataPtr = (*C.int64_t)(unsafe.Pointer(&value))
	} else {
		dataPtr = (*C.int64_t)(unsafe.Pointer(&value[0]))
	}
	res := C.holo_client_set_req_int64_array_val_by_colindex(mutation.cmutation, C.int(colIndex), dataPtr, C.int(len(value)))
	return int(res)
}

//bool[]
func (mutation *mutationRequest) SetBoolArrayValByColIndex(colIndex int, value []bool) int {
	var dataPtr *C.bool
	if len(value) == 0 {
		dataPtr = (*C.bool)(unsafe.Pointer(&value))
	} else {
		dataPtr = (*C.bool)(unsafe.Pointer(&value[0]))
	}
	res := C.holo_client_set_req_bool_array_val_by_colindex(mutation.cmutation, C.int(colIndex), dataPtr, C.int(len(value)))
	return int(res)
}

//float4[]
func (mutation *mutationRequest) SetFloat32ArrayValByColIndex(colIndex int, value []float32) int {
	var dataPtr *C.float
	if len(value) == 0 {
		dataPtr = (*C.float)(unsafe.Pointer(&value))
	} else {
		dataPtr = (*C.float)(unsafe.Pointer(&value[0]))
	}
	res := C.holo_client_set_req_float_array_val_by_colindex(mutation.cmutation, C.int(colIndex), dataPtr, C.int(len(value)))
	return int(res)
}

//float8[]
func (mutation *mutationRequest) SetFloat64ArrayValByColIndex(colIndex int, value []float64) int {
	var dataPtr *C.double
	if len(value) == 0 {
		dataPtr = (*C.double)(unsafe.Pointer(&value))
	} else {
		dataPtr = (*C.double)(unsafe.Pointer(&value[0]))
	}
	res := C.holo_client_set_req_double_array_val_by_colindex(mutation.cmutation, C.int(colIndex), dataPtr, C.int(len(value)))
	return int(res)
}

func (mutation *mutationRequest) SetNullValByColIndex(colIndex int) int {
	res := C.holo_client_set_req_null_val_by_colindex(mutation.cmutation, C.int(colIndex))
	return int(res)
}

// Set value by Colname
//所有类型
func (mutation *mutationRequest) SetValWithTextByColName(colName string, value string, len int) int {
	cname := C.CString(colName)
	cvalue := C.CString(value)
	res := C.holo_client_set_req_val_with_text_by_colname(mutation.cmutation, cname, cvalue, C.int(len))
	C.free(unsafe.Pointer(cvalue))
	C.free(unsafe.Pointer(cname))
	return int(res)
}

//smallint
func (mutation *mutationRequest) SetInt16ValByColName(colName string, value int16) int {
	cname := C.CString(colName)
	res := C.holo_client_set_req_int16_val_by_colname(mutation.cmutation, cname, C.int16_t(value))
	C.free(unsafe.Pointer(cname))
	return int(res)
}

//int
func (mutation *mutationRequest) SetInt32ValByColName(colName string, value int32) int {
	cname := C.CString(colName)
	res := C.holo_client_set_req_int32_val_by_colname(mutation.cmutation, cname, C.int32_t(value))
	C.free(unsafe.Pointer(cname))
	return int(res)
}

//bigint
func (mutation *mutationRequest) SetInt64ValByColName(colName string, value int64) int {
	cname := C.CString(colName)
	res := C.holo_client_set_req_int64_val_by_colname(mutation.cmutation, cname, C.int64_t(value))
	C.free(unsafe.Pointer(cname))
	return int(res)
}

//bool
func (mutation *mutationRequest) SetBoolValByColName(colName string, value bool) int {
	cname := C.CString(colName)
	res := C.holo_client_set_req_bool_val_by_colname(mutation.cmutation, cname, C.bool(value))
	C.free(unsafe.Pointer(cname))
	return int(res)
}

//real(float4)
func (mutation *mutationRequest) SetFloat32ValByColName(colName string, value float32) int {
	cname := C.CString(colName)
	res := C.holo_client_set_req_float_val_by_colname(mutation.cmutation, cname, C.float(value))
	C.free(unsafe.Pointer(cname))
	return int(res)
}

//float(float8 double precision)
func (mutation *mutationRequest) SetFloat64ValByColName(colName string, value float64) int {
	cname := C.CString(colName)
	res := C.holo_client_set_req_double_val_by_colname(mutation.cmutation, cname, C.double(value))
	C.free(unsafe.Pointer(cname))
	return int(res)
}

//text
func (mutation *mutationRequest) SetTextValByColName(colName string, value string, len int) int {
	cname := C.CString(colName)
	cvalue := C.CString(value)
	res := C.holo_client_set_req_text_val_by_colname(mutation.cmutation, cname, cvalue, C.int(len))
	C.free(unsafe.Pointer(cvalue))
	C.free(unsafe.Pointer(cname))
	return int(res)
}

//timestamp
func (mutation *mutationRequest) SetTimestampValByColName(colName string, value int64) int {
	cname := C.CString(colName)
	res := C.holo_client_set_req_timestamp_val_by_colname(mutation.cmutation, cname, C.int64_t(value))
	C.free(unsafe.Pointer(cname))
	return int(res)
}

//timestamptz
func (mutation *mutationRequest) SetTimestamptzValByColName(colName string, value int64) int {
	cname := C.CString(colName)
	res := C.holo_client_set_req_timestamptz_val_by_colname(mutation.cmutation, cname, C.int64_t(value))
	C.free(unsafe.Pointer(cname))
	return int(res)
}

//int[]
func (mutation *mutationRequest) SetInt32ArrayValByColName(colName string, value []int32) int {
	cname := C.CString(colName)
	var dataPtr *C.int32_t
	if len(value) == 0 {
		dataPtr = (*C.int32_t)(unsafe.Pointer(&value))
	} else {
		dataPtr = (*C.int32_t)(unsafe.Pointer(&value[0]))
	}
	res := C.holo_client_set_req_int32_array_val_by_colname(mutation.cmutation, cname, dataPtr, C.int(len(value)))
	C.free(unsafe.Pointer(cname))
	return int(res)
}

//bigint[]
func (mutation *mutationRequest) SetInt64ArrayValByColName(colName string, value []int64) int {
	cname := C.CString(colName)
	var dataPtr *C.int64_t
	if len(value) == 0 {
		dataPtr = (*C.int64_t)(unsafe.Pointer(&value))
	} else {
		dataPtr = (*C.int64_t)(unsafe.Pointer(&value[0]))
	}
	res := C.holo_client_set_req_int64_array_val_by_colname(mutation.cmutation, cname, dataPtr, C.int(len(value)))
	C.free(unsafe.Pointer(cname))
	return int(res)
}

//bool[]
func (mutation *mutationRequest) SetBoolArrayValByColName(colName string, value []bool) int {
	cname := C.CString(colName)
	var dataPtr *C.bool
	if len(value) == 0 {
		dataPtr = (*C.bool)(unsafe.Pointer(&value))
	} else {
		dataPtr = (*C.bool)(unsafe.Pointer(&value[0]))
	}
	res := C.holo_client_set_req_bool_array_val_by_colname(mutation.cmutation, cname, dataPtr, C.int(len(value)))
	C.free(unsafe.Pointer(cname))
	return int(res)
}

//float4[]
func (mutation *mutationRequest) SetFloat32ArrayValByColName(colName string, value []float32) int {
	cname := C.CString(colName)
	var dataPtr *C.float
	if len(value) == 0 {
		dataPtr = (*C.float)(unsafe.Pointer(&value))
	} else {
		dataPtr = (*C.float)(unsafe.Pointer(&value[0]))
	}
	res := C.holo_client_set_req_float_array_val_by_colname(mutation.cmutation, cname, dataPtr, C.int(len(value)))
	C.free(unsafe.Pointer(cname))
	return int(res)
}

//float8[]
func (mutation *mutationRequest) SetFloat64ArrayValByColName(colName string, value []float64) int {
	cname := C.CString(colName)
	var dataPtr *C.double
	if len(value) == 0 {
		dataPtr = (*C.double)(unsafe.Pointer(&value))
	} else {
		dataPtr = (*C.double)(unsafe.Pointer(&value[0]))
	}
	res := C.holo_client_set_req_double_array_val_by_colname(mutation.cmutation, cname, dataPtr, C.int(len(value)))
	C.free(unsafe.Pointer(cname))
	return int(res)
}

func (mutation *mutationRequest) SetNullValByColName(colName string) int {
	cname := C.CString(colName)
	res := C.holo_client_set_req_null_val_by_colname(mutation.cmutation, cname)
	C.free(unsafe.Pointer(cname))
	return int(res)
}

type GetRequest struct {
	cget C.HoloGet
}

func NewGetRequest(schema *HoloTableSchema) *GetRequest {
	get := new(GetRequest)
	get.cget = C.holo_client_new_get_request(schema.ctableSchema)
	return get
}

//必须以string的形式设置值
func (get *GetRequest) SetGetValByColIndex(colIndex int, value string, len int) int {
	cvalue := C.CString(value)
	res := C.holo_client_set_get_val_with_text_by_colindex(get.cget, C.int(colIndex), cvalue, C.int(len))
	C.free(unsafe.Pointer(cvalue))
	return int(res)
}

func (get *GetRequest) DestroyGet() {
	C.holo_client_destroy_get_request(get.cget)
}
