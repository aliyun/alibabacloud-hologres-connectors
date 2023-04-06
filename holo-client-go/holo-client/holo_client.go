package holoclient

/*
#cgo CFLAGS: -I./include
#cgo LDFLAGS: -L./lib -lholo-client
#include "holo_client.h"
#include "holo_config.h"
#include "request.h"
#include <stdlib.h>
#include <stdbool.h>
*/
import "C"

import "unsafe"

type HoloClient struct {
	cclient *C.HoloClient
}

func NewHoloClient(config *holoConfig) *HoloClient {
	client := new(HoloClient)
	client.cclient = C.holo_client_new_client(config.cconfig)
	return client
}

func (client *HoloClient) Flush() {
	C.holo_client_flush_client(client.cclient)
}

func (client *HoloClient) Close() {
	C.holo_client_close_client(client.cclient)
}

//当schemaName为空字符串时认为schema是public
func (client *HoloClient) GetTableschema(schemaName string, tableName string, withCache bool) *HoloTableSchema {
	var cschemaName *C.char
	if schemaName == "" {
		cschemaName = nil
	} else {
		cschemaName = C.CString(schemaName)
	}
	ctableName := C.CString(tableName)
	schema := new(HoloTableSchema)
	schema.ctableSchema = C.holo_client_get_tableschema(client.cclient, cschemaName, ctableName, C.bool(withCache))
	C.free(unsafe.Pointer(cschemaName))
	C.free(unsafe.Pointer(ctableName))
	return schema
}

func (client *HoloClient) Submit(mutation *mutationRequest) int {
	res := C.holo_client_submit(client.cclient, mutation.cmutation)
	return int(res)
}

type record struct {
	crecord *C.HoloRecord
}

func (client *HoloClient) Get(get *GetRequest) *record {
	C.holo_client_get(client.cclient, get.cget)
	return get.getRecord()
}

func (client *HoloClient) GetList(getList []*GetRequest) []*record {
	recordList := make([]*record, len(getList))
	for i := 0; i < len(getList); i++ {
		C.holo_client_get(client.cclient, getList[i].cget)
	}
	for i := 0; i < len(getList); i++ {
		recordList[i] = getList[i].getRecord()
	}
	return recordList
}

func (get *GetRequest) getRecord() *record {
	crecord := C.holo_client_get_record(get.cget)
	if crecord == nil {
		return nil
	}
	r := new(record)
	r.crecord = crecord
	return r
}

func (r *record) GetVal(colIndex int) string {
	return C.GoString(C.holo_client_get_record_val(r.crecord, C.int(colIndex)))
}

func HoloClientLoggerOpen() {
	C.holo_client_logger_open()
}

func HoloClientLoggerClose() {
	C.holo_client_logger_close()
}
