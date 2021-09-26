package holoclient

/*
#cgo CFLAGS: -I./include
#cgo LDFLAGS: -L./lib -lholo-client
#include "holo_config.h"
#include <stdlib.h>
#include <stdbool.h>
*/
import "C"

import "unsafe"

type holoConfig struct {
	cconfig C.HoloConfig
}

type HoloWriteMode C.HoloWriteMode
const (
	INSERT_OR_IGNORE  HoloWriteMode = C.INSERT_OR_IGNORE
	INSERT_OR_UPDATE  HoloWriteMode = C.INSERT_OR_UPDATE
	INSERT_OR_REPLACE HoloWriteMode = C.INSERT_OR_REPLACE
)

func NewHoloConfig(connInfo string) *holoConfig {
	config := new(holoConfig)
	cInfo := C.CString(connInfo)
	config.cconfig = C.holo_client_new_config(cInfo)
	C.free(unsafe.Pointer(cInfo))
	return config
}

func (config *holoConfig) SetThreadSize(threadSize int) {
	config.cconfig.threadSize = C.int(threadSize)
}

func (config *holoConfig) SetBatchSize(batchSize int) {
	config.cconfig.batchSize = C.int(batchSize)
}

func (config *holoConfig) SetShardCollectorSize(shardCollectorSize int) {
	config.cconfig.shardCollectorSize = C.int(shardCollectorSize)
}

func (config *holoConfig) SetWriteMode(mode HoloWriteMode) {
	config.cconfig.writeMode = C.HoloWriteMode(mode)
}

func (config *holoConfig) SetWriteMaxIntervalMs(writeMaxIntervalMs int64) {
	config.cconfig.writeMaxIntervalMs = C.long(writeMaxIntervalMs)
}

func (config *holoConfig) SetWriteBatchByteSize(writeBatchByteSize int64) {
	config.cconfig.writeBatchByteSize = C.long(writeBatchByteSize)
}

func (config *holoConfig) SetWriteBatchTotalByteSize(writeBatchTotalByteSize int64) {
	config.cconfig.writeBatchTotalByteSize = C.long(writeBatchTotalByteSize)
}

func (config *holoConfig) SetRetryCount(retryCount int) {
	config.cconfig.retryCount = C.int(retryCount)
}

func (config *holoConfig) SetRetrySleepStepMs(retrySleepStepMs int64) {
	config.cconfig.retrySleepStepMs = C.longlong(retrySleepStepMs)
}

func (config *holoConfig) SetRetrySleepInitMs(retrySleepInitMs int64) {
	config.cconfig.retrySleepInitMs = C.longlong(retrySleepInitMs)
}

func (config *holoConfig) SetConnectionMaxIdleMs(connectionMaxIdleMs int64) {
	config.cconfig.connectionMaxIdleMs = C.longlong(connectionMaxIdleMs)
}

func (config *holoConfig) SetReadBatchSize(readBatchSize int) {
	config.cconfig.readBatchSize = C.int(readBatchSize)
}

func (config *holoConfig) SetDynamicPartition(dynamicPartition bool) {
	config.cconfig.dynamicPartition = C.bool(dynamicPartition)
}

func (config *holoConfig) SetReportInterval(reportInterval int64) {
	config.cconfig.reportInterval = C.long(reportInterval)
}
