#ifndef _HOLO_CONFIG_H_
#define _HOLO_CONFIG_H_

#include <stdbool.h>
#include "defs.h"
#include "record.h"

__HOLO_CLIENT_BEGIN_DECLS

typedef enum _HoloWriteMode {
    INSERT_OR_IGNORE,
	INSERT_OR_UPDATE,
	INSERT_OR_REPLACE
} HoloWriteMode;

typedef void* (*HoloExceptionHandler)(const HoloRecord*, const char*, void*);

typedef struct _HoloConfig {
    char *connInfo; //e.g. "host=xxxxxx port=xxxx dbname=xxx user=xxxxx password=xxxxx"
    int threadSize; //1
    int batchSize; //512
    int shardCollectorSize; //2 * threadSize;
    HoloWriteMode writeMode; //INSERT_OR_REPLACE
    long writeMaxIntervalMs; //10000
    long writeBatchByteSize; //2 * 1024 * 1024
    long writeBatchTotalByteSize; //writeBatchByteSize * shardCollectorSize
    
    int retryCount; //3
    long long retrySleepStepMs; //10000
    long long retrySleepInitMs; //1000
    bool failFastWhenInit; //true
    long long connectionMaxIdleMs; //60000

    int readBatchSize; //128
    bool dynamicPartition; //false
    bool useFixedFe; //false
    int connectionSizeWhenUseFixedFe; //1
    bool autoFlush; //true
    bool unnestMode; //false

    /* 
     * HoloExceptionHandler callback, and pointer which will be passed to the actual callback as parameter
     * you can make your own implement of exceptionHandler, to handle failed record and error message
     * must not throw C++ exception in the callback!
     * and make sure the Param pointer be safely used!
     */
    HoloExceptionHandler exceptionHandler; //handle_exception_by_doing_nothing
    void *exceptionHandlerParam; //NULL

    long reportInterval; //20000

} HoloConfig;

HoloConfig holo_client_new_config(const char*);
void* handle_exception_by_doing_nothing(const HoloRecord* record, const char* errMsg, void* exceptionHandlerParam);

__HOLO_CLIENT_END_DECLS

#endif