#ifndef _HOLO_CONFIG_H_
#define _HOLO_CONFIG_H_

#include <stdbool.h>
#include "record.h"

typedef enum _HoloWriteMode {
    INSERT_OR_IGNORE,
	INSERT_OR_UPDATE,
	INSERT_OR_REPLACE
} HoloWriteMode;

typedef void* (*ExceptionHandler)(Record*, char*);

typedef struct _HoloConfig {
    char *connInfo; //e.g. "host=xxxxxx port=xxxx dbname=xxx user=xxxxx password=xxxxx"
    int threadSize;
    int batchSize;
    int shardCollectorSize;
    HoloWriteMode writeMode;
    long writeMaxIntervalMs;
    long writeBatchByteSize;
    long writeBatchTotalByteSize;
    
    int retryCount; //3
    long long retrySleepStepMs; //10000
    long long retrySleepInitMs; //1000
    bool failFastWhenInit; //true
    long long connectionMaxIdleMs; //60000

    int readBatchSize; //128
    bool dynamicPartition;

    ExceptionHandler exceptionHandler;

    long reportInterval;

} HoloConfig;

HoloConfig holo_client_new_config(char*);
void* handle_exception_by_doing_nothing(Record*, char*);

#endif