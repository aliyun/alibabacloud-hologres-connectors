#include "holo_config.h"
#include "holo_config_private.h"
#include "logger_private.h"
#include "stddef.h"
#include "string.h"
#include "utils.h"

void* handle_exception_by_doing_nothing(const HoloRecord* record, const char* errMsg, void* exceptionHandlerParam){
    return NULL;
}

HoloConfig holo_client_new_config(const char* connInfo){
    HoloConfig config;
    config.connInfo = deep_copy_string(connInfo);
    config.threadSize = 1;
    config.batchSize = 512;
    config.shardCollectorSize = -1;
    config.writeMode = INSERT_OR_REPLACE;
    config.writeMaxIntervalMs = 10000;
    config.retryCount = 3;
    config.retrySleepStepMs = 10000;
    config.retrySleepInitMs = 1000;
    config.failFastWhenInit = true;
    config.connectionMaxIdleMs = 60000;
    config.exceptionHandler = handle_exception_by_doing_nothing;
    config.exceptionHandlerParam = NULL;
    config.reportInterval = 20000;
    config.readBatchSize = 128;
    config.dynamicPartition = false;
    config.useFixedFe = false;
    config.unnestMode = false;
    config.autoFlush = true;
    config.connectionSizeWhenUseFixedFe = 1;
    config.writeBatchByteSize = 2 * 1024 * 1024;
    config.writeBatchTotalByteSize = -1;
    return config;
}

bool holo_config_is_valid(HoloConfig* config){
    bool rc = true;
    if (config->connInfo == NULL) {
        LOG_ERROR("Holo Config - Connection info not set.");
        rc = false;
    }
    else if (strstr(config->connInfo, "connect_timeout=") == NULL){
        int len = strlen(config->connInfo);
        char* newConnInfo = MALLOC(len + 19, char);
        strncpy(newConnInfo, config->connInfo, len);
        strncpy(newConnInfo + len, " connect_timeout=2", 19);
        FREE(config->connInfo);
        config->connInfo = newConnInfo;
        LOG_DEBUG("Holo Config - Connect timeout not set in connection info. Set to 2s.");
    }
    if (strstr(config->connInfo, "application_name=") == NULL){
        int len = strlen(config->connInfo);
        int lenVer = strlen(BUILD_VERSION);
        char* newConnInfo = MALLOC(len + lenVer + 33, char);
        strncpy(newConnInfo, config->connInfo, len);
        strncpy(newConnInfo + len, " application_name=holo-client-c_", 33);
        strncpy(newConnInfo + len + 32, BUILD_VERSION, lenVer + 1);
        FREE(config->connInfo);
        config->connInfo = newConnInfo;
        LOG_DEBUG("Holo Config - Application_name not set in connection info. Set to holo-client-c");
    }
    if (config->threadSize <= 0 || config->threadSize > HOLO_CLIENT_MAX_THREAD_SIZE) {
        LOG_ERROR("Holo Config - Thread size <= 0 or Thread size > HOLO_CLIENT_MAX_THREAD_SIZE. Use holo_client_new_config to create config.");
        rc = false;
    }
    if (config->batchSize <= 0) {
        LOG_ERROR("Holo Config - Batch size <= 0. Use holo_client_new_config to create config.");
        rc = false;
    }
    if (config->shardCollectorSize == -1) {
        config->shardCollectorSize = 2 * config->threadSize;
        LOG_DEBUG("Holo Config - Shard collector size not set. Set to twice the thread size.");
    }
    if (config->shardCollectorSize <= 0) {
        LOG_ERROR("Holo Config - Shard collector size <= 0. Use holo_client_new_config to create config.");
        rc = false;
    }
    if (config->writeMaxIntervalMs <= 0) {
        LOG_WARN("Holo Config - Write max interval time <= 0. Will not check writeMaxIntervalMs.");
    }
    if (config->retryCount <= 0) {
        LOG_ERROR("Holo Config - Retry count <= 0. Use holo_client_new_config to create config.");
        rc = false;
    }
    if (config->retrySleepInitMs <= 0) {
        LOG_ERROR("Holo Config - Retry sleep initial time <= 0. Use holo_client_new_config to create config.");
        rc = false;
    }
    if (config->retrySleepStepMs <= 0) {
        LOG_ERROR("Holo Config - Retry sleep step time <= 0. Use holo_client_new_config to create config.");
        rc = false;
    }
    if (config->connectionMaxIdleMs <= 0) {
        LOG_ERROR("Holo Config - Connection max idle time <= 0. Use holo_client_new_config to create config.");
        rc = false;
    }
    if (config->exceptionHandler == NULL) {
        LOG_ERROR("Holo Config - Exception handler not set. Use holo_client_new_config to create config.");
        rc = false;
    }
    if (config->reportInterval <= 0) {
        LOG_ERROR("Holo Config - Report interval <= 0. Use holo_client_new_config to create config.");
        rc = false;
    }
    if (config->writeBatchByteSize <= 0) {
        LOG_ERROR("Holo Config - Write batch byte size <= 0. Use holo_client_new_config to create config.");
        rc = false;
    }
    if (config->writeBatchTotalByteSize == -1) {
        config->writeBatchTotalByteSize = config->shardCollectorSize * config->writeBatchByteSize;
        LOG_DEBUG("Holo Config - Write batch total byte size not set. Set to the product of shard collector size and write batch byte size.");
    }
    if (config->writeBatchTotalByteSize <= 0) {
        LOG_ERROR("Holo Config - Write batch total byte size <= 0. Use holo_client_new_config to create config.");
        rc = false;
    }
    return rc;
}

void log_holo_config(HoloConfig* config){
    LOG_INFO("Holo Config - Connnection Info: %s,\
Thread Size: %d,\
Batch Size: %d,\
Batch Byte Size: %ld,\
Total Byte Size: %ld,\
Shard Collector Size: %d,\
Write Max Interval: %ldms,\
Max Retry Count: %d,\
Retry Sleep Initial Interval: %lldms,\
Retry Sleep Step Interval: %lldms,\
Connection Max Idle Time: %lldms,\
Metrics Report Interval: %ldms", config->connInfo, config->threadSize, config->batchSize, config->writeBatchByteSize, config->writeBatchTotalByteSize, config->shardCollectorSize, config->writeMaxIntervalMs, config->retryCount, config->retrySleepInitMs, config->retrySleepStepMs, config->connectionMaxIdleMs, config->reportInterval);
}