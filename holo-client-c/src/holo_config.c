#include "holo_config.h"
#include "holo_config_private.h"
#include "logger.h"
#include "stddef.h"
#include "string.h"
#include "utils.h"

void* handle_exception_by_doing_nothing(Record* record, char* errorMsg){
    return NULL;
}

HoloConfig holo_client_new_config(char* connInfo){
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
    config.reportInterval = 20000;
    config.readBatchSize = 128;
    config.dynamicPartition = false;
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
        LOG_WARN("Holo Config - Connect timeout not set in connection info. Set to 2s.");
    }
    if (config->threadSize <= 0) {
        LOG_ERROR("Holo Config - Thread size <= 0. Use holo_client_new_config to create config.");
        rc = false;
    }
    if (config->batchSize <= 0) {
        LOG_ERROR("Holo Config - Batch size <= 0. Use holo_client_new_config to create config.");
        rc = false;
    }
    if (config->shardCollectorSize == -1) {
        config->shardCollectorSize = 2 * config->threadSize;
        LOG_WARN("Holo Config - Shard collector size not set. Set to twice the thread size.");
    }
    if (config->shardCollectorSize <= 0) {
        LOG_ERROR("Holo Config - Shard collector size <= 0. Use holo_client_new_config to create config.");
        rc = false;
    }
    if (config->writeMaxIntervalMs <= 0) {
        LOG_ERROR("Holo Config - Write max interval time <= 0. Use holo_client_new_config to create config.");
        rc = false;
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
        LOG_WARN("Holo Config - Write batch total byte size not set. Set to the product of shard collector size and write batch byte size.");
    }
    if (config->writeBatchTotalByteSize <= 0) {
        LOG_ERROR("Holo Config - Write batch total byte size <= 0. Use holo_client_new_config to create config.");
        rc = false;
    }
    return rc;
}

void log_holo_config(HoloConfig* config){
    LOG_INFO("Holo Config - Connnection Info: %s", config->connInfo);
    LOG_INFO("Holo Config - Thread Size: %d", config->threadSize);
    LOG_INFO("Holo Config - Batch Size: %d", config->batchSize);
    LOG_INFO("Holo Config - Batch Byte Size: %ld", config->writeBatchByteSize);
    LOG_INFO("Holo Config - Total Byte Size: %ld", config->writeBatchTotalByteSize);
    LOG_INFO("Holo Config - Shard Collector Size: %d", config->shardCollectorSize);
    LOG_INFO("Holo Config - Write Max Interval: %ldms", config->writeMaxIntervalMs);
    LOG_INFO("Holo Config - Max Retry Count: %d", config->retryCount);
    LOG_INFO("Holo Config - Retry Sleep Initial Interval: %lldms", config->retrySleepInitMs);
    LOG_INFO("Holo Config - Retry Sleep Step Interval: %lldms", config->retrySleepStepMs);
    LOG_INFO("Holo Config - Connection Max Idle Time: %lldms", config->connectionMaxIdleMs);
    LOG_INFO("Holo Config - Exception Handler: Already Set");
    LOG_INFO("Holo Config - Metrics Report Interval: %ldms", config->reportInterval);
}