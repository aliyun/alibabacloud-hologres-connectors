#include <pthread.h>
#include "logger_log4c.h"
#include "defs.h"

pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;
log4c_category_t *log_category = NULL;

int log4c_open() {
    pthread_mutex_lock(&mtx);
    if (log_category != NULL){
        pthread_mutex_unlock(&mtx);
        return 0;
    }
    if (log4c_init() != 0){
        fprintf(stderr, "Log4c init failed.\n");
        pthread_mutex_unlock(&mtx);
        return -1;
    }
    log_category = log4c_category_get("holo-client");
    pthread_mutex_unlock(&mtx);
    return 0;
}

int log4c_close() {
    pthread_mutex_lock(&mtx);
    if (log_category == NULL){
        fprintf(stderr, "Logger is NULL.\n");
        pthread_mutex_unlock(&mtx);
        return -1;
    }
    if (log4c_fini() != 0){
        fprintf(stderr, "Log4c destruct failed.\n");
        pthread_mutex_unlock(&mtx);
        return -1;
    }
    log_category = NULL;
    pthread_mutex_unlock(&mtx);
    return 0;
}

void* holo_client_log_log4c(const int logLevel, const char* msg) {
    switch (logLevel)
    {
    case HOLO_LOG_LEVEL_DEBUG:
        log4c_category_log(log_category, LOG4C_PRIORITY_DEBUG, "[%ld] %s", (unsigned long)pthread_self(), msg);
        break;
    case HOLO_LOG_LEVEL_INFO:
        log4c_category_log(log_category, LOG4C_PRIORITY_INFO, "[%ld] %s", (unsigned long)pthread_self(), msg);
        break;
    case HOLO_LOG_LEVEL_WARNING:
        log4c_category_log(log_category, LOG4C_PRIORITY_WARN, "[%ld] %s", (unsigned long)pthread_self(), msg);
        break;
    case HOLO_LOG_LEVEL_ERROR:
        log4c_category_log(log_category, LOG4C_PRIORITY_ERROR, "[%ld] %s", (unsigned long)pthread_self(), msg);
        break;
    case HOLO_LOG_LEVEL_FATAL:
        log4c_category_log(log_category, LOG4C_PRIORITY_FATAL, "[%ld] %s", (unsigned long)pthread_self(), msg);
        break;
    case HOLO_LOG_LEVEL_NONE:
        break;
    default:
        log4c_category_log(log_category, LOG4C_PRIORITY_INFO, "[%ld] %s", (unsigned long)pthread_self(), msg);
        break;
    }
    return NULL;
}