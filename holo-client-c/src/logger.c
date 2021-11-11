#include "logger.h"
#include "stdio.h"

log4c_category_t *log_category = NULL;

int logger_open() {
    if (log_category != NULL){
        LOG_WARN("Logger cannot be initialized multiple times. Check if multiple holo clients have been created.");
        return -1;
    }
    if (log4c_init() != 0){
        fprintf(stderr, "Log4c init failed.\n");
        return -1;
    }
    log_category = log4c_category_get("holo-client");
    return 0;
}

int logger_close() {
    if (log_category == NULL){
        fprintf(stderr, "Logger is NULL.\n");
        return -1;
    }
    if (log4c_fini() != 0){
        fprintf(stderr, "Log4c destruct failed.\n");
        return -1;
    }
    return 0;
}