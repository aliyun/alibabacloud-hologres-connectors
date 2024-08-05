#include "logger_private.h"
#include "stdio.h"

HoloLogger holo_client_logger = holo_client_log_log4c;
int holo_client_logger_level = HOLO_LOG_LEVEL_INFO;

void* holo_client_log_do_nothing(const int logLevel, const char* msg) {
    return NULL;
}

void holo_client_setup_logger(HoloLogger logger, int loglevel) {
    holo_client_logger = logger;
    holo_client_logger_level = loglevel;
}

void holo_client_logger_open() {
    if (holo_client_logger == holo_client_log_log4c) {
        log4c_open();
    }
}

void holo_client_logger_close() {
    if (holo_client_logger == holo_client_log_log4c) {
        log4c_close();
    }
}