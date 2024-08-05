#ifndef _LOGGER_H_
#define _LOGGER_H_

#include "defs.h"

__HOLO_CLIENT_BEGIN_DECLS

/**
 * Definition of HoloLogger callback, the default logger is log4c
 * you can make your own implement of HoloLogger, and use holo_client_setup_logger to setup
 * the first arg is log level, the second is message that will be logged
 * if you don't want any log, set logger to holo_client_log_do_nothing, which will reduce overhead
 */
typedef void* (*HoloLogger)(const int, const char*);

void* holo_client_log_do_nothing(const int logLevel, const char* msg);

void holo_client_setup_logger(HoloLogger logger, int loglevel);

void holo_client_logger_open();
void holo_client_logger_close();

__HOLO_CLIENT_END_DECLS

#endif