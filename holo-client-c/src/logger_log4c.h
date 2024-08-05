#ifndef _LOGGER_LOG4C_H_
#define _LOGGER_LOG4C_H_

#include "log4c.h"

extern log4c_category_t *log_category;

int log4c_open();
int log4c_close();

void* holo_client_log_log4c(const int logLevel, const char* msg);

#endif