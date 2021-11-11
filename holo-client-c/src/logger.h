#ifndef _LOGGER_H_
#define _LOGGER_H_
 
#include "log4c.h"

extern log4c_category_t *log_category;

int logger_open();
int logger_close();
 
#define LOG_ERROR(msg, args...) \
    log4c_category_log(log_category, LOG4C_PRIORITY_ERROR, "\e[1;31m"msg"\e[0m", ##args)

#define LOG_WARN(msg, args...) \
    log4c_category_log(log_category, LOG4C_PRIORITY_WARN, "\e[0;33m"msg"\e[0m", ##args)

#define LOG_INFO(msg, args...) \
    log4c_category_log(log_category, LOG4C_PRIORITY_INFO, msg, ##args)

#define LOG_DEBUG(msg, args...) \
    log4c_category_log(log_category, LOG4C_PRIORITY_DEBUG, msg, ##args)

#endif