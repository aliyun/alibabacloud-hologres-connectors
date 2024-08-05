#ifndef _LOGGER_PRIVATE_H_
#define _LOGGER_PRIVATE_H_
 
#include "defs.h"
#include "logger.h"
#include "logger_log4c.h"

extern HoloLogger holo_client_logger;
extern int holo_client_logger_level;

#define LOG_ERROR(msg, args...) \
    do {\
        if (holo_client_logger != holo_client_log_do_nothing && holo_client_logger_level <= HOLO_LOG_LEVEL_ERROR) {\
            char buff[HOLO_MAX_LOG_LEN];\
            snprintf(buff, HOLO_MAX_LOG_LEN, msg, ##args);\
            holo_client_logger(HOLO_LOG_LEVEL_ERROR, buff);\
        }\
    } while (0)

#define LOG_WARN(msg, args...) \
    do {\
        if (holo_client_logger != holo_client_log_do_nothing && holo_client_logger_level <= HOLO_LOG_LEVEL_WARNING) {\
            char buff[HOLO_MAX_LOG_LEN];\
            snprintf(buff, HOLO_MAX_LOG_LEN, msg, ##args);\
            holo_client_logger(HOLO_LOG_LEVEL_WARNING, buff);\
        }\
    } while (0)

#define LOG_FATAL(msg, args...) \
    do {\
        if (holo_client_logger != holo_client_log_do_nothing && holo_client_logger_level <= HOLO_LOG_LEVEL_FATAL) {\
            char buff[HOLO_MAX_LOG_LEN];\
            snprintf(buff, HOLO_MAX_LOG_LEN, msg, ##args);\
            holo_client_logger(HOLO_LOG_LEVEL_FATAL, buff);\
        }\
    } while (0)

#define LOG_INFO(msg, args...) \
    do {\
        if (holo_client_logger != holo_client_log_do_nothing && holo_client_logger_level <= HOLO_LOG_LEVEL_INFO) {\
            char buff[HOLO_MAX_LOG_LEN];\
            snprintf(buff, HOLO_MAX_LOG_LEN, msg, ##args);\
            holo_client_logger(HOLO_LOG_LEVEL_INFO, buff);\
        }\
    } while (0)

#define LOG_DEBUG(msg, args...) \
    do {\
        if (holo_client_logger != holo_client_log_do_nothing && holo_client_logger_level <= HOLO_LOG_LEVEL_DEBUG) {\
            char buff[HOLO_MAX_LOG_LEN];\
            snprintf(buff, HOLO_MAX_LOG_LEN, msg, ##args);\
            holo_client_logger(HOLO_LOG_LEVEL_DEBUG, buff);\
        }\
    } while (0)

#endif