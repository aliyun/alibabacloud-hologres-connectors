#ifndef _HOLO_CLIENT_H_
#define _HOLO_CLIENT_H_

#include "defs.h"
#include "logger.h"
#include "request.h"
#include "worker_pool.h"
#include "record.h"

__HOLO_CLIENT_BEGIN_DECLS

struct _HoloClient;
typedef struct _HoloClient HoloClient;

HoloClient* holo_client_new_client(HoloConfig);
HoloClient* holo_client_new_client_with_workerpool(HoloConfig, HoloWorkerPool*);

int holo_client_flush_client(HoloClient*);
int holo_client_flush_client_with_errmsg(HoloClient*, char**);
int holo_client_close_client(HoloClient*);

//Schema name可以为NULL，会被设为"public"
HoloTableSchema* holo_client_get_tableschema(HoloClient*, const char*, const char*, bool);
HoloTableSchema* holo_client_get_tableschema_with_errmsg(HoloClient*, const char*, const char*, bool, char**);

int holo_client_submit(HoloClient*, HoloMutation);
int holo_client_submit_with_errmsg(HoloClient*, HoloMutation, char**);
int holo_client_submit_with_attachments(HoloClient*, HoloMutation, int64_t, int64_t);
int holo_client_get(HoloClient*, HoloGet);
HoloRecord* holo_client_get_record(const HoloGet);
char* holo_client_get_record_val(const HoloRecord* record, int colIndex);

//通过接口返回的error code，可以获取相应错误的error message（字符串形式）
const char* holo_client_get_errmsg_with_errcode(int errCode);

__HOLO_CLIENT_END_DECLS

#endif