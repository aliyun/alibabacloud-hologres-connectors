#ifndef _HOLO_CLIENT_H_
#define _HOLO_CLIENT_H_

#include "defs.h"
#include "request.h"
#include "worker_pool.h"
#include "record.h"

__HOLO_CLIENT_BEGIN_DECLS

struct _HoloClient;
typedef struct _HoloClient HoloClient;

HoloClient* holo_client_new_client(HoloConfig);
HoloClient* holo_client_new_client_with_workerpool(HoloConfig, HoloWorkerPool*);

int holo_client_flush_client(HoloClient*);
int holo_client_close_client(HoloClient*);

//Schema name可以为NULL，会被设为"public"
HoloTableSchema* holo_client_get_tableschema(HoloClient*, const char*, const char*, bool);

int holo_client_submit(HoloClient*, HoloMutation);
int holo_client_get(HoloClient*, HoloGet);
HoloRecord* holo_client_get_record(const HoloGet);
char* holo_client_get_record_val(const HoloRecord* record, int colIndex);

void holo_client_logger_open();
void holo_client_logger_close();

__HOLO_CLIENT_END_DECLS

#endif