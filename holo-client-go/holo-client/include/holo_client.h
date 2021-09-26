#ifndef _HOLO_CLIENT_H_
#define _HOLO_CLIENT_H_

#include "request.h"
#include "worker_pool.h"
#include "record.h"

struct _HoloClient;
typedef struct _HoloClient HoloClient;

HoloClient* holo_client_new_client(HoloConfig);
HoloClient* holo_client_new_client_with_workerpool(HoloConfig, WorkerPool*);

int holo_client_flush_client(HoloClient*);
int holo_client_close_client(HoloClient*);

//Schema name可以为NULL，会被设为"public"
TableSchema* holo_client_get_tableschema(HoloClient*, const char*, const char*, bool);

int holo_client_submit(HoloClient*, Mutation);
int holo_client_get(HoloClient*, Get);
Record* holo_client_get_record(Get);
char* holo_client_get_record_val(Record* record, int colIndex);

void holo_client_logger_open();
void holo_client_logger_close();

#endif