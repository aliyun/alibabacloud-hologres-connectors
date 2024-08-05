#ifndef _FUTURE_H_
#define _FUTURE_H_

#include <pthread.h>
#include <stdbool.h>

typedef struct _Future {
    bool completed;
    void* retVal;
    char* errMsg;
    pthread_mutex_t* mutex;
    pthread_cond_t* cond;
} Future;

Future* create_future();
void destroy_future(Future*);
void complete_future(Future*, void*);
void* get_future_result(Future*);

#endif