#include "future.h"
#include "utils.h"

Future* create_future() {
    Future* future = MALLOC(1, Future);
    future->completed = false;
    future->retVal = NULL;
    future->mutex = MALLOC(1, pthread_mutex_t);
    future->cond = MALLOC(1, pthread_cond_t);
    pthread_mutex_init(future->mutex, NULL);
    pthread_cond_init(future->cond, NULL); 
    return future; 
}
void destroy_future(Future* future) {
    pthread_mutex_destroy(future->mutex);
    pthread_cond_destroy(future->cond);
    FREE(future->mutex);
    FREE(future->cond);
    FREE(future);
    future = NULL;
}
void complete_future(Future* future, void* value) {
    pthread_mutex_lock(future->mutex);
    future->retVal = value;
    future->completed = true;
    pthread_cond_signal(future->cond);
    pthread_mutex_unlock(future->mutex);
}
void* get_future_result(Future* future) {
    pthread_mutex_lock(future->mutex);
    while(!future->completed) {
        pthread_cond_wait(future->cond, future->mutex);
    }
    pthread_mutex_unlock(future->mutex);
    return future->retVal;
}