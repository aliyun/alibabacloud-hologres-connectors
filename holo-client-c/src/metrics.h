#ifndef _METRICS_H_
#define _METRICS_H_

#include "utils.h"
#include "logger.h"
#include "pthread.h"

typedef struct _MetricsMeter {
    pthread_rwlock_t* rwlock;
    long long value;
    long long count;
    long startTime;
    long lastUpdateTime;
} MetricsMeter;

MetricsMeter* new_metrics_meter();
void destroy_metrics_meter(MetricsMeter*);
void metrics_meter_mark(MetricsMeter*, long);
void metrics_meter_reset(MetricsMeter*);
void metrics_meter_gather(MetricsMeter*, MetricsMeter*);

typedef struct _MetricsHistogram {
    pthread_rwlock_t* rwlock;
    long long value;
    long long count;
    long startTime;
    long lastUpdateTime;
    long max;
    long min;
} MetricsHistogram;

MetricsHistogram* new_metrics_histogram();
void destroy_metrics_histogram(MetricsHistogram*);
void metrics_histogram_update(MetricsHistogram*, long);
void metrics_histogram_reset(MetricsHistogram*);
void metrics_histogram_gather(MetricsHistogram*, MetricsHistogram*);

typedef struct _MetricsInWorker {
    MetricsMeter* rps;
    MetricsMeter* qps;
    MetricsHistogram* idleTime;
    MetricsHistogram* handleActionTime;
    MetricsHistogram* execPreparedTime;
} MetricsInWorker;

MetricsInWorker* holo_client_new_metrics_in_worker();
void holo_client_destroy_metrics_in_worker(MetricsInWorker*);

typedef struct _Metrics {
    MetricsInWorker** metricsList;
    int nWorkers;
    MetricsHistogram* gatherTime;
    MetricsMeter* timeoutFlush;
    MetricsHistogram* actionSize;
    MetricsHistogram* getFutureTime;
    MetricsHistogram* submitActionTime;
    long startTime;
    long reportInterval;
} Metrics;

Metrics* holo_client_new_metrics(int, long);
void holo_client_destroy_metrics(Metrics*);
void metrics_gather_and_show(Metrics*);
void metrics_try_gather_and_show(Metrics*);

#endif