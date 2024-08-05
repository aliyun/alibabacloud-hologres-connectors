#include "metrics.h"

MetricsMeter* new_metrics_meter(){
    MetricsMeter* meter = MALLOC(1, MetricsMeter);
    meter->rwlock = MALLOC(1, pthread_rwlock_t);
    pthread_rwlock_init(meter->rwlock, NULL);
    metrics_meter_reset(meter);
    return meter;
}

void destroy_metrics_meter(MetricsMeter* meter){
    pthread_rwlock_destroy(meter->rwlock);
    FREE(meter->rwlock);
    FREE(meter);
}

void metrics_meter_mark(MetricsMeter* meter, long a){
    pthread_rwlock_wrlock(meter->rwlock);
    if (meter->startTime == -1) meter->startTime = current_time_ms();
    meter->lastUpdateTime = current_time_ms();
    meter->value += a;
    meter->count++;
    pthread_rwlock_unlock(meter->rwlock);
}

void metrics_meter_reset(MetricsMeter* meter){
    meter->startTime = -1;
    meter->lastUpdateTime = -1;
    meter->value = 0;
    meter->count = 0;
}

void metrics_meter_gather(MetricsMeter* dst, MetricsMeter* src){
    pthread_rwlock_wrlock(src->rwlock);
    dst->count += src->count;
    dst->value += src->value;
    metrics_meter_reset(src);
    pthread_rwlock_unlock(src->rwlock);
}

MetricsHistogram* new_metrics_histogram(){
    MetricsHistogram* histogram = MALLOC(1, MetricsHistogram);
    histogram->rwlock = MALLOC(1, pthread_rwlock_t);
    pthread_rwlock_init(histogram->rwlock, NULL);
    metrics_histogram_reset(histogram);
    return histogram;
}

void destroy_metrics_histogram(MetricsHistogram* histogram){
    pthread_rwlock_destroy(histogram->rwlock);
    FREE(histogram->rwlock);
    FREE(histogram);
}

void metrics_histogram_update(MetricsHistogram* histogram, long a){
    pthread_rwlock_wrlock(histogram->rwlock);
    if (histogram->startTime == -1) histogram->startTime = current_time_ms();
    histogram->lastUpdateTime = current_time_ms();
    histogram->value += a;
    histogram->count += 1;
    if (a > histogram->max) histogram->max = a;
    if (a < histogram->min) histogram->min = a;
    pthread_rwlock_unlock(histogram->rwlock);
}

void metrics_histogram_reset(MetricsHistogram* histogram){
    histogram->startTime = -1;
    histogram->lastUpdateTime = -1;
    histogram->min = __LONG_MAX__;
    histogram->max = 0;
    histogram->value = 0;
    histogram->count = 0;
}

void metrics_histogram_gather(MetricsHistogram* dst, MetricsHistogram* src){
    pthread_rwlock_wrlock(src->rwlock);
    dst->count += src->count;
    dst->value += src->value;
    if (src->max > dst->max) dst->max = src->max;
    if (src->min < dst->min) dst->min = src->min;
    metrics_histogram_reset(src);
    pthread_rwlock_unlock(src->rwlock);
}

MetricsInWorker* holo_client_new_metrics_in_worker(){
    MetricsInWorker* metrics = MALLOC(1, MetricsInWorker);
    metrics->rps = new_metrics_meter();
    metrics->qps = new_metrics_meter();
    metrics->execPreparedTime = new_metrics_histogram();
    metrics->handleActionTime = new_metrics_histogram();
    metrics->idleTime = new_metrics_histogram();
    return metrics;
}

void holo_client_destroy_metrics_in_worker(MetricsInWorker* metrics){
    destroy_metrics_meter(metrics->rps);
    destroy_metrics_meter(metrics->qps);
    destroy_metrics_histogram(metrics->execPreparedTime);
    destroy_metrics_histogram(metrics->handleActionTime);
    destroy_metrics_histogram(metrics->idleTime);
    FREE(metrics);
}

Metrics* holo_client_new_metrics(int nWorkers, long reportInterval){
    Metrics* metrics = MALLOC(1, Metrics);
    metrics->nWorkers = nWorkers;
    metrics->metricsList = MALLOC(nWorkers, MetricsInWorker*);
    metrics->gatherTime = new_metrics_histogram();
    metrics->timeoutFlush = new_metrics_meter();
    metrics->actionSize = new_metrics_histogram();
    metrics->getFutureTime = new_metrics_histogram();
    metrics->submitActionTime = new_metrics_histogram();
    metrics->startTime = current_time_ms();
    metrics->reportInterval = reportInterval;
    return metrics;
}

void holo_client_destroy_metrics(Metrics* metrics){
    destroy_metrics_meter(metrics->timeoutFlush);
    destroy_metrics_histogram(metrics->actionSize);
    destroy_metrics_histogram(metrics->gatherTime);
    destroy_metrics_histogram(metrics->getFutureTime);
    destroy_metrics_histogram(metrics->submitActionTime);
    FREE(metrics->metricsList);
    FREE(metrics);
}

void metrics_gather_and_show(Metrics* metrics){
    MetricsInWorker* metricsGather = holo_client_new_metrics_in_worker();
    for (int i = 0;i < metrics->nWorkers;i++){
        MetricsInWorker* metricsInWorker = metrics->metricsList[i];
        metrics_meter_gather(metricsGather->qps, metricsInWorker->qps);
        metrics_meter_gather(metricsGather->rps, metricsInWorker->rps);
        metrics_histogram_gather(metricsGather->execPreparedTime, metricsInWorker->execPreparedTime);
        metrics_histogram_gather(metricsGather->handleActionTime, metricsInWorker->handleActionTime);
        metrics_histogram_gather(metricsGather->idleTime, metricsInWorker->idleTime);
    }
    long endTime = current_time_ms();
    LOG_DEBUG("---------------------- Metrics of last %ldms ----------------------", metrics->reportInterval);
    if (endTime - metrics->startTime != 0)              LOG_DEBUG("           Queries - Tot: %-7lld Cnt: %-7lld QPS: %lld", metricsGather->qps->value, metricsGather->qps->count, metricsGather->qps->value*1000/(endTime - metrics->startTime));
    if (endTime - metrics->startTime != 0)              LOG_DEBUG("           Records - Tot: %-7lld Cnt: %-7lld RPS: %lld", metricsGather->rps->value, metricsGather->rps->count, metricsGather->rps->value*1000/(endTime - metrics->startTime));
    if (metrics->timeoutFlush->count != 0)              LOG_DEBUG("   Timeout Flushes - Tot: %-7lld Cnt: %-7lld Pct: %.1lf%%", metrics->timeoutFlush->value, metrics->timeoutFlush->count, (double)metrics->timeoutFlush->value/metrics->timeoutFlush->count * 100);
    if (metrics->actionSize->count != 0)                LOG_DEBUG("Requests in Action - Cnt: %-7lld Avr: %-7lld Min: %-7ld Max: %ld", metrics->actionSize->count, metrics->actionSize->value/metrics->actionSize->count, metrics->actionSize->min, metrics->actionSize->max);
    if (metricsGather->execPreparedTime->count != 0)    LOG_DEBUG("Exec Prepared Time - Cnt: %-7lld Avr: %-7lld Min: %-7ld Max: %ld", metricsGather->execPreparedTime->count, metricsGather->execPreparedTime->value/metricsGather->execPreparedTime->count, metricsGather->execPreparedTime->min, metricsGather->execPreparedTime->max);
    if (metricsGather->handleActionTime->count != 0)    LOG_DEBUG("Handle Action Time - Cnt: %-7lld Avr: %-7lld Min: %-7ld Max: %ld", metricsGather->handleActionTime->count, metricsGather->handleActionTime->value/metricsGather->handleActionTime->count, metricsGather->handleActionTime->min, metricsGather->handleActionTime->max);
    if (metricsGather->idleTime->count != 0)            LOG_DEBUG("  Worker Idle Time - Cnt: %-7lld Avr: %-7lld Min: %-7ld Max: %ld", metricsGather->idleTime->count, metricsGather->idleTime->value/metricsGather->idleTime->count, metricsGather->idleTime->min, metricsGather->idleTime->max);
    // if (metrics->gatherTime->count != 0)                LOG_DEBUG("       Gather time - Cnt: %lld  Avr: %lld, Min: %ld, Max: %ld", metrics->gatherTime->count, metrics->gatherTime->value/metrics->gatherTime->count, metrics->gatherTime->min, metrics->gatherTime->max);
    if (metrics->getFutureTime->count != 0)             LOG_DEBUG("   Get Future Time - Cnt: %-7lld Avr: %-7lld Min: %-7ld Max: %ld", metrics->getFutureTime->count, metrics->getFutureTime->value/metrics->getFutureTime->count, metrics->getFutureTime->min, metrics->getFutureTime->max);
    if (metrics->submitActionTime->count != 0)          LOG_DEBUG("Submit Action Time - Cnt: %-7lld Avr: %-7lld Min: %-7ld Max: %ld", metrics->submitActionTime->count, metrics->submitActionTime->value/metrics->submitActionTime->count, metrics->submitActionTime->min, metrics->submitActionTime->max);
    metrics_histogram_reset(metrics->gatherTime);
    metrics_meter_reset(metrics->timeoutFlush);
    metrics_histogram_reset(metrics->actionSize);
    metrics_histogram_reset(metrics->getFutureTime);
    metrics_histogram_reset(metrics->submitActionTime);
    holo_client_destroy_metrics_in_worker(metricsGather);
    metrics->startTime = endTime;
}

void metrics_try_gather_and_show(Metrics* metrics){
    if (current_time_ms() - metrics->startTime >= metrics->reportInterval) metrics_gather_and_show(metrics);
}