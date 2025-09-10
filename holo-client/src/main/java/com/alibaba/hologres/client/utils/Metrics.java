package com.alibaba.hologres.client.utils;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.jvm.CachedThreadStatesGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** class for Metrics. */
public class Metrics {
    public static final String METRICS_GC = "gc";
    public static final String METRICS_THREADS = "threads";
    public static final String METRICS_MEMORY = "memory";

    public static final String METRICS_BATCH_MEMORY = "batch_memory";

    public static final String METRICS_WRITE_QPS = "write_qps";
    public static final String METRICS_WRITE_BPS = "write_bps";
    public static final String METRICS_WRITE_RPS = "write_rps";
    public static final String METRICS_WRITE_SQL_PER_BATCH = "write_sql_per_batch";
    public static final String METRICS_WRITE_LATENCY = "write_latency";

    public static final String METRICS_WRITE_COST_MS_ALL = "write_cost_ms";
    public static final String METRICS_SCAN_COST_MS_ALL = "scan_cost_ms";
    public static final String METRICS_GET_COST_MS_ALL = "get_cost_ms";
    public static final String METRICS_SQL_COST_MS_ALL = "sql_cost_ms";
    public static final String METRICS_META_COST_MS_ALL = "meta_cost_ms";
    public static final String METRICS_COPY_COST_MS_ALL = "copy_cost_ms";
    public static final String METRICS_ALL_COST_MS_ALL = "all_cost_ms";

    public static final String METRICS_SCAN_QPS = "scan_qps";
    public static final String METRICS_SCAN_LATENCY = "scan_latency";

    public static final String METRICS_DIMLOOKUP_QPS = "dimlookup_qps_";
    public static final String METRICS_DIMLOOKUP_RPS = "dimlookup_rps_";
    public static final String METRICS_DIMLOOKUP_LATENCY = "dimlookup_latency_";
    public static final String METRICS_DIMLOOKUP_RPS_ALL = "dimlookup_all_rps";

    private static final Logger log = LoggerFactory.getLogger(Metrics.class);
    private static final MetricRegistry registry;
    private static final Slf4jReporter reporter;

    static {
        registry = new MetricRegistry();
        registry.register(METRICS_GC, new GarbageCollectorMetricSet());
        registry.register(METRICS_THREADS, new CachedThreadStatesGaugeSet(60, TimeUnit.SECONDS));
        registry.register(METRICS_MEMORY, new MemoryUsageGaugeSet());

        // Register reporters here
        reporter = Slf4jReporter.forRegistry(registry).build();
    }

    public static MetricRegistry registry() {
        return registry;
    }

    public static Slf4jReporter reporter() {
        return reporter;
    }

    static AtomicBoolean closed = new AtomicBoolean(false);

    public static synchronized void closeReporter() {
        if (closed.compareAndSet(true, false)) {
            reporter().close();
        }
    }

    public static synchronized void startSlf4jReporter(long period, TimeUnit unit) {
        if (closed.compareAndSet(false, true)) {
            reporter().start(period, unit);
        }
    }
}
