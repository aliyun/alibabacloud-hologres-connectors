package com.alibaba.hologres.kafka.model;

/** DirtyDataStrategy. */
public enum DirtyDataStrategy {
    // 抛出异常
    EXCEPTION,

    // 跳过脏数据
    SKIP,

    // 有限制的跳过脏数据，需要指定需要跳过的数据的topic、partition、offset
    SKIP_ONCE;
}
