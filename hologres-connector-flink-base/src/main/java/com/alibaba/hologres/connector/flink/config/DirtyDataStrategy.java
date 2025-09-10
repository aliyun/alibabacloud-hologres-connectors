package com.alibaba.hologres.connector.flink.config;

/** DirtyDataStrategy. */
public enum DirtyDataStrategy {
    // 抛出异常
    EXCEPTION,

    // 跳过脏数据
    SKIP,
}
