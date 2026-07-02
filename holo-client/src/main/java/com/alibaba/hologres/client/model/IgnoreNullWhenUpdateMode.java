/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model;

/** 当Record中有null列时，在冲突更新场景下如何处理null值. 仅在onConflictAction为INSERT_OR_UPDATE时生效. */
public enum IgnoreNullWhenUpdateMode {
    /** 关闭: 不做特殊处理，null值正常写入覆盖旧值. */
    DISABLED,
    /** 不写此列: 将null列从写入列中移除，不参与更新. */
    SKIP_NULL_COLUMN,
    /** 通过表达式: 使用coalesce表达式实现null值不覆盖旧值，需要Hologres 4.0及以上版本. */
    USE_EXPRESSION
}
