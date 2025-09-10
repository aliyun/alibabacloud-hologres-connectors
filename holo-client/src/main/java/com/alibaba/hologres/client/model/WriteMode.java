/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model;

/** enum for write mode, please use OnConflictAction instead now. */
@Deprecated
public enum WriteMode {
    INSERT_OR_IGNORE,
    INSERT_OR_UPDATE,
    INSERT_OR_REPLACE;

    public static WriteMode fromOnConflictAction(OnConflictAction onConflictAction) {
        switch (onConflictAction) {
            case INSERT_OR_IGNORE:
                return INSERT_OR_IGNORE;
            case INSERT_OR_UPDATE:
                return INSERT_OR_UPDATE;
            case INSERT_OR_REPLACE:
                return INSERT_OR_REPLACE;
            default:
                throw new IllegalArgumentException(
                        "unsupported onConflictAction " + onConflictAction);
        }
    }
}
