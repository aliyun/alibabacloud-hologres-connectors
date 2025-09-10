/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model;

/** enum for action when on conflict. */
public enum OnConflictAction {
    INSERT_OR_IGNORE,
    INSERT_OR_UPDATE,
    INSERT_OR_REPLACE;

    public static OnConflictAction fromWriteMode(WriteMode writeMode) {
        switch (writeMode) {
            case INSERT_OR_IGNORE:
                return INSERT_OR_IGNORE;
            case INSERT_OR_UPDATE:
                return INSERT_OR_UPDATE;
            case INSERT_OR_REPLACE:
                return INSERT_OR_REPLACE;
            default:
                throw new IllegalArgumentException("unsupported writeMode " + writeMode);
        }
    }
}
