/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.exception;

/** enum for exception code. */
public enum ExceptionCode {
    INVALID_Config(1),
    INVALID_REQUEST(2),

    GENERATOR_PARAMS_ERROR(51),

    /* 可重试，非脏数据 */
    CONNECTION_ERROR(100),
    READ_ONLY(103),
    META_NOT_MATCH(201),
    TIMEOUT(250),
    BUSY(251),
    TOO_MANY_CONNECTIONS(106),

    /* 不重试，非脏数据 */
    AUTH_FAIL(101),
    ALREADY_CLOSE(102),
    PERMISSION_DENY(104),
    SYNTAX_ERROR(105),
    TOO_MANY_WAL_SENDERS(107),
    INTERNAL_ERROR(300),
    INTERRUPTED(301),
    NOT_SUPPORTED(302),

    /* 不重试，脏数据 */
    TABLE_NOT_FOUND(200, true),
    CONSTRAINT_VIOLATION(202, true),
    DATA_TYPE_ERROR(203, true),
    DATA_VALUE_ERROR(204, true),

    UNKNOWN_ERROR(500);

    private final int code;

    private final boolean dirtyDataException;

    ExceptionCode(int code) {
        this(code, false);
    }

    ExceptionCode(int code, boolean dirtyDataException) {
        this.code = code;
        this.dirtyDataException = dirtyDataException;
    }

    public int getCode() {
        return this.code;
    }

    public boolean isDirtyDataException() {
        return dirtyDataException;
    }
}
