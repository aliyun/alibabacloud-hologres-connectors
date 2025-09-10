/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.exception;

/** TableName不合法时的异常. */
public class InvalidIdentifierException extends RuntimeException {
    public InvalidIdentifierException() {}

    public InvalidIdentifierException(String message) {
        super(message);
    }

    public InvalidIdentifierException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidIdentifierException(Throwable cause) {
        super(cause);
    }
}
