package com.alibaba.hologres.hive.exception;

/** HiveHoloStorageException. */
public class HiveHoloStorageException extends Exception {

    private static final long serialVersionUID = 4858210651037826401L;

    public HiveHoloStorageException() {
        super();
    }

    public HiveHoloStorageException(String message) {
        super(message);
    }

    public HiveHoloStorageException(Throwable cause) {
        super(cause);
    }

    public HiveHoloStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
