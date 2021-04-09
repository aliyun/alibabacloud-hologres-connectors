package com.alibaba.hologres.spark.exception;

/** SparkHoloException. */
public class SparkHoloException extends Exception {
    private static final long serialVersionUID = 4858210651037826401L;

    public SparkHoloException() {
        super();
    }

    public SparkHoloException(String message) {
        super(message);
    }

    public SparkHoloException(Throwable cause) {
        super(cause);
    }

    public SparkHoloException(String message, Throwable cause) {
        super(message, cause);
    }
}
