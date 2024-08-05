package com.alibaba.hologres.kafka.exception;

/** KafkaHoloException. */
public class KafkaHoloException extends RuntimeException {

    private static final long serialVersionUID = 4858210651037826401L;

    public KafkaHoloException() {
        super();
    }

    public KafkaHoloException(String message) {
        super(message);
    }

    public KafkaHoloException(Throwable cause) {
        super(cause);
    }

    public KafkaHoloException(String message, Throwable cause) {
        super(message, cause);
    }
}
