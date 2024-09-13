package com.alibaba.hologres.kafka.sink.writer;

import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.kafka.exception.KafkaHoloException;

/** Abstract Holo Writer. */
public abstract class AbstractHoloWriter {
    public abstract void write(Put put) throws KafkaHoloException;

    public abstract void flush() throws KafkaHoloException;

    public abstract void close() throws KafkaHoloException;
}
