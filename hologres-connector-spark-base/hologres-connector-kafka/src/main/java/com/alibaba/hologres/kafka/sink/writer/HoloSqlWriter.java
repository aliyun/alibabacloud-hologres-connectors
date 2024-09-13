package com.alibaba.hologres.kafka.sink.writer;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.kafka.conf.HoloSinkConfigManager;
import com.alibaba.hologres.kafka.exception.KafkaHoloException;

/** Holo sql writer: write use holo client put method(insert query). */
public class HoloSqlWriter extends AbstractHoloWriter {
    HoloClient client;

    public HoloSqlWriter(HoloSinkConfigManager configManager) {
        client = configManager.getClient();
    }

    @Override
    public void write(Put put) throws KafkaHoloException {
        try {
            client.put(put);
        } catch (HoloClientException e) {
            throw new KafkaHoloException(e);
        }
    }

    @Override
    public void flush() {
        try {
            client.flush();
        } catch (HoloClientException e) {
            throw new KafkaHoloException(e);
        }
    }

    @Override
    public void close() throws KafkaHoloException {
        if (client != null) {
            client.close();
        }
    }
}
