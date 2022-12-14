package com.alibaba.hologres.hive;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.hive.conf.HoloClientParam;

/** HoloClient factory which supports create holo client. */
public class HoloClientProvider {

    private final HoloClientParam param;
    private HoloClient client;

    public HoloClientProvider(HoloClientParam param) {
        this.param = param;
    }

    public void closeClient() {
        if (client == null) {
            return;
        }
        try {
            client.flush();
        } catch (HoloClientException e) {
            throw new RuntimeException("Failed to close client", e);
        } finally {
            client.close();
        }
        client = null;
    }

    public HoloClient createOrGetClient() {
        if (client == null) {
            HoloConfig holoConfig = param.generateHoloConfig();

            try {
                client = new HoloClient(holoConfig);
            } catch (HoloClientException e) {
                throw new RuntimeException("create holo client failed", e);
            }
        }
        return client;
    }

    public TableSchema getTableSchema() {
        if (client == null) {
            createOrGetClient();
        }
        try {
            return client.getTableSchema(TableName.valueOf(param.getTableName()));
        } catch (HoloClientException e) {
            throw new RuntimeException(
                    String.format("get table %s schema failed", param.getTableName()), e);
        }
    }
}
