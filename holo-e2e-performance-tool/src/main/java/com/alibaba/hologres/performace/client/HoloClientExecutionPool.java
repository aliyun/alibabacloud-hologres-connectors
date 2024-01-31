package com.alibaba.hologres.performace.client;


import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.ExecutionPool;


import java.io.Closeable;
import java.io.IOException;


public class HoloClientExecutionPool implements Closeable {

    private ExecutionPool pool = null;
    private ExecutionPool fixedPool = null;
    private String executionPoolName = "e2e-pool";
    private String executionFixedPoolName = "fixed-e2e-pool";
    private boolean useFixedFe = false;

    public HoloClientExecutionPool(HoloConfig poolConfig, int id, boolean isSingleExecutionPool) {
        if (!isSingleExecutionPool) {
            executionPoolName += "_" + id;
            executionFixedPoolName += "_" + id;
        }
        useFixedFe = poolConfig.isUseFixedFe();
        pool = ExecutionPool.buildOrGet(executionPoolName, poolConfig, true, false);
        if (poolConfig.isUseFixedFe()) {
            fixedPool = ExecutionPool.buildOrGet(executionFixedPoolName, poolConfig, true, true);
        }
    }

    public void setHoloClientPool(HoloClient client) throws HoloClientException {
        client.setPool(pool);
        if (useFixedFe) {
            client.setFixedPool(fixedPool);
        }
    }

    @Override
    public void close() {
        if (pool != null) {
            pool.close();
        }
        if (fixedPool != null) {
            fixedPool.close();
        }
    }
}
