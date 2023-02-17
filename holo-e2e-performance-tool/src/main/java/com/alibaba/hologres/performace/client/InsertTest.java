package com.alibaba.hologres.performace.client;


import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.RecordOutputFormat;
import com.alibaba.hologres.client.impl.ConnectionHolder;
import com.alibaba.hologres.client.impl.ExecutionPool;
import com.alibaba.hologres.client.model.ImportContext;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.ConfLoader;
import com.alibaba.hologres.client.utils.Metrics;
import com.alibaba.hologres.com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class InsertTest extends PutTest {
  public static final Logger LOG = LoggerFactory.getLogger(InsertTest.class);

  private AtomicLong tic = new AtomicLong(0);
  private AtomicInteger singleExecutionPoolJobSize;
  private InsertTestConf insertTestConf = new InsertTestConf();

  @Override
  Runnable buildJob(int id) {
    return new InsertJob(id);
  }

  @Override
  void init() throws Exception {
    ConfLoader.load(confName, "insert.", insertTestConf);
    if (insertTestConf.singleExecutionPool) {
      singleExecutionPoolJobSize = new AtomicInteger(conf.threadSize);
    } else {
      singleExecutionPoolJobSize = new AtomicInteger(0);
    }
  }

  class InsertJob implements Runnable {
    final int id;

    public InsertJob(int id) {
      this.id = id;
    }

    @Override
    public void run() {
      HoloConfig poolConf = new HoloConfig();
      HoloConfig clientConf = new HoloConfig();
      try {
        ConfLoader.load(confName, "holoClient.", poolConf);
        ConfLoader.load(confName, "holoClient.", clientConf);
        ConfLoader.load(confName, "pool.", poolConf);

        String executionPoolName = "hello";
        if (!insertTestConf.singleExecutionPool) {
          executionPoolName += "_" + id;
        }
        Meter meter = Metrics.registry().meter(Metrics.METRICS_WRITE_RPS);
        ExecutionPool pool = ExecutionPool.buildOrGet(executionPoolName, poolConf, true, poolConf.isUseFixedFe());
        try (HoloClient client = new HoloClient(clientConf)) {
          Random rand = new Random();
          if (poolConf.isUseFixedFe()) {
            client.setFixedPool(pool);
          } else {
            client.setPool(pool);
          }
          TableSchema schema = client.getTableSchema(conf.tableName);
          int i = 0;
          List<String> writeColumns = Util.getWriteColumnsName(conf, schema);
          while (true) {
            long pk = tic.incrementAndGet();
            ++i;
            if(conf.testByTime) {
              if (i % 1000 == 0) {
                if (System.currentTimeMillis() > targetTime) {
                  LOG.info("test time reached");
                  totalCount.addAndGet(i-1);
                  break;
                }
              }
            } else {
              if (pk > conf.rowNumber) {
                LOG.info("insert write : {}", i - 1);
                totalCount.addAndGet(i-1);
                break;
              }
            }
            Put put = newPut(pk, schema, rand, writeColumns);
            client.put(put);
          }
          client.flush();
        } finally {
          if (insertTestConf.singleExecutionPool && singleExecutionPoolJobSize.decrementAndGet() == 0 || (!insertTestConf.singleExecutionPool)) {
            pool.close();
          }
        }

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }


  private Put newPut(long id, TableSchema schema, Random random, List<String> writeColumns) {
    Put put = new Put(schema);
    fillRecord(put.getRecord(), id, schema, random, writeColumns);
    return put;
  }
}

class InsertTestConf {
  public boolean singleExecutionPool = true;
}
