package com.alibaba.hologres.performace.client;


import com.alibaba.hologres.client.*;
import com.alibaba.hologres.client.impl.binlog.BinlogOffset;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.binlog.BinlogRecord;
import com.alibaba.hologres.client.utils.ConfLoader;
import com.alibaba.hologres.client.utils.Metrics;
import com.alibaba.hologres.com.codahale.metrics.Histogram;
import com.alibaba.hologres.com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.min;

public class BinlogTest {
  public static final Logger LOG = LoggerFactory.getLogger(PutTest.class);
  public static final String METRICS_BINLOG_PERF_RPS = "binlog_perf_rps";
  public static final String METRICS_BINLOG_PERF_LATENCY = "binlog_perf_latency";

  private String confName;
  private long targetTime;
  private List<List<Integer>> shardListPerThread = new ArrayList<>();
  BinlogTestConf conf = new BinlogTestConf();
  private int shardCount = 0;
  private static long memoryUsage = 0;
  private CyclicBarrier barrier = null;

  public void run(String confName) throws Exception {
    LOG.info("confName:{}", confName);
    this.confName = confName;
    ConfLoader.load(confName, "binlog.", conf);
    HoloConfig clientConf = new HoloConfig();
    ConfLoader.load(confName, "holoClient.", clientConf);

    Reporter reporter = new Reporter(confName);
    try (HoloClient client = new HoloClient(clientConf)) {
      TableSchema schema = client.sql(conn -> {
        if (conf.vacuumTableBeforeRun) {
          SqlUtil.vaccumTable(conn, conf.tableName);
        }
        reporter.start(ConnectionUtil.getHoloVersion(conn));
        return ConnectionUtil.getTableSchema(conn, TableName.valueOf(conf.tableName));
      }).get();
      if (schema == null) {
        throw new Exception("table not found");
      }
      shardCount = Command.getShardCount(client, schema);
    }
    conf.threadSize = min(conf.threadSize, shardCount);
    barrier = new CyclicBarrier(conf.threadSize, ()->{
      memoryUsage = Util.getMemoryStat();
      Util.dumpHeap(confName);
    });
    for(int i = 0; i < shardCount; i++) {
      int index = i % conf.threadSize;
      if (shardListPerThread.size() <= index) {
        shardListPerThread.add(new ArrayList<>());
      }
      shardListPerThread.get(index).add(i);
    }
    targetTime = System.currentTimeMillis() + conf.testTime;

    Thread[] threads = new Thread[conf.threadSize];
    Metrics.startSlf4jReporter(60L, TimeUnit.SECONDS);
    for (int i = 0; i < threads.length; ++i) {
      threads[i] = new Thread(new Job(i));
      threads[i].start();
    }

    for (int i = 0; i < threads.length; ++i) {
      threads[i].join();
    }

    Metrics.reporter().report();
    {
      Meter meter = Metrics.registry().meter(METRICS_BINLOG_PERF_RPS);
      Histogram hist = Metrics.registry().histogram(METRICS_BINLOG_PERF_LATENCY);
      reporter.report(meter.getCount(), meter.getOneMinuteRate(), meter.getFiveMinuteRate(),
          meter.getFifteenMinuteRate(), hist.getSnapshot().getMean(),
          hist.getSnapshot().get99thPercentile(), hist.getSnapshot().get999thPercentile(), memoryUsage);
    }

    if (conf.deleteTableAfterDone) {
      SqlUtil.dropTableByHoloClient(clientConf, conf.tableName);
    }
  }


  class Job implements Runnable {
    int id;

    public Job(int id) {
      this.id = id;
    }

    @Override
    public void run() {
      HoloConfig clientConf = new HoloConfig();
      BinlogShardGroupReader reader = null;
      HoloClient client = null;
      try {
        List<Integer> shardList = shardListPerThread.get(id);
        Map<Integer, Integer> shardIdIndexMapping = new HashMap<>();
        for (int i =0; i < shardList.size(); i++) {
          shardIdIndexMapping.put(shardList.get(i),i);
        }
        BitSet shardHeartBeatReceived = new BitSet(shardList.size());
        ConfLoader.load(confName, "holoClient.", clientConf);

        Meter meter = Metrics.registry().meter(METRICS_BINLOG_PERF_RPS);
        Histogram hist = Metrics.registry().histogram(METRICS_BINLOG_PERF_LATENCY);
        client = new HoloClient(clientConf);
        Subscribe.OffsetBuilder subscribeBuilder = Subscribe.newOffsetBuilder(conf.tableName, conf.slotName);
        for (Integer shardId : shardList) {
          subscribeBuilder.addShardStartOffset(shardId, new BinlogOffset().setTimestamp(conf.binlogStartTime));
        }
        Subscribe subscribe = subscribeBuilder.build();
        int i = 0;
        reader = client.binlogSubscribe(subscribe);
        long startNano = System.nanoTime();
        BinlogRecord r;
        while (true) {
          r = reader.getBinlogRecord();
          if (++i % 1000 == 0 && System.currentTimeMillis() > targetTime) {
            break;
          }
          if (r != null && r.isHeartBeat()) {
            shardHeartBeatReceived.set(shardIdIndexMapping.get(r.getShardId()));
            if (shardHeartBeatReceived.cardinality() == shardList.size()){
              break;
            } else {
              continue;
            }
          }
          meter.mark();
          long endNano = System.nanoTime();
          hist.update((endNano - startNano) / 1000000L);
          startNano = endNano;
        }
      } catch (Exception e) {
        e.printStackTrace();
      }  finally {
        if (reader != null) {
          reader.cancel();
        }
        if (conf.dumpMemoryStat) {
          try {
            barrier.await();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        if (client != null) {
          client.close();
        }
      }
    }
  }
}

class BinlogTestConf {
  public int threadSize = 10;
  public long testTime = 600000;
  public String tableName = "holo_perf";
  public String publicationName = "holo_perf_publication";
  public String slotName = "holo_perf_slot";
  public String binlogStartTime = new Timestamp(System.currentTimeMillis() - 360 * 60 * 1000).toString(); //1 hours ago
  public boolean vacuumTableBeforeRun = true;
  public boolean deleteTableAfterDone = false;
  public boolean dumpMemoryStat = false;
}