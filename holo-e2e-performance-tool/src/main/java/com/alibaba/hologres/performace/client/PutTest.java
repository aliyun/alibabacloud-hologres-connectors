package com.alibaba.hologres.performace.client;


import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.ConfLoader;
import com.alibaba.hologres.client.utils.Metrics;
import com.alibaba.hologres.com.codahale.metrics.Histogram;
import com.alibaba.hologres.com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Statement;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public abstract class PutTest {
  public static final Logger LOG = LoggerFactory.getLogger(PutTest.class);

  protected String confName;
  protected PutTestConf conf = new PutTestConf();
  protected long targetTime;
  private String prefix;
  protected AtomicLong totalCount = new AtomicLong(0);


  public void run(String confName) throws Exception {
    LOG.info("confName:{}", confName);
    this.confName = confName;
    ConfLoader.load(confName, "put.", conf);
    HoloConfig config = new HoloConfig();
    ConfLoader.load(confName, "holoClient.", config);
    this.init();
    Reporter reporter = new Reporter(confName);

    try (HoloClient client = new HoloClient(config)) {
      client.sql(conn -> {
        if (conf.createTableBeforeRun) {
          SqlUtil.createTable(conn, conf.partition, conf.tableName, conf.columnCount,
              conf.shardCount,
              conf.orientation, conf.enableBinlog, conf.enableBitmap, conf.additionTsColumn,
              conf.hasPk, conf.prefixPk, conf.dataColumnType);
        }
        if (conf.vacuumTableBeforeRun) {
          SqlUtil.vaccumTable(conn, conf.tableName);
        }
        reporter.start(ConnectionUtil.getHoloVersion(conn));
        return null;
      }).get();
    }

    {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < conf.columnSize; ++i) {
        sb.append("a");
      }
      prefix = sb.toString();
    }

    targetTime = System.currentTimeMillis() + conf.testTime;
    Thread[] threads = new Thread[conf.threadSize];
    Metrics.startSlf4jReporter(60L, TimeUnit.SECONDS);
    for (int i = 0; i < threads.length; ++i) {
      threads[i] = new Thread(buildJob(i));
      threads[i].start();
    }

    for (int i = 0; i < threads.length; ++i) {
      threads[i].join();
    }
    LOG.info("finished, {} rows has written", totalCount.get());

    Metrics.reporter().report();
    {
      Meter meter = Metrics.registry().meter(Metrics.METRICS_WRITE_RPS);
      Histogram hist = Metrics.registry().histogram(Metrics.METRICS_WRITE_LATENCY);
      reporter.report(meter.getCount(), meter.getOneMinuteRate(), meter.getFiveMinuteRate(),
          meter.getFifteenMinuteRate(), hist.getSnapshot().getMean(),
          hist.getSnapshot().get99thPercentile(), hist.getSnapshot().get999thPercentile());
    }

    if (conf.deleteTableAfterDone) {
      SqlUtil.dropTableByHoloClient(config, conf.tableName);
    }
  }

  abstract Runnable buildJob(int id);

  abstract void init() throws Exception;

  protected void fillRecord(Record record, long pk, TableSchema schema, Random random,
      List<String> writeColumns) {
    Record put = record;
    for (String columnName : writeColumns) {
      int columnIndex = schema.getColumnIndex(columnName);
      Column column = schema.getColumn(columnIndex);
      long value = pk;
      Set<String> distributionKeys = new HashSet<>(Arrays.asList(schema.getDistributionKeys()));
      if (conf.prefixPk && schema.isPrimaryKey(columnName) && distributionKeys.contains(columnName)) {
        value = (pk - 1) / conf.recordCountPerPrefix;
      }

      if (schema.getPartitionIndex() == columnIndex) {
        if (conf.partition) {
          value = random.nextInt(conf.partitionCount + conf.partitionRatio);
          if (value > conf.partitionCount) {
            value = 0;
          }
        } else {
          value = 0;
        }
      }
      switch (column.getType()) {
        case Types.DOUBLE:
          put.setObject(columnIndex, value);
          break;
        case Types.NUMERIC:
          put.setObject(columnIndex, new BigDecimal(value));
        case Types.INTEGER:
        case Types.BIGINT:
        case Types.SMALLINT:
          put.setObject(columnIndex, value);
          break;
        case Types.VARCHAR:
          if (columnIndex == schema.getPartitionIndex()) {
            put.setObject(columnIndex, String.valueOf(value));
          } else {
            put.setObject(columnIndex, prefix + value);
          }
          break;
        case Types.BINARY:
          byte[] bytes = new byte[conf.columnSize];
          random.nextBytes(bytes);
          put.setObject(columnIndex, bytes);
          break;
        case Types.TIMESTAMP:
          if (conf.fillTimestampWithNow) {
            put.setObject(columnIndex, new Date());
          } else {
            put.setObject(columnIndex, new Date(value));
          }
          break;
        case Types.DATE:
          if (conf.fillTimestampWithNow) {
            put.setObject(columnIndex, new java.sql.Date(System.currentTimeMillis()));
          } else {
            put.setObject(columnIndex, new java.sql.Date(value));
          }
          break;
        default:
          throw new RuntimeException(
              "unknow type " + column.getType() + "," + column.getTypeName());
      }
    }
  }

}

class PutTestConf {
  public int threadSize = 10;
  public long testTime = 600000;
  public long rowNumber = 1000000;

  public boolean testByTime = true;

  public String tableName = "holo_perf";
  public boolean partition = false;
  public int partitionRatio = 10;
  public int partitionCount = 30;
  public int columnCount = 100;
  public int columnSize = 10;
  public String dataColumnType = "text";
  public String orientation = "column";
  public int shardCount = -1;
  public boolean enableBinlog = false;
  public int writeColumnCount = -1;

  public boolean additionTsColumn = true;
  public boolean hasPk = true;
  public boolean fillTimestampWithNow = true;
  public boolean enableBitmap = true;

  public boolean createTableBeforeRun = true;
  public boolean vacuumTableBeforeRun = false;
  public boolean deleteTableAfterDone = true;

  public boolean prefixPk = false;
  public int recordCountPerPrefix = 100;

  public String beforeSql = null;

  public String afterSql = null;
}