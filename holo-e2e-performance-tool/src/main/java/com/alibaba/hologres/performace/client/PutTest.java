package com.alibaba.hologres.performace.client;


import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.*;
import com.alibaba.hologres.client.utils.ConfLoader;
import com.alibaba.hologres.client.utils.Metrics;
import com.alibaba.hologres.com.codahale.metrics.Histogram;
import com.alibaba.hologres.com.codahale.metrics.Meter;
import com.alibaba.hologres.org.postgresql.PGProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public abstract class PutTest {
  public static final Logger LOG = LoggerFactory.getLogger(PutTest.class);

  protected String confName;
  protected PutTestConf conf = new PutTestConf();
  protected long targetTime;
  protected AtomicLong totalCount = new AtomicLong(0);
  protected static long memoryUsage = 0;
  protected CyclicBarrier barrier = null;
  protected HoloVersion version;



  public void run(String confName) throws Exception {
    LOG.info("confName:{}", confName);
    this.confName = confName;
    ConfLoader.load(confName, "put.", conf);
    HoloConfig config = new HoloConfig();
    ConfLoader.load(confName, "holoClient.", config);
    Properties props = new Properties();
    PGProperty.USER.set(props, config.getUsername());
    PGProperty.PASSWORD.set(props, config.getPassword());
    PGProperty.APPLICATION_NAME.set(props, config.getAppName());
    LOG.info("props : {}", props);
    String jdbcUrl = config.getJdbcUrl();
    this.init();
    Reporter reporter = new Reporter(confName);

    try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
      if (conf.createTableBeforeRun) {
        SqlUtil.createTable(conn, conf.partition, conf.tableName, conf.columnCount,
                conf.shardCount,
                conf.orientation, conf.enableBinlog, conf.binlogTTL, conf.enableBitmap, conf.additionTsColumn,
                conf.hasPk, conf.prefixPk, conf.dataColumnType);
      }
      if (conf.enableBinlogConsumption) {
        SqlUtil.createBinLogExtension(conn);
        if (conf.recreatePublicationAndSlot) {
          SqlUtil.dropPublication(conn, conf.publicationName);
          try {
            SqlUtil.deleteReplicationProgress(conn, conf.slotName);
          } catch (SQLException e) {
            e.printStackTrace();
          }
          try {
            SqlUtil.dropSlot(conn, conf.slotName);
          } catch (SQLException e) {
            e.printStackTrace();
          }
          SqlUtil.createPublication(conn, conf.publicationName, conf.tableName);
          SqlUtil.createSlot(conn, conf.publicationName, conf.slotName);
        }
      }
      if (conf.vacuumTableBeforeRun) {
        SqlUtil.vaccumTable(conn, conf.tableName);
      }
      version = ConnectionUtil.getHoloVersion(conn);
      reporter.start(version);
    }

    barrier = new CyclicBarrier(conf.threadSize, ()->{
      memoryUsage = Util.getMemoryStat();
      Util.dumpHeap(confName);
    });
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
          hist.getSnapshot().get99thPercentile(), hist.getSnapshot().get999thPercentile(), memoryUsage);
    }

    if (conf.deleteTableAfterDone) {
      SqlUtil.dropTableByHoloClient(config, conf.tableName);
    }
  }

  abstract Runnable buildJob(int id);

  abstract void init() throws Exception;

  protected void fillRecord(Record record, long pk, TableSchema schema, Random random,
                            List<String> writeColumns) {
    fillRecord(record, pk, schema, random, writeColumns, false);
  }
  protected void fillRecord(Record record, long pk, TableSchema schema, Random random,
      List<String> writeColumns, boolean enableRandomPartialCol) {
    Record put = record;
    for (String columnName : writeColumns) {
      int columnIndex = schema.getColumnIndex(columnName);
      Column column = schema.getColumn(columnIndex);
      long value = pk;
      if (!schema.isPrimaryKey(columnName) && enableRandomPartialCol) {
        int randNum = random.nextInt(3);
        if (randNum == 0) {
          continue;
        }
      }
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
            put.setObject(columnIndex, Util.alignWithColumnSize(value, conf.columnSize));
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
  public String publicationName = "holo_perf_publication";
  public String slotName = "holo_perf_slot";
  public boolean partition = false;
  public int partitionRatio = 10;
  public int partitionCount = 30;
  public int columnCount = 100;
  public int columnSize = 10;
  public String dataColumnType = "text";
  public String orientation = "column";
  public int shardCount = -1;
  public boolean enableBinlog = false;
  public boolean enableBinlogConsumption = false;
  public boolean recreatePublicationAndSlot = true;
  public int binlogTTL = 19600; //6 hours
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

  public boolean dumpMemoryStat = false;

  public String beforeSql = null;

  public String afterSql = null;
}