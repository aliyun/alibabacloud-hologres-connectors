package com.alibaba.hologres.performace.client;


import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.copy.CopyInOutputStream;
import com.alibaba.hologres.client.copy.CopyUtil;
import com.alibaba.hologres.client.copy.RecordBinaryOutputStream;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.ConfLoader;
import com.alibaba.hologres.client.utils.Metrics;
import com.alibaba.hologres.client.utils.RecordChecker;
import com.alibaba.hologres.com.codahale.metrics.Meter;
import com.alibaba.hologres.org.postgresql.PGProperty;
import com.alibaba.hologres.org.postgresql.copy.CopyManager;
import com.alibaba.hologres.org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class FixedCopyTest extends PutTest {
  public static final Logger LOG = LoggerFactory.getLogger(InsertTest.class);

  FixedCopyTestConf fixedCopyConf = new FixedCopyTestConf();
  private AtomicLong tic = new AtomicLong(0);
  private int feNumber = 0;
  private int randomOffset = 0;

  @Override
  Runnable buildJob(int id) {
    return new CopyJob(id);
  }

  @Override
  void init() throws Exception {
    ConfLoader.load(confName, "fixedCopy.", fixedCopyConf);
    if (fixedCopyConf.enableClientLoadBalance) {
      Properties props = new Properties();
      HoloConfig config = new HoloConfig();
      ConfLoader.load(confName, "holoClient.", config);
      PGProperty.USER.set(props, config.getUsername());
      PGProperty.PASSWORD.set(props, config.getPassword());
      PGProperty.APPLICATION_NAME.set(props, config.getAppName());
      LOG.info("props : {}", props);
      String jdbcUrl = config.getJdbcUrl();
      try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
        feNumber = SqlUtil.getNumberFrontends(conn);
      }
      Random random = new Random();
      this.randomOffset = random.nextInt(this.feNumber);
    }
  }

    class CopyJob implements Runnable {
    final int id;

    public CopyJob(int id) {
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

        Meter meter = Metrics.registry().meter(Metrics.METRICS_WRITE_RPS);
        Meter bpsMeter = Metrics.registry().meter(Metrics.METRICS_WRITE_BPS);
        Properties props = new Properties();
        PGProperty.USER.set(props, poolConf.getUsername());
        PGProperty.PASSWORD.set(props, poolConf.getPassword());
        PGProperty.APPLICATION_NAME.set(props, poolConf.getAppName());
        LOG.info("props : {}", props);
        String jdbcUrl = poolConf.getJdbcUrl();
        if (fixedCopyConf.enableClientLoadBalance && feNumber > 0) {
          int curFeId = (this.id + randomOffset) % feNumber + 1;
          if (jdbcUrl.indexOf("options") != -1) {
            throw new RuntimeException("holoClient.jdbcUrl not allowed to contain options when fixedCopy.enableClientLoadBalance is true");
          }
          jdbcUrl += ("?options=fe=" + curFeId);
          LOG.info("will connect to fe id {} with url {}", curFeId, jdbcUrl);
        }

        try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {

          Random rand = new Random();
          TableSchema schema =
              ConnectionUtil.getTableSchema(conn, TableName.valueOf(conf.tableName));
          SqlUtil.disableAffectedRows(conn);
          SqlUtil.setOnSessionThreadSize(conn, fixedCopyConf.onSessionThreadPoolSize);
          String copySql = null;
          List<String> columns = Util.getWriteColumnsName(conf, schema);
          if (conf.writeColumnCount > 0) {
            copySql = CopyUtil.buildCopyInSql(schema.getTableName(), columns, true,
                schema.getPrimaryKeys().length > 0, poolConf.getWriteMode(), true);
          } else {
            copySql = CopyUtil.buildCopyInSql(schema, true, poolConf.getWriteMode());
          }
          int i = 0;
          BaseConnection baseConn = conn.unwrap(BaseConnection.class);
          CopyManager copyManager = baseConn.getCopyAPI();
          LOG.info("start copy: {}", copySql);
          CopyInOutputStream cos = new CopyInOutputStream(copyManager.copyIn(copySql));

          try (RecordBinaryOutputStream os = new RecordBinaryOutputStream(cos, schema, baseConn,
              fixedCopyConf.maxRowBufferSize)) {
            while (true) {
              long pk = tic.incrementAndGet();
              ++i;
              if(conf.testByTime) {
                if (i % 1000 == 0) {
                  if (System.currentTimeMillis() > targetTime) {
                    totalCount.addAndGet(i-1);
                    LOG.info("test time reached");
                    break;
                  }
                }
              } else {
                if (pk > conf.rowNumber) {
                  totalCount.addAndGet(i-1);
                  break;
                }
              }
              Record record = new Record(schema);
              fillRecord(record, pk, schema, rand, columns);
              if (fixedCopyConf.checkRecordBeforeInsert) {
                RecordChecker.check(record);
              }
              os.putRecord(record);
              meter.mark();
              bpsMeter.mark(record.getByteSize());
            }
          }
          LOG.info("copy write : {}", cos.getResult());
        }
      } catch (Exception e) {
        LOG.error("", e);
      } finally {
        if (conf.dumpMemoryStat) {
          try {
            barrier.await();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    }
  }
}

class FixedCopyTestConf {
  public int maxRowBufferSize = 512 * 1024;
  public int onSessionThreadPoolSize = 1;
  public boolean checkRecordBeforeInsert = false;
  public boolean enableClientLoadBalance = false;
}