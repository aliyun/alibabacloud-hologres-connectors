package com.alibaba.hologres.client;

import com.alibaba.hologres.client.copy.CopyFormat;
import com.alibaba.hologres.client.copy.CopyMode;
import com.alibaba.hologres.client.copy.CopyUtil;
import com.alibaba.hologres.client.copy.in.CopyInOutputStream;
import com.alibaba.hologres.client.copy.in.CopyInWrapper;
import com.alibaba.hologres.client.copy.in.RecordTextOutputStream;
import com.alibaba.hologres.client.copy.out.CopyOutWrapper;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.binlog.BinlogRecord;
import com.alibaba.hologres.client.model.checkandput.CheckCompareOp;
import com.alibaba.hologres.client.utils.RecordChecker;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;
import org.testng.Assert;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** HoloClient Tester. HoloClientTest的行数超过了style-check的上限4000，之后的通用测试可以在本文件中实现. */
public class HoloClientGenericTest extends HoloClientTestBase {
    /** HoloConfig 的enableDeduplication参数设置测试, 设置为false表示不进行去重. */
    @Test
    public void testDeduplicationWhenInsert() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();

        try (Connection conn = buildConnection()) {
            String tableName = "\"holO_client_put_de_dup\"";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,id2 int not null, name text, primary key(id,id2))";
            String enableBinlog =
                    "call set_table_property('" + tableName + "', 'binlog.level', 'replica')";
            execute(conn, new String[] {dropSql, createSql, enableBinlog});

            // default true, 默认会去重
            Assert.assertTrue(config.isEnableDeduplication());
            try (HoloClient client = new HoloClient(config)) {
                TableSchema schema = client.getTableSchema(tableName);
                {
                    Put put2 = new Put(schema);
                    put2.setObject("id", 0);
                    put2.setObject("id2", 1);
                    put2.setObject("name", "aaa");
                    client.put(put2);
                }
                {
                    Put put2 = new Put(schema);
                    put2.setObject("id", 0);
                    put2.setObject("id2", 1);
                    put2.setObject("name", "bbb");
                    client.put(put2);
                }
                client.flush();
                {
                    Put put2 = new Put(schema);
                    put2.getRecord().setType(Put.MutationType.DELETE);
                    put2.setObject("id", 0);
                    put2.setObject("id2", 1);
                    client.put(put2);
                }
                {
                    Put put2 = new Put(schema);
                    put2.setObject("id", 0);
                    put2.setObject("id2", 1);
                    put2.setObject("name", "ccc");
                    client.put(put2);
                }
                {
                    Put put2 = new Put(schema);
                    put2.getRecord().setType(Put.MutationType.DELETE);
                    put2.setObject("id", 0);
                    put2.setObject("id2", 1);
                    client.put(put2);
                }
                {
                    Put put2 = new Put(schema);
                    put2.setObject("id", 0);
                    put2.setObject("id2", 1);
                    put2.setObject("name", "ddd");
                    client.put(put2);
                }
                client.flush();

                /*
                 hg_binlog_event_type | id | id2 | name
                ----------------------+----+-----+------
                					5 |  0 |   1 | bbb
                					3 |  0 |   1 | bbb
                					7 |  0 |   1 | ddd
                */
                int count = 0;
                try (Statement stat = conn.createStatement()) {
                    try (ResultSet rs =
                            stat.executeQuery(
                                    "select hg_binlog_event_type,* from "
                                            + tableName
                                            + " order by hg_binlog_lsn")) {
                        while (rs.next()) {
                            ++count;
                            Assert.assertEquals(0, rs.getInt(2));
                            Assert.assertEquals(1, rs.getInt(3));
                            if (count == 1) {
                                Assert.assertEquals(5, rs.getInt(1));
                                Assert.assertEquals("bbb", rs.getString(4));
                            } else if (count == 2) {
                                Assert.assertEquals(3, rs.getInt(1));
                                Assert.assertEquals("bbb", rs.getString(4));
                            } else if (count == 3) {
                                Assert.assertEquals(7, rs.getInt(1));
                                Assert.assertEquals("ddd", rs.getString(4));
                            } else {
                                throw new RuntimeException("count should not greater than 3");
                            }
                        }
                    }
                }
                Assert.assertEquals(3, count);
            }

            execute(conn, new String[] {dropSql, createSql, enableBinlog});
            // 设为false，不去重
            config.setEnableDeduplication(false);
            try (HoloClient client = new HoloClient(config)) {
                TableSchema schema = client.getTableSchema(tableName);
                {
                    Put put2 = new Put(schema);
                    put2.setObject("id", 0);
                    put2.setObject("id2", 1);
                    put2.setObject("name", "aaa");
                    client.put(put2);
                }
                {
                    Put put2 = new Put(schema);
                    put2.setObject("id", 0);
                    put2.setObject("id2", 1);
                    put2.setObject("name", "bbb");
                    client.put(put2);
                }
                client.flush();
                {
                    Put put2 = new Put(schema);
                    put2.getRecord().setType(Put.MutationType.DELETE);
                    put2.setObject("id", 0);
                    put2.setObject("id2", 1);
                    client.put(put2);
                }
                {
                    Put put2 = new Put(schema);
                    put2.setObject("id", 0);
                    put2.setObject("id2", 1);
                    put2.setObject("name", "ccc");
                    client.put(put2);
                }
                {
                    Put put2 = new Put(schema);
                    put2.getRecord().setType(Put.MutationType.DELETE);
                    put2.setObject("id", 0);
                    put2.setObject("id2", 1);
                    client.put(put2);
                }
                {
                    Put put2 = new Put(schema);
                    put2.setObject("id", 0);
                    put2.setObject("id2", 1);
                    put2.setObject("name", "ddd");
                    client.put(put2);
                }
                client.flush();

                /*
                 hg_binlog_event_type | id | id2 | name
                ----------------------+----+-----+------
                					5 |  0 |   1 | aaa
                					3 |  0 |   1 | aaa
                					7 |  0 |   1 | bbb
                					2 |  0 |   1 | bbb
                					5 |  0 |   1 | ccc
                					2 |  0 |   1 | ccc
                					5 |  0 |   1 | ddd
                */
                int count = 0;
                try (Statement stat = conn.createStatement()) {
                    try (ResultSet rs =
                            stat.executeQuery(
                                    "select hg_binlog_event_type,* from "
                                            + tableName
                                            + " order by hg_binlog_lsn")) {
                        while (rs.next()) {
                            ++count;
                            Assert.assertEquals(0, rs.getInt(2));
                            Assert.assertEquals(1, rs.getInt(3));
                            if (count == 1) {
                                Assert.assertEquals(5, rs.getInt(1));
                                Assert.assertEquals("aaa", rs.getString(4));
                            } else if (count == 2) {
                                Assert.assertEquals(3, rs.getInt(1));
                                Assert.assertEquals("aaa", rs.getString(4));
                            } else if (count == 3) {
                                Assert.assertEquals(7, rs.getInt(1));
                                Assert.assertEquals("bbb", rs.getString(4));
                            } else if (count == 4) {
                                Assert.assertEquals(2, rs.getInt(1));
                                Assert.assertEquals("bbb", rs.getString(4));
                            } else if (count == 5) {
                                Assert.assertEquals(5, rs.getInt(1));
                                Assert.assertEquals("ccc", rs.getString(4));
                            } else if (count == 6) {
                                Assert.assertEquals(2, rs.getInt(1));
                                Assert.assertEquals("ccc", rs.getString(4));
                            } else if (count == 7) {
                                Assert.assertEquals(5, rs.getInt(1));
                                Assert.assertEquals("ddd", rs.getString(4));
                            } else {
                                throw new RuntimeException("count should not greater than 7");
                            }
                        }
                    }
                }
                Assert.assertEquals(count, 7);
            }
            execute(conn, new String[] {dropSql});
        }
    }

    /** HoloConfig 的enableAggressive参数设置测试, 设置为true表示激进模式写入,期望数据量较小时可以有效减小延迟. */
    @Test
    public void testAggressiveInsert() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteBatchSize(256);
        config.setWriteThreadSize(1);
        config.setWriteMaxIntervalMs(10000);

        try (Connection conn = buildConnection()) {
            String tableName = "\"holO_client_put_aggressive\"";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null, name text, primary key(id));"
                            + "call set_table_property('"
                            + tableName
                            + "', 'shard_count', '1');\n";
            String enableBinlog =
                    "call set_table_property('" + tableName + "', 'binlog.level', 'replica')";
            execute(conn, new String[] {dropSql, "begin;", createSql, "commit;", enableBinlog});

            long interval1 = 0;
            long interval2 = 0;

            // default false, 默认不走激进模式
            Assert.assertFalse(config.isEnableAggressive());
            try (HoloClient client = new HoloClient(config)) {
                TableSchema schema = client.getTableSchema(tableName);
                {
                    for (int i = 0; i < 50; i++) {
                        Put put2 = new Put(schema);
                        put2.setObject("id", i);
                        put2.setObject("name", "aaa");
                        client.put(put2);
                        Thread.sleep(100);
                    }
                }
                client.flush();
                try (Statement stat = conn.createStatement()) {
                    try (ResultSet rs =
                            stat.executeQuery(
                                    String.format(
                                            "select max(hg_binlog_timestamp_us) - min(hg_binlog_timestamp_us) from  %s;",
                                            tableName))) {
                        while (rs.next()) {
                            // 非激进模式,数据会攒批提交,写入时间相差很小
                            interval1 = rs.getLong(1);
                        }
                    }
                }
            }

            execute(conn, new String[] {dropSql, "begin;", createSql, "commit;", enableBinlog});
            // 设为true，激进模式写入
            config.setEnableAggressive(true);
            try (HoloClient client = new HoloClient(config)) {
                TableSchema schema = client.getTableSchema(tableName);
                {
                    for (int i = 0; i < 50; i++) {
                        Put put2 = new Put(schema);
                        put2.setObject("id", i);
                        put2.setObject("name", "aaa");
                        client.put(put2);
                        Thread.sleep(100);
                    }
                }
                client.flush();
                try (Statement stat = conn.createStatement()) {
                    try (ResultSet rs =
                            stat.executeQuery(
                                    String.format(
                                            "select max(hg_binlog_timestamp_us) - min(hg_binlog_timestamp_us) from  %s;",
                                            tableName))) {
                        while (rs.next()) {
                            // 激进模式,连接空闲即提交,写入时间相差接近5秒钟
                            interval2 = rs.getLong(1);
                        }
                    }
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
            Assert.assertTrue(interval2 >= interval1);
        }
    }

    @Ignore
    @Test
    public void testTableVersionChangeWhenInsert() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteBatchSize(256);
        config.setWriteThreadSize(1);
        config.setWriteMaxIntervalMs(3000);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "\"holO_client_table_version_change\"";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table " + tableName + "(id int not null, name text, primary key(id));";
            execute(conn, new String[] {dropSql, createSql});

            AtomicBoolean running = new AtomicBoolean(true);
            AtomicReference<Exception> failed = new AtomicReference(null);
            ExecutorService es =
                    new ThreadPoolExecutor(
                            20,
                            20,
                            0L,
                            TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<>(2),
                            r -> {
                                Thread t = new Thread(r);
                                return t;
                            },
                            new ThreadPoolExecutor.AbortPolicy());

            Runnable insert =
                    () -> {
                        try {
                            int i = 0;
                            while (running.get()) {
                                TableSchema schema = client.getTableSchema(tableName, true);
                                {
                                    Put put2 = new Put(schema);
                                    put2.setObject("id", i++);
                                    put2.setObject("name", "aaa");
                                    client.put(put2);
                                }
                            }
                        } catch (Exception e) {
                            failed.set(e);
                        }
                    };
            Runnable alter =
                    () -> {
                        try {
                            int i = 0;
                            while (running.get()) {
                                Thread.sleep(100);
                                execute(
                                        conn,
                                        new String[] {
                                            "alter table "
                                                    + tableName
                                                    + "add column c_"
                                                    + i++
                                                    + " int;"
                                        });
                            }
                        } catch (Exception e) {
                            failed.set(e);
                        }
                    };

            es.execute(insert);
            es.execute(alter);

            Thread.sleep(10000);
            running.set(false);

            es.shutdown();
            while (!es.awaitTermination(5000L, TimeUnit.MILLISECONDS)) {}

            client.flush();
            if (failed.get() != null) {
                Assert.fail("fail", failed.get());
            }
            execute(conn, new String[] {dropSql});
        }
    }

    @Test
    public void testInsertWhenIgnoreDirtyData() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteBatchSize(256);
        config.setWriteThreadSize(1);
        config.setEnableDeduplication(false);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setEnableDefaultForNotNullColumn(false);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "\"holO_client_table_version_change\"";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null, name text not null, primary key(id)) with (binlog_level='replica');";
            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName, true);
            for (int i = 0; i < 5; i++) {
                try {
                    Put put = new Put(schema);
                    put.setObject("id", 0);
                    if (i == 2) {
                        put.setObject("name", null);
                    } else {
                        put.setObject("name", "aaa" + i);
                    }
                    client.put(put);
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage().contains("violates not-null constraint"));
                }
            }
            try {
                client.flush();
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("violates not-null constraint"));
            }

            try (Statement stat = conn.createStatement()) {
                try (ResultSet rs =
                        stat.executeQuery(
                                String.format(
                                        "select count(*) from %s where hg_binlog_event_type in (5,7) ;",
                                        tableName))) {
                    while (rs.next()) {
                        // aaa0, aaa1, aaa3, aaa4
                        Assert.assertEquals(4, rs.getInt(1));
                    }
                }
            }
            execute(conn, new String[] {dropSql});
        }
    }

    /** INSERT. Method: checkAndPut(CheckAndPut put). checkAndPut 和 Put 交替调用 */
    @Test
    public void testPutWithCheckAndPutUseSameClient() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setForceFlushInterval(100000);
        config.setWriteThreadSize(1);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_put_with_check_and_put_same_client_batch";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text,modify_time timestamptz, address text,primary key(id)) with (binlog_level='replica');";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            Put put = new Put(schema);
            put.setObject(0, 0);
            put.setObject(1, "name0");
            put.setObject(2, "2020-01-01 00:00:00");
            put.setObject(3, "address0");
            client.put(put);
            put = new Put(schema);
            put.setObject(0, 1);
            put.setObject(1, "name1");
            put.setObject(2, "2020-01-01 00:00:00");
            put.setObject(3, "address1");
            client.put(put);

            CheckAndPut checkAndPut =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            null,
                            "1970-01-01 00:08:00");
            checkAndPut.setObject(0, 0);
            checkAndPut.setObject(1, "name0_new");
            checkAndPut.setObject(2, "2021-01-01 00:00:00");
            client.checkAndPut(checkAndPut);
            checkAndPut =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            null,
                            "1970-01-01 00:08:00");
            checkAndPut.setObject(0, 1);
            checkAndPut.setObject(1, "name1_new");
            checkAndPut.setObject(2, "2019-01-01 00:00:00");
            client.checkAndPut(checkAndPut);

            put = new Put(schema);
            put.setObject(0, 0);
            put.setObject(3, "address0_new");
            client.put(put);
            put = new Put(schema);
            put.setObject(0, 1);
            put.setObject(3, "address1_new");
            client.put(put);
            client.flush();

            // pk = 0, check通过，更新checkAndPut + put 2次
            try (Statement stat = conn.createStatement()) {
                try (ResultSet rs =
                        stat.executeQuery(
                                String.format(
                                        "select count(*) from %s where id = 0 and hg_binlog_event_type in (5,7) ;",
                                        tableName))) {
                    while (rs.next()) {
                        Assert.assertEquals(3, rs.getInt(1));
                    }
                }
            }
            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
            Assert.assertEquals("name0_new", r.getObject(1));
            Assert.assertEquals(Timestamp.valueOf("2021-01-01 00:00:00.0"), r.getObject(2));
            Assert.assertEquals("address0_new", r.getObject(3));

            // pk = 1, check未通过，更新put 1次
            try (Statement stat = conn.createStatement()) {
                try (ResultSet rs =
                        stat.executeQuery(
                                String.format(
                                        "select count(*) from %s where id = 1 and hg_binlog_event_type in (5,7) ;",
                                        tableName))) {
                    while (rs.next()) {
                        Assert.assertEquals(2, rs.getInt(1));
                    }
                }
            }
            r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            Assert.assertEquals("name1", r.getObject(1));
            Assert.assertEquals(Timestamp.valueOf("2020-01-01 00:00:00.0"), r.getObject(2));
            Assert.assertEquals("address1_new", r.getObject(3));

            execute(conn, new String[] {dropSql});
        }
    }

    /**
     * generated column
     *
     * <p>Method: put(Put put).
     */
    @Test
    public void testGeneratedColumn() throws Exception {
        if (properties == null) {
            return;
        }
        if (holoVersion.compareTo(new HoloVersion(3, 1, 0)) < 0) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setDynamicPartition(true);
        config.setConnectionMaxIdleMs(10000L);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "\"test_SCHema\".\"holO_client_generated_column_as_pk_001\"";
            String dropSql = "drop table if exists " + tableName;
            String createSchema = "create schema if not exists \"test_SCHema\"";
            String createSql =
                    "create table "
                            + tableName
                            + "(a text not null,b text not null, ts timestamp not null, ds timestamp "
                            + "GENERATED ALWAYS AS (date_trunc('day', ts)) STORED not null, primary key(a,ds)) "
                            + "with (distribution_key='a')";

            execute(conn, new String[] {createSchema, dropSql, createSql});

            try {
                TableSchema schema = client.getTableSchema(tableName, true);
                Assert.assertTrue(schema.getColumn(3).isGeneratedColumn());
                try {
                    Put put1 = new Put(schema);
                    put1.setObject("a", "testaaa");
                    put1.setObject("b", "testbbb");
                    put1.setObject("ts", LocalDateTime.now());
                    put1.setObject("ds", LocalDateTime.now());
                    client.put(put1);
                    client.flush();
                } catch (Exception e) {
                    // generated column is primary key and distribution key, could not do update now
                    if (!e.getMessage().contains("generated column ds can not be set")) {
                        throw e;
                    }
                }

                Put put1 = new Put(schema);
                put1.setObject("a", "testaaa");
                put1.setObject("b", "testbbb");
                put1.setObject("ts", LocalDateTime.now());
                client.put(put1);
                client.flush();

                int count = 0;
                try (Statement stat = conn.createStatement()) {
                    try (ResultSet rs =
                            stat.executeQuery("select * from " + tableName + " order by ds")) {
                        while (rs.next()) {
                            ++count;
                            Assert.assertEquals("testaaa", rs.getObject(1));
                            Assert.assertEquals("testbbb", rs.getString(2));
                            Timestamp ts = rs.getTimestamp(3);
                            Timestamp ds = rs.getTimestamp(4);
                            ts.setHours(0);
                            ts.setMinutes(0);
                            ts.setSeconds(0);
                            ts.setNanos(0);
                            Assert.assertEquals(ts, ds);
                        }
                    }
                }
                Assert.assertEquals(1, count);
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /**
     * generated column is distribution key
     *
     * <p>Method: put(Put put).
     */
    @Test
    public void testGeneratedColumnIsDistributionKey() throws Exception {
        if (properties == null) {
            return;
        }
        if (holoVersion.compareTo(new HoloVersion(3, 1, 0)) < 0) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_IGNORE);
        HoloConfig config1 = buildConfig();
        config1.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
        HoloConfig config2 = buildConfig();
        config2.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config);
                HoloClient client1 = new HoloClient(config1);
                HoloClient client2 = new HoloClient(config2)) {
            String tableName = "\"test_SCHema\".\"holO_client_generated_column_as_pk_002\"";
            String dropSql = "drop table if exists " + tableName;
            String createSchema = "create schema if not exists \"test_SCHema\"";
            String createSql =
                    "create table "
                            + tableName
                            + "(a text not null,b text not null, ts timestamp not null, ds timestamp "
                            + "GENERATED ALWAYS AS (date_trunc('day', ts)) STORED not null, primary key(a,ds))  "
                            + "with (distribution_key='a,ds')";

            execute(conn, new String[] {createSchema, dropSql, createSql});
            try {
                TableSchema schema = client1.getTableSchema(tableName, true);
                Assert.assertTrue(schema.getColumn(3).isGeneratedColumn());

                LocalDateTime now = LocalDateTime.now();
                Put put1 = new Put(schema);
                put1.setObject("a", 1);
                put1.setObject("b", "test");
                put1.setObject("ts", now);
                client1.put(put1);
                client1.flush();
            } catch (Exception e) {
                // generated column is primary key and distribution key, could not do update now
                if (!e.getMessage()
                        .contains(
                                "Feature not supported: UPDATE distribution key generated column ds")) {
                    throw e;
                }
            }
            try {
                TableSchema schema = client2.getTableSchema(tableName, true);
                Assert.assertTrue(schema.getColumn(3).isGeneratedColumn());

                LocalDateTime now = LocalDateTime.now();
                Put put1 = new Put(schema);
                put1.setObject("a", 1);
                put1.setObject("b", "test");
                put1.setObject("ts", now);
                client2.put(put1);
                client2.flush();
            } catch (Exception e) {
                // generated column is primary key and distribution key, could not do update now
                if (!e.getMessage()
                        .contains(
                                "Feature not supported: UPDATE distribution key generated column ds")) {
                    throw e;
                }
            }
            try {
                TableSchema schema = client.getTableSchema(tableName, true);
                Assert.assertTrue(schema.getColumn(3).isGeneratedColumn());

                Put put1 = new Put(schema);
                put1.setObject("a", "testaaa");
                put1.setObject("b", "testbbb");
                put1.setObject("ts", LocalDateTime.now());
                client.put(put1);
                client.flush();

                int count = 0;
                try (Statement stat = conn.createStatement()) {
                    try (ResultSet rs =
                            stat.executeQuery("select * from " + tableName + " order by ds")) {
                        while (rs.next()) {
                            ++count;
                            Assert.assertEquals("testaaa", rs.getObject(1));
                            Assert.assertEquals("testbbb", rs.getString(2));
                            Timestamp ts = rs.getTimestamp(3);
                            Timestamp ds = rs.getTimestamp(4);
                            ts.setHours(0);
                            ts.setMinutes(0);
                            ts.setSeconds(0);
                            ts.setNanos(0);
                            Assert.assertEquals(ts, ds);
                        }
                    }
                }
                Assert.assertEquals(1, count);
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /**
     * The case cannot verify whether the log slow query is effective, just print slow query id in
     * log.
     */
    @Test
    public void testLogSlowQueryId() throws Exception {
        if (properties == null) {
            return;
        }
        boolean[] useFixedFes = new boolean[] {false, true};
        for (boolean useFixed : useFixedFes) {
            HoloConfig config = buildConfig();
            config.setAppName("testLogSlowQueryId");
            config.setReadThreadSize(2);
            config.setWriteThreadSize(2);
            config.setWriteBatchSize(32);
            config.setEnableLogSlowQuery(true);
            config.setLogSlowQueryThresholdMs(10);
            config.setLogSlowQueryIntervalSeconds(1);
            config.setUseFixedFe(useFixed);
            try (Connection conn = buildConnection()) {
                String tableName = "holo_client_log_slow_query_id_" + useFixed;
                String dropSql = "drop table if exists " + tableName;
                String createSql =
                        "create table "
                                + tableName
                                + "(id int not null,name text, primary key(id))";
                execute(conn, new String[] {dropSql, createSql});

                try (HoloClient client = new HoloClient(config)) {
                    TableSchema schema = client.getTableSchema(tableName, true);
                    Runnable bulkLoad =
                            () -> {
                                try {
                                    try (Statement stat = conn.createStatement()) {
                                        stat.execute(
                                                "set hg_experimental_copy_enable_on_conflict = on");
                                    }
                                    String copySql =
                                            CopyUtil.buildCopyInSql(
                                                    schema,
                                                    CopyFormat.CSV,
                                                    OnConflictAction.INSERT_OR_UPDATE,
                                                    CopyMode.BULK_LOAD_ON_CONFLICT);
                                    CopyManager copyManager =
                                            new CopyManager(conn.unwrap(PgConnection.class));
                                    CopyInOutputStream os =
                                            new CopyInOutputStream(copyManager.copyIn(copySql));
                                    RecordTextOutputStream ros =
                                            new RecordTextOutputStream(
                                                    os,
                                                    schema,
                                                    conn.unwrap(PgConnection.class),
                                                    1024 * 1024 * 10);
                                    for (int i = 0; i < 100; i++) {
                                        Put put = new Put(schema);
                                        put.setObject(0, i);
                                        put.setObject(1, "name0");
                                        ros.putRecord(put.getRecord());
                                        Thread.sleep(100);
                                    }
                                    ros.close();
                                } catch (SQLException | IOException | InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            };
                    Thread t = new Thread(bulkLoad);
                    t.start();

                    try {
                        Thread.sleep(5000);
                        for (int i = 100; i < 200; i++) {
                            Put put = new Put(schema);
                            put.setObject(0, i);
                            put.setObject(1, "name0");
                            client.put(put);
                        }
                        client.flush();
                        for (int i = 100; i < 200; i++) {
                            Record r =
                                    client.get(
                                                    Get.newBuilder(schema)
                                                            .setPrimaryKey("id", i)
                                                            .build())
                                            .get();
                            Assert.assertEquals("name0", r.getObject("name"));
                        }
                    } catch (HoloClientException e) {
                        throw new RuntimeException(e);
                    }
                    try {
                        t.join(15000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    execute(conn, new String[] {dropSql});
                }
            }
        }
    }

    /** put,get,copy in,copy out,binlog timestamp Daylight Saving Time test. */
    @Test(groups = "nonConcurrentGroup")
    public void testReadWriteTimestampDST007() throws Exception {
        if (properties == null) {
            return;
        }
        TimeZone old = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Berlin"));
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(buildConfig())) {
            String tableName = "holo_copy_test_daylight_saving_time_007";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(a int not null,b timestamp, c timestamptz, d time, e timetz, f date, primary key(a))"
                            + "with (binlog_level = 'replica')";
            try {
                execute(conn, new String[] {dropSql, createSql});
                try (CopyInWrapper copyIn =
                        new CopyInWrapper(
                                conn,
                                tableName,
                                Arrays.asList("a", "b", "c", "d", "e", "f"),
                                CopyFormat.BINARY,
                                CopyMode.STREAM,
                                OnConflictAction.INSERT_OR_UPDATE,
                                1024 * 1024 * 10)) {
                    TableSchema schema = copyIn.getSchema();
                    {
                        Record record = new Record(schema);
                        record.setObject(0, 1);
                        record.setObject(1, Timestamp.valueOf("2025-01-02 12:00:00"));
                        record.setObject(2, Timestamp.valueOf("2025-01-02 12:00:00"));
                        record.setObject(3, Time.valueOf("12:00:00"));
                        record.setObject(4, Time.valueOf("12:00:00"));
                        record.setObject(5, Date.valueOf("2025-01-02"));

                        RecordChecker.check(record);
                        copyIn.putRecord(record);
                        copyIn.flush();
                    }
                    {
                        Record record = new Record(schema);
                        record.setObject(0, 2);
                        record.setObject(1, Timestamp.valueOf("2025-07-02 12:00:00"));
                        record.setObject(2, Timestamp.valueOf("2025-07-02 12:00:00"));
                        record.setObject(3, Time.valueOf("12:00:00"));
                        record.setObject(4, Time.valueOf("12:00:00"));
                        record.setObject(5, Date.valueOf("2025-07-02"));

                        RecordChecker.check(record);
                        copyIn.putRecord(record);
                        copyIn.flush();
                    }
                }
                TableSchema schema = client.getTableSchema(tableName);
                {
                    Put put = new Put(schema);
                    put.setObject(0, 3);
                    put.setObject(1, Timestamp.valueOf("2025-01-02 12:00:00"));
                    put.setObject(2, Timestamp.valueOf("2025-01-02 12:00:00"));
                    put.setObject(3, Time.valueOf("12:00:00"));
                    put.setObject(4, Time.valueOf("12:00:00"));
                    put.setObject(5, Date.valueOf("2025-01-02"));
                    client.put(put);
                    client.flush();
                }
                {
                    Put put = new Put(schema);
                    put.setObject(0, 4);
                    put.setObject(1, Timestamp.valueOf("2025-07-02 12:00:00"));
                    put.setObject(2, Timestamp.valueOf("2025-07-02 12:00:00"));
                    put.setObject(3, Time.valueOf("12:00:00"));
                    put.setObject(4, Time.valueOf("12:00:00"));
                    put.setObject(5, Date.valueOf("2025-07-02"));
                    client.put(put);
                    client.flush();
                }
                try (Statement statement = conn.createStatement()) {
                    try (ResultSet rs =
                            statement.executeQuery("select * from " + tableName + " order by a")) {
                        while (rs.next()) {
                            if (rs.getInt(1) % 2 == 1) {
                                Assert.assertEquals(
                                        Timestamp.valueOf("2025-01-02 12:00:00"),
                                        rs.getTimestamp(2));
                                Assert.assertEquals(
                                        Timestamp.valueOf("2025-01-02 12:00:00"),
                                        rs.getTimestamp(3));
                                Assert.assertEquals(Time.valueOf("12:00:00"), rs.getTime(4));
                                Assert.assertEquals(Time.valueOf("12:00:00"), rs.getTime(5));
                                Assert.assertEquals(Date.valueOf("2025-01-02"), rs.getDate(6));
                            } else {
                                Assert.assertEquals(
                                        Timestamp.valueOf("2025-07-02 12:00:00"),
                                        rs.getTimestamp(2));
                                Assert.assertEquals(
                                        Timestamp.valueOf("2025-07-02 12:00:00"),
                                        rs.getTimestamp(3));
                                Assert.assertEquals(Time.valueOf("12:00:00"), rs.getTime(4));
                                Assert.assertEquals(Time.valueOf("12:00:00"), rs.getTime(5));
                                Assert.assertEquals(Date.valueOf("2025-07-02"), rs.getDate(6));
                            }
                        }
                    }
                }

                for (int i = 1; i <= 4; i++) {
                    Record pkRecord = new Record(schema);
                    pkRecord.setObject(0, i);
                    Record r = client.get(new Get(pkRecord)).get();
                    Assert.assertEquals(i, r.getObject("a"));
                    if (i % 2 == 1) {
                        Assert.assertEquals(
                                Timestamp.valueOf("2025-01-02 12:00:00"), r.getObject(1));
                        Assert.assertEquals(
                                Timestamp.valueOf("2025-01-02 12:00:00"), r.getObject(2));
                        Assert.assertEquals(Time.valueOf("12:00:00"), r.getObject(3));
                        Assert.assertEquals(Time.valueOf("12:00:00"), r.getObject(4));
                        Assert.assertEquals(Date.valueOf("2025-01-02"), r.getObject(5));
                    } else {
                        Assert.assertEquals(
                                Timestamp.valueOf("2025-07-02 12:00:00"), r.getObject(1));
                        Assert.assertEquals(
                                Timestamp.valueOf("2025-07-02 12:00:00"), r.getObject(2));
                        Assert.assertEquals(Time.valueOf("12:00:00"), r.getObject(3));
                        Assert.assertEquals(Time.valueOf("12:00:00"), r.getObject(4));
                        Assert.assertEquals(Date.valueOf("2025-07-02"), r.getObject(5));
                    }
                }

                try (CopyOutWrapper copyOutWrapper =
                        new CopyOutWrapper(
                                conn,
                                tableName,
                                Arrays.asList("a", "b", "c", "d", "e", "f"),
                                CopyFormat.ARROW,
                                Collections.emptyList(),
                                "",
                                128)) {
                    int count = 0;
                    while (copyOutWrapper.hasNextBatch()) {
                        List<Record> records = copyOutWrapper.getRecords();
                        for (Record r : records) {
                            if ((int) r.getObject("a") % 2 == 1) {
                                Assert.assertEquals(
                                        Timestamp.valueOf("2025-01-02 12:00:00"), r.getObject(1));
                                Assert.assertEquals(
                                        Timestamp.valueOf("2025-01-02 12:00:00"), r.getObject(2));
                                Assert.assertEquals(Time.valueOf("12:00:00"), r.getObject(3));
                                Assert.assertEquals(Time.valueOf("12:00:00"), r.getObject(4));
                                Assert.assertEquals(Date.valueOf("2025-01-02"), r.getObject(5));
                            } else {
                                Assert.assertEquals(
                                        Timestamp.valueOf("2025-07-02 12:00:00"), r.getObject(1));
                                Assert.assertEquals(
                                        Timestamp.valueOf("2025-07-02 12:00:00"), r.getObject(2));
                                Assert.assertEquals(Time.valueOf("12:00:00"), r.getObject(3));
                                Assert.assertEquals(Time.valueOf("12:00:00"), r.getObject(4));
                                Assert.assertEquals(Date.valueOf("2025-07-02"), r.getObject(5));
                            }
                            ++count;
                        }
                    }
                    Assert.assertEquals(4, count);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                try (BinlogShardGroupReader reader =
                        client.binlogSubscribe(
                                Subscribe.newStartTimeBuilder(tableName)
                                        .setBinlogReadStartTime("2025-01-01 00:00:00")
                                        .build())) {
                    BinlogRecord r;
                    int count = 0;
                    while ((r = reader.getBinlogRecord()) != null) {
                        if ((int) r.getObject("a") % 2 == 1) {
                            Assert.assertEquals(
                                    Timestamp.valueOf("2025-01-02 12:00:00"), r.getObject(1));
                            Assert.assertEquals(
                                    Timestamp.valueOf("2025-01-02 12:00:00"), r.getObject(2));
                            Assert.assertEquals(Time.valueOf("12:00:00"), r.getObject(3));
                            Assert.assertEquals(Time.valueOf("12:00:00"), r.getObject(4));
                            Assert.assertEquals(Date.valueOf("2025-01-02"), r.getObject(5));
                        } else {
                            Assert.assertEquals(
                                    Timestamp.valueOf("2025-07-02 12:00:00"), r.getObject(1));
                            Assert.assertEquals(
                                    Timestamp.valueOf("2025-07-02 12:00:00"), r.getObject(2));
                            Assert.assertEquals(Time.valueOf("12:00:00"), r.getObject(3));
                            Assert.assertEquals(Time.valueOf("12:00:00"), r.getObject(4));
                            Assert.assertEquals(Date.valueOf("2025-07-02"), r.getObject(5));
                        }
                        ++count;
                        if (count == 4) {
                            break;
                        }
                    }
                }
            } finally {
                TimeZone.setDefault(old);
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** DEFAULT列. */
    @Test
    public void testDefaultColumn() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        // 生成列根据主键字段生成, 且是分布键
        boolean supportPkG = holoVersion.compareTo(new HoloVersion(3, 2, 6)) >= 0;

        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        boolean[] isUseLegacys = new boolean[] {true, false};

        for (boolean isUseLegacy : isUseLegacys) {
            config.setUseLegacyPutHandler(isUseLegacy);

            try (Connection conn = buildConnection();
                    HoloClient client = new HoloClient(config)) {
                String tableName = "holo_client_put_default_column_" + isUseLegacy;
                String dropSql = "drop table if exists " + tableName;
                String id2 =
                        supportPkG
                                ? "id2 int GENERATED ALWAYS AS (id + 1) STORED"
                                : "id2 int default 1";
                String createSql =
                        "create table "
                                + tableName
                                + "(id int not null, id1 int default 1, "
                                + id2
                                + " , "
                                + "name text, col1 text default 'foo', col2 text GENERATED ALWAYS AS (name || '_bar') STORED, primary key(id, id1, id2))";

                execute(conn, new String[] {dropSql, createSql});

                TableSchema schema = client.getTableSchema(tableName);

                Put put = new Put(schema);
                put.setObject("id", 0);
                put.setObject("name", "name0");
                client.put(put);

                client.flush();

                Record r = client.get(new Get(schema, new Object[] {0, 1, 1})).get();
                Assert.assertEquals("name0", r.getObject("name"));
                Assert.assertEquals("foo", r.getObject("col1"));
                Assert.assertEquals("name0_bar", r.getObject("col2"));

                execute(conn, new String[] {dropSql});
            }
        }
    }
}
