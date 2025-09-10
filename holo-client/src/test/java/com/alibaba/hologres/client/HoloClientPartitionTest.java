/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.HoloClientWithDetailsException;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.Partition;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.RecordScanner;
import com.alibaba.hologres.client.model.TableSchema;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Random;

/** 分区表测试用例. */
public class HoloClientPartitionTest extends HoloClientTestBase {
    public class PartitionColumnCase {
        public String datatype;
        public String partitionTableNameSuffix;
        public String partitionValue;
        public Object putValue;

        PartitionColumnCase(
                String datatype,
                String partitionTableNameSuffix,
                String partitionValue,
                Object putValue) {
            this.datatype = datatype;
            this.partitionTableNameSuffix = partitionTableNameSuffix;
            this.partitionValue = partitionValue;
            this.putValue = putValue;
        }
    }

    @DataProvider(name = "partitionColumnCase")
    public Object[] createData() {
        PartitionColumnCase[] ret = new PartitionColumnCase[9];
        // date类型可以通过字符串或者date对象写入, 但默认创建的分区表名后缀格式为yyyy-MM-dd, 分区字段(for values in)一定是yyyy-MM-dd
        ret[0] =
                new PartitionColumnCase(
                        "date", "2023-07-14", "2023-07-14", Date.valueOf("2023-07-14"));
        ret[1] = new PartitionColumnCase("date", "2023-07-14", "2023-07-14", "2023-07-14");
        ret[2] = new PartitionColumnCase("date", "2023-07-14", "2023-07-14", "20230714");

        // date类型可以通过字符串或者date对象写入, 但用户动态分区的后缀是YYYYMMDD, 分区字段(for values in)一定是yyyy-MM-dd
        ret[3] =
                new PartitionColumnCase(
                        "date", "20230714", "2023-07-14", Date.valueOf("2023-07-14"));
        ret[4] = new PartitionColumnCase("date", "20230714", "2023-07-14", "2023-07-14");
        ret[5] = new PartitionColumnCase("date", "20230714", "2023-07-14", "20230714");

        ret[6] = new PartitionColumnCase("int", "1234", "1234", 1234);
        ret[7] = new PartitionColumnCase("text", "2023-07-14", "2023-07-14", "2023-07-14");
        ret[8] = new PartitionColumnCase("varchar(20)", "20230714_1", "20230714_1", "20230714_1");
        return ret;
    }

    @Test
    public void testPartition001() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
        config.setDynamicPartition(true);
        config.setConnectionMaxIdleMs(10000L);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "test_schema.\"partition_001\"";
            String childTableName = "test_schema.\"partition_001_a\"";
            String createSchema = "create schema if not exists test_schema";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text,ds text not null,primary key(id,ds)) partition by list(ds)";
            String createChildSql =
                    "create table "
                            + childTableName
                            + " partition of "
                            + tableName
                            + " for values in ('a')";
            String addColumn = "alter table " + tableName + " add column age int";
            execute(conn, new String[] {createSchema, dropSql, createSql, createChildSql});

            try {
                TableSchema schema = client.getTableSchema(tableName, true);

                Assert.assertEquals(3, schema.getColumnSchema().length);
                Put put = new Put(schema);
                put.setObject(0, 0);
                put.setObject(1, "name");
                put.setObject(2, "a");

                execute(conn, new String[] {addColumn});

                client.put(put);

                client.flush();

                int count = 0;
                try (Statement stat = conn.createStatement()) {
                    try (ResultSet rs = stat.executeQuery("select * from " + tableName)) {
                        while (rs.next()) {
                            ++count;
                            Assert.assertEquals(0, rs.getInt(1));
                            Assert.assertEquals("name", rs.getString(2));
                            Assert.assertEquals("a", rs.getString(3));
                            Assert.assertNull(rs.getObject(4));
                        }
                    }
                }
                Assert.assertEquals(1, count);
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** delete/insert not exists partition table Method: put(Put put). */
    @Test
    public void testPartition002() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setDynamicPartition(false); // child table is not exists
        config.setConnectionMaxIdleMs(10000L);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "\"holO_client_partition_002\"";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(iD int not null,id2 int not null, name text, primary key(id,id2)) partition by list(iD)";

            execute(conn, new String[] {dropSql, createSql});

            try {
                TableSchema schema = client.getTableSchema(tableName, true);

                // delete not throw exception
                Put put1 = new Put(schema);
                put1.setObject("id", 10);
                put1.setObject("id2", 1);
                put1.getRecord().setType(Put.MutationType.DELETE);
                client.put(put1);
                client.flush();

                // insert throw exception
                Assert.assertThrows(
                        HoloClientWithDetailsException.class,
                        () -> {
                            try {
                                Put put2 = new Put(schema);
                                put2.setObject("id", 10);
                                put2.setObject("id2", 1);
                                client.put(put2);
                                client.flush();
                            } catch (HoloClientWithDetailsException e) {
                                Assert.assertTrue(
                                        e.getMessage().contains("child table is not found"));
                                throw e;
                            }
                        });
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** Partition column type test Method: put(Put put). */
    @Test(dataProvider = "partitionColumnCase")
    public void testPartition003(PartitionColumnCase partitionColumnCase) throws Exception {
        if (properties == null) {
            return;
        }
        boolean yyyymmdd =
                partitionColumnCase.datatype.equals("date")
                        && partitionColumnCase.partitionTableNameSuffix.length() == 8;
        if (holoVersion.compareTo(new HoloVersion(3, 0, 12)) < 0 && yyyymmdd) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setDynamicPartition(true);
        config.setWriteThreadSize(3);
        config.setConnectionMaxIdleMs(10000L);

        try (Connection conn = buildConnection()) {

            boolean isStr =
                    !Objects.equals(partitionColumnCase.datatype, "int")
                            && !Objects.equals(partitionColumnCase.datatype, "bigint");
            String tableName =
                    "\"holO_client_partition_003_"
                            + partitionColumnCase.datatype
                            + new Random().nextLong()
                            + "\"";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(iD "
                            + partitionColumnCase.datatype
                            + " not null,id2 int not null, name text, primary key(id,id2)) partition by list(iD)"
                            + (yyyymmdd
                                    ? " with (auto_partitioning_enable = 'true', auto_partitioning_time_unit = 'DAY', auto_partitioning_time_format = 'YYYYMMDD')"
                                    : "");

            execute(conn, new String[] {dropSql, createSql});

            try (HoloClient client = new HoloClient(config)) {
                TableSchema schema = client.getTableSchema(tableName, true);

                for (int i = 0; i < 100; i++) {
                    Put put2 = new Put(schema);
                    put2.setObject("id", partitionColumnCase.putValue);
                    put2.setObject("id2", i);
                    client.put(put2);
                }
                client.flush();
            }
            try (HoloClient client = new HoloClient(config)) {
                TableSchema schema = client.getTableSchema(tableName, true);

                for (int i = 100; i < 200; i++) {
                    Put put2 = new Put(schema);
                    put2.setObject("id", partitionColumnCase.putValue);
                    put2.setObject("id2", i);
                    client.put(put2);
                }
                client.flush();
                Partition partition =
                        ConnectionUtil.getPartition(
                                conn,
                                schema.getSchemaName(),
                                schema.getTableName(),
                                partitionColumnCase.partitionValue,
                                isStr);

                Assert.assertEquals(
                        partition.getTableName(),
                        tableName.replaceAll("\"", "")
                                + "_"
                                + partitionColumnCase.partitionTableNameSuffix);

                int count = 0;
                try (Statement stat = conn.createStatement()) {
                    // 分区子表没同步的话，select结果可能不对
                    ConnectionUtil.refreshMeta(conn, 1000);
                    try (ResultSet rs = stat.executeQuery("select * from " + tableName)) {
                        while (rs.next()) {
                            ++count;
                            Assert.assertEquals(
                                    partitionColumnCase.partitionValue, rs.getObject(1).toString());
                            Assert.assertNull(rs.getObject(3));
                        }
                    }
                }
                Assert.assertEquals(200, count);
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /**
     * DynamicPartition with drop column table
     *
     * <p>Method: put(Put put).
     */
    @Test
    public void testPartition004() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setDynamicPartition(true);
        config.setConnectionMaxIdleMs(10000L);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "\"test_SCHema\".\"holO_client_partition_004\"";
            String dropSql = "drop table if exists " + tableName;
            String createSchema = "create schema if not exists \"test_SCHema\"";
            String createSql =
                    "create table "
                            + tableName
                            + "(a text not null,b text not null, ds text, primary key(a,ds)) partition by list(ds)";

            execute(conn, new String[] {createSchema, dropSql, createSql});

            try {
                TableSchema schema = client.getTableSchema(tableName, true);

                Put put1 = new Put(schema);
                put1.setObject("a", "test");
                put1.setObject("b", "test");
                put1.setObject("ds", "20230920");
                client.put(put1);
                client.flush();

                String dropColumn =
                        "set hg_experimental_enable_drop_column = on;"
                                + "ALTER TABLE IF EXISTS "
                                + tableName
                                + " DROP COLUMN b;";
                execute(conn, new String[] {dropColumn});

                schema = client.getTableSchema(tableName, true);
                Put put2 = new Put(schema);
                put2.setObject("a", "test");
                put2.setObject("ds", "20230921");
                client.put(put2);
                client.flush();

                int count = 0;
                try (Statement stat = conn.createStatement()) {
                    // 分区子表没同步的话，select结果可能不对
                    ConnectionUtil.refreshMeta(conn, 1000);
                    try (ResultSet rs =
                            stat.executeQuery("select * from " + tableName + " order by ds")) {
                        while (rs.next()) {
                            ++count;
                            Assert.assertEquals("test", rs.getObject(1));
                            if (count == 1) {
                                Assert.assertEquals("20230920", rs.getString(2));
                            } else {
                                Assert.assertEquals("20230921", rs.getString(2));
                            }
                        }
                    }
                }
                Assert.assertEquals(2, count);
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** 验证Put,Get,Put(Delete)在写入分区表时的表现 */
    @Test
    public void testPartition005() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setDynamicPartition(true);
        config.setConnectionMaxIdleMs(10000L);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "\"test_SCHema\".\"holO_client_partition_005\"";
            String dropSql = "drop table if exists " + tableName;
            String createSchema = "create schema if not exists \"test_SCHema\"";
            String createSql =
                    "create table "
                            + tableName
                            + "(a text not null,b int not null, ds text, primary key(a,ds)) partition by list(ds)";

            execute(conn, new String[] {createSchema, dropSql, createSql});

            try {
                TableSchema schema = client.getTableSchema(tableName, true);

                for (int i = 0; i < 2; i++) {
                    Put put2 = new Put(schema);
                    put2.setObject("a", "test");
                    put2.setObject("b", i);
                    if (i == 0) {
                        put2.setObject("ds", "20230920");
                    } else {
                        put2.setObject("ds", "20230921");
                    }
                    client.put(put2);
                }
                client.flush();

                Record pk1Record = new Record(schema);
                pk1Record.setObject(0, "test");
                pk1Record.setObject(2, "20230920");
                Record r1 = client.get(new Get(pk1Record)).get();
                Assert.assertEquals("test", r1.getObject("a"));
                Assert.assertEquals(0, r1.getObject("b"));
                Assert.assertEquals("20230920", r1.getObject("ds"));

                Record pk2Record = new Record(schema);
                pk2Record.setObject(0, "test");
                pk2Record.setObject(2, "20230921");
                Record r2 = client.get(new Get(pk2Record)).get();
                Assert.assertEquals("test", r2.getObject("a"));
                Assert.assertEquals(1, r2.getObject("b"));
                Assert.assertEquals("20230921", r2.getObject("ds"));

                RecordScanner scanner =
                        client.scan(new Scan.Builder(schema).addEqualFilter("a", "test").build());
                while (scanner.next()) {
                    Record r = scanner.getRecord();
                    if (r.getObject("ds").equals("20230920")) {
                        Assert.assertEquals(0, r.getObject("b"));
                    } else if (r.getObject("ds").equals("20230921")) {
                        Assert.assertEquals(1, r.getObject("b"));
                    }
                    Assert.assertEquals("test", r.getObject("a"));
                }

                for (int i = 0; i < 2; i++) {
                    Put put2 = new Put(schema);
                    put2.setObject("a", "test");
                    put2.setObject("b", i);
                    if (i == 0) {
                        put2.setObject("ds", "20230920");
                    } else {
                        put2.setObject("ds", "20230921");
                    }
                    put2.getRecord().setType(Put.MutationType.DELETE);
                    client.put(put2);

                    // delete and insert, will be merge in batch
                    Put put3 = new Put(schema);
                    put3.setObject("a", "test");
                    put3.setObject("b", 100);
                    if (i == 0) {
                        put3.setObject("ds", "20230920");
                    } else {
                        put3.setObject("ds", "20230921");
                    }
                    put3.getRecord().setType(Put.MutationType.INSERT);
                    client.put(put3);
                }
                client.flush();
                scanner = client.scan(new Scan.Builder(schema).addEqualFilter("a", "test").build());
                while (scanner.next()) {
                    Record r = scanner.getRecord();
                    if (r.getObject("ds").equals("20230920")) {
                        Assert.assertEquals(100, r.getObject("b"));
                    } else if (r.getObject("ds").equals("20230921")) {
                        Assert.assertEquals(100, r.getObject("b"));
                    }
                    Assert.assertEquals("test", r.getObject("a"));
                }

                for (int i = 0; i < 2; i++) {
                    Put put2 = new Put(schema);
                    put2.setObject("a", "test");
                    put2.setObject("b", i);
                    if (i == 0) {
                        put2.setObject("ds", "20230920");
                    } else {
                        put2.setObject("ds", "20230921");
                    }
                    put2.getRecord().setType(Put.MutationType.DELETE);
                    client.put(put2);
                }
                client.flush();

                scanner = client.scan(new Scan.Builder(schema).addEqualFilter("a", "test").build());
                Assert.assertFalse(scanner.next());
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /**
     * logical partition
     *
     * <p>Method: put(Put put).
     */
    @Test
    public void testLogicalPartition() throws Exception {
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
            String tableName = "\"test_SCHema\".\"holO_client_logical_partition_005\"";
            String dropSql = "drop table if exists " + tableName;
            String createSchema = "create schema if not exists \"test_SCHema\"";
            String createSql =
                    "create table "
                            + tableName
                            + "(a int not null, b text not null, ds text not null, primary key(a,ds)) logical partition by list(ds)"
                            + "with (distribution_key='a')";

            execute(conn, new String[] {createSchema, dropSql, createSql});

            try {
                TableSchema schema = client.getTableSchema(tableName, true);

                Put put1 = new Put(schema);
                put1.setObject("a", 1);
                put1.setObject("b", "test");
                put1.setObject("ds", "20230920");
                client.put(put1);
                client.flush();

                schema = client.getTableSchema(tableName, true);
                Put put2 = new Put(schema);
                put2.setObject("a", 1);
                put2.setObject("b", "test");
                put2.setObject("ds", "20230921");
                client.put(put2);
                client.flush();

                int count = 0;
                try (Statement stat = conn.createStatement()) {
                    try (ResultSet rs =
                            stat.executeQuery("select * from " + tableName + " order by ds")) {
                        while (rs.next()) {
                            ++count;
                            Assert.assertEquals(1, rs.getObject(1));
                            Assert.assertEquals("test", rs.getObject(2));
                            if (count == 1) {
                                Assert.assertEquals("20230920", rs.getString(3));
                            } else {
                                Assert.assertEquals("20230921", rs.getString(3));
                            }
                        }
                    }
                }
                Assert.assertEquals(2, count);
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** logical partition generated column is distribution key Method: put(Put put). */
    @Test
    public void testLogicalPartitionWithGeneratedColumnDistributionKey() throws Exception {
        if (properties == null) {
            return;
        }
        if (holoVersion.compareTo(new HoloVersion(3, 1, 0)) < 0) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_IGNORE);
        config.setDynamicPartition(true);
        config.setConnectionMaxIdleMs(10000L);
        HoloConfig config1 = buildConfig();
        config1.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config1.setDynamicPartition(true);
        config1.setConnectionMaxIdleMs(10000L);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config);
                HoloClient client1 = new HoloClient(config1)) {
            String tableName =
                    "\"test_SCHema\".\"holO_client_logical_partition_generate_Part_005\"";
            String dropSql = "drop table if exists " + tableName;
            String createSchema = "create schema if not exists \"test_SCHema\"";
            String createSql =
                    "create table "
                            + tableName
                            + "(a int not null,b text not null, ts timestamp not null, ds timestamp "
                            + "GENERATED ALWAYS AS (date_trunc('day', ts)) STORED not null, primary key(a,ds)) logical partition by list(ds)"
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
                TableSchema schema = client.getTableSchema(tableName, true);
                Assert.assertTrue(schema.getColumn(3).isGeneratedColumn());

                LocalDateTime now = LocalDateTime.now();
                Put put1 = new Put(schema);
                put1.setObject("a", 1);
                put1.setObject("b", "test");
                put1.setObject("ts", now);
                client.put(put1);
                client.flush();
                Put put2 = new Put(schema);
                put2.setObject("a", 1);
                put2.setObject("b", "test");
                put2.setObject("ts", now.plusDays(1));
                client.put(put2);
                client.flush();

                int count = 0;
                try (Statement stat = conn.createStatement()) {
                    try (ResultSet rs =
                            stat.executeQuery("select * from " + tableName + " order by ds")) {
                        while (rs.next()) {
                            ++count;
                            Assert.assertEquals(1, rs.getInt(1));
                            Assert.assertEquals("test", rs.getObject(2));
                            Timestamp ts = rs.getTimestamp(3);
                            Timestamp ds = rs.getTimestamp(4);
                            ts.setHours(0);
                            ts.setMinutes(0);
                            ts.setSeconds(0);
                            ts.setNanos(0);
                            Assert.assertEquals(ts, ds);
                            if (count == 1) {
                                Assert.assertEquals(
                                        now.truncatedTo(ChronoUnit.DAYS), ds.toLocalDateTime());
                            } else {
                                Assert.assertEquals(
                                        now.plusDays(1).truncatedTo(ChronoUnit.DAYS),
                                        ds.toLocalDateTime());
                            }
                        }
                    }
                }
                Assert.assertEquals(2, count);
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }
}
