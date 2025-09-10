/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.copy.CopyFormat;
import com.alibaba.hologres.client.copy.CopyMode;
import com.alibaba.hologres.client.copy.CopyUtil;
import com.alibaba.hologres.client.copy.in.CopyInOutputStream;
import com.alibaba.hologres.client.copy.in.RecordBinaryOutputStream;
import com.alibaba.hologres.client.copy.in.RecordOutputStream;
import com.alibaba.hologres.client.copy.in.RecordTextOutputStream;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.RecordChecker;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Timestamp;

/** HoloClient defaultValue Tester. */
public class HoloClientDefaultValueTest extends HoloClientTestBase {

    /** INSERT_IGNORE. Method: put(Put put). */
    @Test
    public void testDefaultValue01() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_IGNORE);
        config.setInputNumberAsEpochMsForDatetimeColumn(true);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_default_001";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(\n"
                            + "    \"iD\" int not null,\n"
                            + "    c_str_nullable text ,\n"
                            + "    c_str_d text not null default '-',\n"
                            + "    c_str text not null,\n"
                            + "\n"
                            + "    c_int8_nullable int8 ,\n"
                            + "    c_int8_d int8 not null default 6,\n"
                            + "    c_int8 int8 not null,\n"
                            + "\n"
                            + "    c_numeric_nullable numeric(10,5) ,\n"
                            + "    c_numeric_d numeric(10,5) not null default 3.141,\n"
                            + "    c_numeric numeric(10,5) not null,\n"
                            + "\n"
                            + "\n"
                            + "    c_timestamp_nullable timestamptz ,\n"
                            + "    c_timestamp_d timestamptz not null default now(),\n"
                            + "    c_timestamp timestamptz not null,\n"
                            + "\n"
                            + "    c_timestamp1_nullable timestamptz ,\n"
                            + "    c_timestamp1_d timestamptz not null default '1970-01-01 00:00:00',\n"
                            + "    c_timestamp1 timestamptz not null,\n"
                            + "\n"
                            + "    c_bool_nullable boolean,\n"
                            + "    c_bool_d boolean not null default true,\n"
                            + "    c_bool boolean not null,\n"
                            + "    primary key(\"iD\")\n"
                            + ")";

            execute(conn, new String[] {dropSql, createSql});

            try {
                TableSchema schema = client.getTableSchema(tableName);

                long minTs = System.currentTimeMillis();
                int index = -1;
                Put put = new Put(schema);
                put.setObject(++index, 0);
                put.setObject(++index, "name0");
                put.setObject(++index, "name1");
                put.setObject(++index, "name2");
                put.setObject(++index, 1);
                put.setObject(++index, 2);
                put.setObject(++index, 3);
                put.setObject(++index, "10.2");
                put.setObject(++index, "10.3");
                put.setObject(++index, "10.4");
                put.setObject(++index, 100L);
                put.setObject(++index, 200L);
                put.setObject(++index, 300L);
                put.setObject(++index, 400L);
                put.setObject(++index, 500L);
                put.setObject(++index, 600L);
                put.setObject(++index, true);
                put.setObject(++index, true);
                put.setObject(++index, true);
                client.put(put);

                put = new Put(schema);
                put.setObject(0, 1);
                client.put(put);

                client.flush();

                long maxTs = System.currentTimeMillis();

                Record r = client.get(Get.newBuilder(schema).setPrimaryKey("iD", 0).build()).get();
                index = 0;
                Assert.assertEquals("name0", r.getObject(++index));
                Assert.assertEquals("name1", r.getObject(++index));
                Assert.assertEquals("name2", r.getObject(++index));
                Assert.assertEquals(1L, r.getObject(++index));
                Assert.assertEquals(2L, r.getObject(++index));
                Assert.assertEquals(3L, r.getObject(++index));
                Assert.assertEquals(new BigDecimal("10.20000"), r.getObject(++index));
                Assert.assertEquals(new BigDecimal("10.30000"), r.getObject(++index));
                Assert.assertEquals(new BigDecimal("10.40000"), r.getObject(++index));
                Assert.assertEquals(100L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertEquals(200L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertEquals(300L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertEquals(400L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertEquals(500L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertEquals(600L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertEquals(true, r.getObject(++index));
                Assert.assertEquals(true, r.getObject(++index));
                Assert.assertEquals(true, r.getObject(++index));

                r = client.get(Get.newBuilder(schema).setPrimaryKey("iD", 1).build()).get();
                index = 0;
                Assert.assertNull(r.getObject(++index));
                Assert.assertEquals("-", r.getObject(++index));
                Assert.assertEquals("", r.getObject(++index));
                Assert.assertNull(r.getObject(++index));
                Assert.assertEquals(6L, r.getObject(++index));
                Assert.assertEquals(0L, r.getObject(++index));
                Assert.assertNull(r.getObject(++index));
                Assert.assertEquals(new BigDecimal("3.14100"), r.getObject(++index));
                Assert.assertEquals(new BigDecimal("0.00000"), r.getObject(++index));
                Assert.assertNull(r.getObject(++index));
                long temp = ((Timestamp) r.getObject(++index)).getTime();
                Assert.assertTrue(minTs <= temp && temp <= maxTs);
                Assert.assertEquals(0L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertNull(r.getObject(++index));
                Assert.assertEquals(
                        -8 * 3600L * 1000L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertEquals(0L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertNull(r.getObject(++index));
                Assert.assertEquals(true, r.getObject(++index));
                Assert.assertEquals(false, r.getObject(++index));
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** INSERT_UPDATE. Method: put(Put put). */
    @Test
    public void testDefaultValue02() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setInputNumberAsEpochMsForDatetimeColumn(true);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_default_002";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(\n"
                            + "    \"iD\" int not null,\n"
                            + "    c_str_nullable text ,\n"
                            + "    c_str_d text not null default '-',\n"
                            + "    c_str text not null,\n"
                            + "\n"
                            + "    c_int8_nullable int8 ,\n"
                            + "    c_int8_d int8 not null default 6,\n"
                            + "    c_int8 int8 not null,\n"
                            + "\n"
                            + "    c_numeric_nullable numeric(10,5) ,\n"
                            + "    c_numeric_d numeric(10,5) not null default 3.141,\n"
                            + "    c_numeric numeric(10,5) not null,\n"
                            + "\n"
                            + "\n"
                            + "    c_timestamp_nullable timestamptz ,\n"
                            + "    c_timestamp_d timestamptz not null default now(),\n"
                            + "    c_timestamp timestamptz not null,\n"
                            + "\n"
                            + "    c_timestamp1_nullable timestamptz ,\n"
                            + "    c_timestamp1_d timestamptz not null default '1970-01-01 00:00:00',\n"
                            + "    c_timestamp1 timestamptz not null,\n"
                            + "\n"
                            + "    c_bool_nullable boolean,\n"
                            + "    c_bool_d boolean not null default true,\n"
                            + "    c_bool boolean not null,\n"
                            + "    primary key(\"iD\")\n"
                            + ")";

            execute(conn, new String[] {dropSql, createSql});

            try {
                TableSchema schema = client.getTableSchema(tableName);

                long minTs = System.currentTimeMillis();
                int index = -1;
                Put put = new Put(schema);
                put.setObject(++index, 0);
                put.setObject(++index, "name0");
                put.setObject(++index, "name1");
                put.setObject(++index, "name2");
                put.setObject(++index, 1);
                put.setObject(++index, 2);
                put.setObject(++index, 3);
                put.setObject(++index, "10.2");
                put.setObject(++index, "10.3");
                put.setObject(++index, "10.4");
                put.setObject(++index, 100L);
                put.setObject(++index, 200L);
                put.setObject(++index, 300L);
                put.setObject(++index, 400L);
                put.setObject(++index, 500L);
                put.setObject(++index, 600L);
                put.setObject(++index, true);
                put.setObject(++index, true);
                put.setObject(++index, true);
                client.put(put);

                client.flush();
                put = new Put(schema);
                put.setObject(0, 0);
                put.setObject(3, "new_name");
                client.put(put);

                client.flush();

                Record r = client.get(Get.newBuilder(schema).setPrimaryKey("iD", 0).build()).get();
                index = 0;
                Assert.assertEquals("name0", r.getObject(++index));
                Assert.assertEquals("name1", r.getObject(++index));
                Assert.assertEquals("new_name", r.getObject(++index));
                Assert.assertEquals(1L, r.getObject(++index));
                Assert.assertEquals(2L, r.getObject(++index));
                Assert.assertEquals(3L, r.getObject(++index));
                Assert.assertEquals(new BigDecimal("10.20000"), r.getObject(++index));
                Assert.assertEquals(new BigDecimal("10.30000"), r.getObject(++index));
                Assert.assertEquals(new BigDecimal("10.40000"), r.getObject(++index));
                Assert.assertEquals(100L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertEquals(200L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertEquals(300L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertEquals(400L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertEquals(500L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertEquals(600L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertEquals(true, r.getObject(++index));
                Assert.assertEquals(true, r.getObject(++index));
                Assert.assertEquals(true, r.getObject(++index));

            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** serial. Method: put(Put put). */
    @Test
    public void testDefaultValue03() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_IGNORE);
        config.setInputNumberAsEpochMsForDatetimeColumn(true);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_default_003";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table " + tableName + "(id serial,name text,primary key (id));";

            execute(conn, new String[] {dropSql, createSql});

            try {
                TableSchema schema = client.getTableSchema(tableName);

                Put put = new Put(schema);
                put.setObject(1, "a");
                client.put(put);

                put = new Put(schema);
                put.setObject(1, "b");
                client.put(put);

                client.flush();

                Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
                Assert.assertEquals("a", r.getObject(1));

                r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).build()).get();
                Assert.assertEquals("b", r.getObject(1));

            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    @DataProvider(name = "copyCaseData")
    public Object[][] createData() {
        Object[][] ret = new Object[8][];
        ret[0] = new Object[] {true, OnConflictAction.INSERT_OR_UPDATE, CopyMode.STREAM};
        ret[1] = new Object[] {true, OnConflictAction.INSERT_OR_IGNORE, CopyMode.STREAM};
        ret[2] = new Object[] {false, OnConflictAction.INSERT_OR_UPDATE, CopyMode.STREAM};
        ret[3] = new Object[] {false, OnConflictAction.INSERT_OR_IGNORE, CopyMode.STREAM};
        // bulkload copy目前不支持binary模式
        ret[4] = new Object[] {false, OnConflictAction.INSERT_OR_UPDATE, CopyMode.BULK_LOAD};
        ret[5] = new Object[] {false, OnConflictAction.INSERT_OR_IGNORE, CopyMode.BULK_LOAD};
        ret[6] =
                new Object[] {
                    false, OnConflictAction.INSERT_OR_UPDATE, CopyMode.BULK_LOAD_ON_CONFLICT
                };
        ret[7] =
                new Object[] {
                    false, OnConflictAction.INSERT_OR_IGNORE, CopyMode.BULK_LOAD_ON_CONFLICT
                };
        return ret;
    }

    /** copy Method: putRecord(Record record). */
    @Test(dataProvider = "copyCaseData")
    public void testDefaultValue04(
            boolean binary, OnConflictAction onConflictAction, CopyMode copyMode) throws Exception {
        if (properties == null) {
            return;
        }
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(buildConfig())) {
            String tableName = "holo_client_default_004";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(\n"
                            + "    \"iD\" int not null,\n"
                            + "    c_str_nullable text ,\n"
                            + "    c_str_d text not null default '-',\n"
                            + "    c_str text not null,\n"
                            + "\n"
                            + "    c_int8_nullable int8 ,\n"
                            + "    c_int8_d int8 not null default 6,\n"
                            + "    c_int8 int8 not null,\n"
                            + "\n"
                            + "    c_numeric_nullable numeric(10,5) ,\n"
                            + "    c_numeric_d numeric(10,5) not null default 3.141,\n"
                            + "    c_numeric numeric(10,5) not null,\n"
                            + "\n"
                            + "    c_timestamp_nullable timestamptz ,\n"
                            + "    c_timestamp_d timestamptz not null default now(),\n"
                            + "    c_timestamp timestamptz not null,\n"
                            + "\n"
                            + "    c_timestamp1_nullable timestamptz ,\n"
                            + "    c_timestamp1_d timestamptz not null default '1970-01-01 00:00:00',\n"
                            + "    c_timestamp1 timestamptz not null,\n"
                            + "\n"
                            + "    c_bool_nullable boolean,\n"
                            + "    c_bool_d boolean not null default true,\n"
                            + "    c_bool boolean not null,\n"
                            + "\n"
                            + "    c_serial serial,\n"
                            + "    primary key(\"iD\")\n"
                            + ")";
            String setCopyOnConflictGucSql = "set hg_experimental_copy_enable_on_conflict = on;";

            execute(conn, new String[] {dropSql, createSql, setCopyOnConflictGucSql});

            try {
                PgConnection pgConn = conn.unwrap(PgConnection.class);

                HoloVersion version = ConnectionUtil.getHoloVersion(pgConn);
                if (copyMode == CopyMode.BULK_LOAD_ON_CONFLICT
                        && version.compareTo(new HoloVersion("3.1.0")) < 0) {
                    // copy on conflict need all columns below 3.1.0
                    return;
                }
                TableName tn = TableName.valueOf(tableName);
                ConnectionUtil.checkMeta(pgConn, version, tn.getFullName(), 120);

                TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);
                CopyManager copyManager = new CopyManager(conn.unwrap(PgConnection.class));
                String copySql = null;
                OutputStream os = null;
                RecordOutputStream ros = null;

                long minTs = System.currentTimeMillis();
                int index = -1;
                Record record = new Record(schema);
                record.setObject(++index, 0);
                record.setObject(++index, "name0");
                record.setObject(++index, "name1");
                record.setObject(++index, "name2");
                record.setObject(++index, 1);
                record.setObject(++index, 2);
                record.setObject(++index, 3);
                record.setObject(++index, new BigDecimal("10.2"));
                record.setObject(++index, new BigDecimal("10.3"));
                record.setObject(++index, new BigDecimal("10.4"));
                record.setObject(++index, new Timestamp(100L));
                record.setObject(++index, new Timestamp(200L));
                record.setObject(++index, new Timestamp(300L));
                record.setObject(++index, new Timestamp(400L));
                record.setObject(++index, new Timestamp(500L));
                record.setObject(++index, new Timestamp(600L));
                record.setObject(++index, true);
                record.setObject(++index, true);
                record.setObject(++index, true);

                Record record2 = new Record(schema);
                record2.setObject(0, 1);
                CopyUtil.prepareRecordForCopy(record2);

                RecordChecker.check(record);
                copySql =
                        CopyUtil.buildCopyInSql(
                                record,
                                binary ? CopyFormat.BINARY : CopyFormat.CSV,
                                onConflictAction,
                                copyMode);
                LOG.info("copySql : {}", copySql);
                os = new CopyInOutputStream(copyManager.copyIn(copySql));
                ros =
                        binary
                                ? new RecordBinaryOutputStream(
                                        os,
                                        schema,
                                        pgConn.unwrap(PgConnection.class),
                                        1024 * 1024 * 10)
                                : new RecordTextOutputStream(
                                        os,
                                        schema,
                                        pgConn.unwrap(PgConnection.class),
                                        1024 * 1024 * 10);
                ros.putRecord(record);
                ros.putRecord(record2);
                ros.close();

                long maxTs = System.currentTimeMillis();

                Record r = client.get(Get.newBuilder(schema).setPrimaryKey("iD", 0).build()).get();
                index = 0;
                Assert.assertEquals("name0", r.getObject(++index));
                Assert.assertEquals("name1", r.getObject(++index));
                Assert.assertEquals("name2", r.getObject(++index));
                Assert.assertEquals(1L, r.getObject(++index));
                Assert.assertEquals(2L, r.getObject(++index));
                Assert.assertEquals(3L, r.getObject(++index));
                Assert.assertEquals(new BigDecimal("10.20000"), r.getObject(++index));
                Assert.assertEquals(new BigDecimal("10.30000"), r.getObject(++index));
                Assert.assertEquals(new BigDecimal("10.40000"), r.getObject(++index));
                Assert.assertEquals(100L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertEquals(200L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertEquals(300L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertEquals(400L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertEquals(500L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertEquals(600L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertEquals(true, r.getObject(++index));
                Assert.assertEquals(true, r.getObject(++index));
                Assert.assertEquals(true, r.getObject(++index));
                Assert.assertNotEquals(null, r.getObject(++index));

                r = client.get(Get.newBuilder(schema).setPrimaryKey("iD", 1).build()).get();
                index = 0;
                Assert.assertNull(r.getObject(++index));
                Assert.assertEquals("-", r.getObject(++index));
                Assert.assertEquals("", r.getObject(++index));
                Assert.assertNull(r.getObject(++index));
                Assert.assertEquals(6L, r.getObject(++index));
                Assert.assertEquals(0L, r.getObject(++index));
                Assert.assertNull(r.getObject(++index));
                Assert.assertEquals(new BigDecimal("3.14100"), r.getObject(++index));
                Assert.assertEquals(new BigDecimal("0.00000"), r.getObject(++index));
                Assert.assertNull(r.getObject(++index));
                long temp = ((Timestamp) r.getObject(++index)).getTime();
                Assert.assertTrue(minTs <= temp && temp <= maxTs);
                Assert.assertEquals(0L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertNull(r.getObject(++index));
                Assert.assertEquals(
                        -8 * 3600L * 1000L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertEquals(0L, ((Timestamp) r.getObject(++index)).getTime());
                Assert.assertNull(r.getObject(++index));
                Assert.assertEquals(true, r.getObject(++index));
                Assert.assertEquals(false, r.getObject(++index));
                Assert.assertNotEquals(null, r.getObject(++index));
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }
}
