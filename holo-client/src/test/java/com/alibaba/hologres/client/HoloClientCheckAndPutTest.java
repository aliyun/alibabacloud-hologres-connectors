package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.exception.HoloClientWithDetailsException;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.checkandput.CheckCompareOp;
import com.alibaba.hologres.client.utils.DataTypeTestUtil;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import static com.alibaba.hologres.client.utils.DataTypeTestUtil.FIXED_PLAN_TYPE_DATA;

/**
 * HoloClientCheckAndPut Tester.
 *
 * @version 1.0
 * @since
 *     <pre>12月, 2023</pre>
 */
public class HoloClientCheckAndPutTest extends HoloClientTestBase {

    /** 两个参数： 第一个boolean表示使用values还是unnest方式拼sql 第二个boolean表示使用insertOrUpdate还是insertOrReplace. */
    @DataProvider(name = "useLegacyPutHandler")
    public Object[][] createData() {
        Object[][] ret = new Object[4][];
        ret[0] = new Object[] {true, true};
        ret[1] = new Object[] {false, true};
        ret[2] = new Object[] {true, false};
        ret[3] = new Object[] {false, false};
        return ret;
    }

    @Override
    protected HoloConfig buildConfig() {
        HoloConfig config = super.buildConfig();
        config.setUseFixedFe(true);
        return config;
    }

    /** check failed. Method: checkAndPut(CheckAndPut put). */
    @Test
    public void testCheckAndPutException() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_batch_001";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text,modify_time timestamptz,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            assertThrowsWithMessage(
                    HoloClientException.class,
                    "CheckAndPut not supports writeMode insertOrIgnore.",
                    () -> {
                        HoloConfig config1 = buildConfig();
                        config1.setOnConflictAction(OnConflictAction.INSERT_OR_IGNORE);
                        try (HoloClient client1 = new HoloClient(config1)) {
                            CheckAndPut put =
                                    new CheckAndPut(
                                            schema,
                                            "modify_time",
                                            CheckCompareOp.GREATER,
                                            null,
                                            "1970-01-01 00:08:00");
                            put.setObject(0, 0);
                            put.setObject(1, "name0");
                            put.setObject(2, "2020-01-01 00:00:00");
                            client1.checkAndPut(put);
                        }
                    });

            assertThrowsWithMessage(
                    HoloClientWithDetailsException.class,
                    "checkColumn not_exist_column is not exists in table",
                    () -> {
                        CheckAndPut put =
                                new CheckAndPut(
                                        schema,
                                        "not_exist_column",
                                        CheckCompareOp.GREATER_OR_EQUAL,
                                        null,
                                        "1970-01-01 00:08:00");
                        put.setObject(0, 0);
                        put.setObject(1, "name0");
                        put.setObject(2, "2020-01-01 00:00:00");
                        client.checkAndPut(put);
                    });

            assertThrowsWithMessage(
                    HoloClientWithDetailsException.class,
                    "checkColumn modify_time should be set when not set checkValue.",
                    () -> {
                        CheckAndPut put =
                                new CheckAndPut(
                                        schema,
                                        "modify_time",
                                        CheckCompareOp.GREATER,
                                        null,
                                        "1970-01-01 00:08:00");
                        put.setObject(0, 0);
                        client.checkAndPut(put);
                    });

            assertThrowsWithMessage(
                    HoloClientWithDetailsException.class,
                    "checkColumn modify_time should be set not null when not set checkValue and mutationType is delete.",
                    () -> {
                        CheckAndPut put =
                                new CheckAndPut(
                                        schema,
                                        "modify_time",
                                        CheckCompareOp.GREATER,
                                        null,
                                        "1970-01-01 00:08:00");
                        put.setObject(0, 0);
                        put.setObject(2, null);
                        put.getRecord().setType(Put.MutationType.DELETE);
                        client.checkAndPut(put);
                    });
        }
    }

    /** INSERT. Method: checkAndPut(CheckAndPut put). */
    @Test(dataProvider = "useLegacyPutHandler")
    public void testCheckAndPutBatch002(boolean useLegacyPutHandler, boolean update)
            throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(
                update ? OnConflictAction.INSERT_OR_UPDATE : OnConflictAction.INSERT_OR_REPLACE);
        config.setForceFlushInterval(100000);
        config.setUseLegacyPutHandler(useLegacyPutHandler);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_batch_002";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text,modify_time timestamptz, address text,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            CheckAndPut put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            null,
                            "1970-01-01 00:08:00");
            put.setObject(0, 0);
            put.setObject(1, "name0");
            put.setObject(2, "2020-01-01 00:00:00");
            put.setObject(3, "address0");
            client.checkAndPut(put);
            put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            null,
                            "1970-01-01 00:08:00");
            put.setObject(0, 1);
            put.setObject(1, "name1");
            put.setObject(2, "2020-01-01 00:00:00");
            put.setObject(3, "address1");
            client.checkAndPut(put);
            client.flush();

            put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            null,
                            "1970-01-01 00:08:00");
            put.setObject(0, 0);
            put.setObject(1, "name0_new");
            put.setObject(2, "2021-01-01 00:00:00");
            client.checkAndPut(put);
            put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            null,
                            "1970-01-01 00:08:00");
            put.setObject(0, 1);
            put.setObject(1, "name1_new");
            put.setObject(2, "2019-01-01 00:00:00");
            client.checkAndPut(put);
            client.flush();

            // pk = 0, check通过，更新
            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
            Assert.assertEquals("name0_new", r.getObject(1));
            Assert.assertEquals(Timestamp.valueOf("2021-01-01 00:00:00.0"), r.getObject(2));
            if (update) {
                Assert.assertEquals("address0", r.getObject(3));
            } else {
                Assert.assertNull(r.getObject(3));
            }

            // pk = 1, check未通过，不更新
            r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            Assert.assertEquals("name1", r.getObject(1));
            Assert.assertEquals(Timestamp.valueOf("2020-01-01 00:00:00.0"), r.getObject(2));
            Assert.assertEquals("address1", r.getObject(3));

            execute(conn, new String[] {dropSql});
        }
    }

    /** INSERT 表中已存在的checkColumn字段是null，nullValue生效 Method: checkAndPut(CheckAndPut put). */
    @Test(dataProvider = "useLegacyPutHandler")
    public void testCheckAndPutBatch003(boolean useLegacyPutHandler, boolean update)
            throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(
                update ? OnConflictAction.INSERT_OR_UPDATE : OnConflictAction.INSERT_OR_REPLACE);
        config.setForceFlushInterval(100000);
        config.setUseLegacyPutHandler(useLegacyPutHandler);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_batch_003";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text,modify_time timestamptz, address text,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            // 将 timestamp字段的 null 视为 2020-01-01 00:00:00
            CheckAndPut put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            null,
                            "2020-01-01 00:00:00");
            put.setObject(0, 0);
            put.setObject(1, "name0");
            put.setObject(2, null);
            put.setObject(3, "address0");
            client.checkAndPut(put);
            put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            null,
                            "2020-01-01 00:00:00");
            put.setObject(0, 1);
            put.setObject(1, "name1");
            put.setObject(2, null);
            put.setObject(3, "address1");
            client.checkAndPut(put);
            client.flush();

            put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            null,
                            "2020-01-01 00:00:00");
            put.setObject(0, 0);
            put.setObject(1, "name0_new");
            put.setObject(2, "2021-01-01 00:00:00");
            client.checkAndPut(put);
            put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            null,
                            "2020-01-01 00:00:00");
            put.setObject(0, 1);
            put.setObject(1, "name1_new");
            put.setObject(2, "2019-01-01 00:00:00");
            client.checkAndPut(put);
            client.flush();

            // pk = 0, check通过，更新
            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
            Assert.assertEquals("name0_new", r.getObject(1));
            Assert.assertEquals(Timestamp.valueOf("2021-01-01 00:00:00.0"), r.getObject(2));
            if (update) {
                Assert.assertEquals("address0", r.getObject(3));
            } else {
                Assert.assertNull(r.getObject(3));
            }

            // pk = 1, check未通过，不更新
            r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            Assert.assertEquals("name1", r.getObject(1));
            Assert.assertEquals(null, r.getObject(2));
            Assert.assertEquals("address1", r.getObject(3));

            execute(conn, new String[] {dropSql});
        }
    }

    /** INSERT 传入了checkValue，相当于和常量比较 Method: checkAndPut(CheckAndPut put). */
    @Test(dataProvider = "useLegacyPutHandler")
    public void testCheckAndPutBatch004(boolean useLegacyPutHandler, boolean update)
            throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(
                update ? OnConflictAction.INSERT_OR_UPDATE : OnConflictAction.INSERT_OR_REPLACE);
        config.setForceFlushInterval(100000);
        config.setUseLegacyPutHandler(useLegacyPutHandler);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_batch_004";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text,modify_time timestamptz, address text,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            CheckAndPut put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            "2020-01-01 00:00:00",
                            "1970-01-01 00:08:00");
            put.setObject(0, 0);
            put.setObject(1, "name0");
            put.setObject(2, "2019-01-01 00:00:00");
            put.setObject(3, "address0");
            client.checkAndPut(put); // insert
            put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            "2020-01-01 00:00:00",
                            "1970-01-01 00:08:00");
            put.setObject(0, 1);
            put.setObject(1, "name1");
            put.setObject(2, null);
            put.setObject(3, "address1");
            client.checkAndPut(put); // insert
            client.flush();

            put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            "2020-01-01 00:00:00",
                            "1970-01-01 00:08:00");
            put.setObject(0, 0);
            put.setObject(1, "name0_new");
            put.setObject(2, "2020-01-01 00:00:00"); // insert
            client.checkAndPut(put);
            put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            "2020-01-01 00:00:00",
                            "1970-01-01 00:08:00");
            put.setObject(0, 1);
            put.setObject(1, "name1_new");
            put.setObject(2, "2019-01-01 00:00:00"); // insert
            client.checkAndPut(put);
            client.flush();

            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
            Assert.assertEquals("name0_new", r.getObject(1));
            Assert.assertEquals(Timestamp.valueOf("2020-01-01 00:00:00.0"), r.getObject(2));
            if (update) {
                Assert.assertEquals("address0", r.getObject(3));
            } else {
                Assert.assertNull(r.getObject(3));
            }

            r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            Assert.assertEquals("name1_new", r.getObject(1));
            Assert.assertEquals(Timestamp.valueOf("2019-01-01 00:00:00.0"), r.getObject(2));
            if (update) {
                Assert.assertEquals("address1", r.getObject(3));
            } else {
                Assert.assertNull(r.getObject(3));
            }
            execute(conn, new String[] {dropSql});
        }
    }

    /** INSERT 传入了不同的checkValue，不会攒批 Method: checkAndPut(CheckAndPut put). */
    @Test(dataProvider = "useLegacyPutHandler")
    public void testCheckAndPutBatch005(boolean useLegacyPutHandler, boolean update)
            throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(
                update ? OnConflictAction.INSERT_OR_UPDATE : OnConflictAction.INSERT_OR_REPLACE);
        config.setForceFlushInterval(100000);
        config.setUseLegacyPutHandler(useLegacyPutHandler);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_batch_005";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text,modify_time timestamptz, address text,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            CheckAndPut put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            "2020-01-01 00:00:00",
                            "1970-01-01 00:08:00");
            put.setObject(0, 0);
            put.setObject(1, "name0");
            put.setObject(2, "2020-01-01 00:00:00");
            put.setObject(3, "address0");
            client.checkAndPut(put);

            // 如果checkValue不同（本质上是CheckAndPutCondition不同），会强制flush之前的攒批
            put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            "2021-01-01 00:00:00",
                            "1970-01-01 00:08:00");
            put.setObject(0, 0);
            put.setObject(1, "name0_new");
            put.setObject(2, "2021-01-01 00:00:00");
            put.setObject(3, "address0_new");
            client.checkAndPut(put);
            put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            "2022-01-01 00:00:00",
                            "1970-01-01 00:08:00");
            put.setObject(0, 0);
            put.setObject(1, "name0_new_new");
            put.setObject(2, "2022-01-01 00:00:00");
            client.checkAndPut(put);
            client.flush();

            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
            Assert.assertEquals("name0_new_new", r.getObject(1));
            Assert.assertEquals(Timestamp.valueOf("2022-01-01 00:00:00.0"), r.getObject(2));
            if (update) {
                Assert.assertEquals("address0_new", r.getObject(3));
            } else {
                Assert.assertNull(r.getObject(3));
            }

            execute(conn, new String[] {dropSql});
        }
    }

    /** DELETE checkValue check old value Method: checkAndPut(CheckAndPut put). */
    @Test
    public void testCheckAndPutBatch006() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
        config.setForceFlushInterval(100000);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_batch_006";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text,modify_time timestamptz, address text,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            Put put = new Put(schema);
            put.setObject(0, 0);
            put.setObject(1, "name0");
            put.setObject(2, "2019-01-01 00:00:00");
            put.setObject(3, "address0");
            client.put(put);
            put = new Put(schema);
            put.setObject(0, 1);
            put.setObject(1, "name1");
            put.setObject(2, "2021-01-01 00:00:00");
            put.setObject(3, "address1");
            client.put(put);
            client.flush();
            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
            Assert.assertEquals("name0", r.getObject(1));
            r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            Assert.assertEquals("name1", r.getObject(1));

            // delete只有checkValue设置且相同时才能攒批
            /* 预期执行的sql：
            delete from "public"."holo_client_check_and_put_batch_006"
            where (id=$1 and '2020-01-01 00:00:00'::timestamptz > coalesce(modify_time, '1970-01-01 00:08:00'::timestamptz))
            OR (id=$2 and '2020-01-01 00:00:00'::timestamptz > coalesce(modify_time, '1970-01-01 00:08:00'::timestamptz))
            */
            CheckAndPut cput =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            "2020-01-01 00:00:00",
                            "1970-01-01 00:08:00");
            cput.setObject(0, 0);
            cput.getRecord().setType(Put.MutationType.DELETE);
            client.checkAndPut(cput);
            cput =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            "2020-01-01 00:00:00",
                            "1970-01-01 00:08:00");
            cput.setObject(0, 1);
            cput.getRecord().setType(Put.MutationType.DELETE);
            client.checkAndPut(cput);
            client.flush();
            // pk = 0 check成功，被删除； pk = 1 没有check成功
            r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
            Assert.assertNull(r);
            r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            Assert.assertEquals("name1", r.getObject(1));

            execute(conn, new String[] {dropSql});
        }
    }

    /** DELETE, new value check old value Method: checkAndPut(CheckAndPut put). */
    @Test
    public void testCheckAndPutBatch007() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
        config.setForceFlushInterval(100000);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_batch_007";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text,modify_time timestamptz, address text,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            CheckAndPut put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            null,
                            "1970-01-01 00:08:00");
            put.setObject(0, 0);
            put.setObject(1, "name0");
            put.setObject(2, "2020-01-01 00:00:00");
            put.setObject(3, "address0");
            client.checkAndPut(put);
            put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            null,
                            "1970-01-01 00:08:00");
            put.setObject(0, 1);
            put.setObject(1, "name1");
            put.setObject(2, "2020-01-01 00:00:00");
            put.setObject(3, "address1");
            client.checkAndPut(put);
            client.flush();
            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
            Assert.assertEquals("name0", r.getObject(1));
            r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            Assert.assertEquals("name1", r.getObject(1));

            try {
                put =
                        new CheckAndPut(
                                schema,
                                "modify_time",
                                CheckCompareOp.GREATER,
                                null,
                                "1970-01-01 00:08:00");
                put.setObject(0, 0);
                put.setObject(2, null);
                put.getRecord().setType(Put.MutationType.DELETE);
                client.checkAndPut(put);
                client.flush();
            } catch (HoloClientException e) {
                e.printStackTrace();
                // 其他error message则测试失败
                Assert.assertTrue(
                        e.getMessage()
                                .contains(
                                        "checkColumn modify_time should be set not null when not set checkValue and mutationType is delete."));
            }

            // delete只有checkValue设置且相同时才能攒批（否则无法走fixed plan），这里不会攒批的
            /* 预期执行的sql是如下两条：
            delete from "public"."holo_client_check_and_put_batch_010" where (id=$1 and '2019-01-01 00:00:00'::timestamptz > coalesce(modify_time, '1970-01-01 00:08:00'::timestamptz))
            delete from "public"."holo_client_check_and_put_batch_010" where (id=$1 and '2021-01-01 00:00:00'::timestamptz > coalesce(modify_time, '1970-01-01 00:08:00'::timestamptz))
            */
            put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            null,
                            "1970-01-01 00:08:00");
            put.setObject(0, 0);
            put.setObject(2, "2019-01-01 00:00:00");
            put.getRecord().setType(Put.MutationType.DELETE);
            client.checkAndPut(put);
            put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            null,
                            "1970-01-01 00:08:00");
            put.setObject(0, 1);
            put.setObject(2, "2021-01-01 00:00:00");
            put.getRecord().setType(Put.MutationType.DELETE);
            client.checkAndPut(put);
            client.flush();
            // pk = 0 没有check成功，所以没有删除； pk = 1 check成功，被删除
            r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
            Assert.assertEquals("name0", r.getObject(1));
            r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            Assert.assertNull(r);

            execute(conn, new String[] {dropSql});
        }
    }

    /** DELETE, checkValue Method: checkAndPut(CheckAndPut put). */
    @Test
    public void testCheckAndPutBatch008() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setForceFlushInterval(100000);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_batch_008";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text,modify_time timestamptz, address text,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            CheckAndPut put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            null,
                            "1970-01-01 00:08:00");
            put.setObject(0, 0);
            put.setObject(1, "name0");
            put.setObject(2, "2020-01-01 00:00:00");
            put.setObject(3, "address0");
            client.checkAndPut(put);
            client.flush();
            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
            Assert.assertEquals("name0", r.getObject(1));

            // 只有表中已有数据满足 < checkValue 时才进行删除
            // delete from "public"."holo_client_check_and_put_008" where (id=$1 and '2020-01-01
            // 00:00:00'::timestamptz > coalesce(modify_time, '1970-01-01 00:08:00'::timestamptz))
            put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            "2020-01-01 00:00:00",
                            "1970-01-01 00:08:00");
            put.setObject(0, 0);
            put.getRecord().setType(Put.MutationType.DELETE);
            client.checkAndPut(put);
            client.flush();
            r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
            Assert.assertEquals("name0", r.getObject(1));

            // 第二条checkValue满足，最终数据被删除
            put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            "2020-01-01 00:00:00",
                            "1970-01-01 00:08:00");
            put.setObject(0, 0);
            put.getRecord().setType(Put.MutationType.DELETE);
            client.checkAndPut(put);
            // delete from "public"."holo_client_check_and_put_008" where (id=$1 and '2021-01-01
            // 00:00:00'::timestamptz > coalesce(modify_time, '1970-01-01 00:08:00'::timestamptz))
            put =
                    new CheckAndPut(
                            schema,
                            "modify_time",
                            CheckCompareOp.GREATER,
                            "2021-01-01 00:00:00",
                            "1970-01-01 00:08:00");
            put.setObject(0, 0);
            put.getRecord().setType(Put.MutationType.DELETE);
            client.checkAndPut(put);
            client.flush();

            r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
            Assert.assertNull(r);

            execute(conn, new String[] {dropSql});
        }
    }

    private void initData(HoloClient client, TableSchema schema) throws HoloClientException {
        initData(client, schema, 0);
    }

    private void initData(HoloClient client, TableSchema schema, Object initValue)
            throws HoloClientException {
        Put put = new Put(schema);
        put.setObject("id", 0);
        put.setObject("name", "name0");
        put.setObject("age", initValue);
        client.put(put);
        put = new Put(schema);
        put.setObject("id", 1);
        put.setObject("name", "name0");
        put.setObject("age", initValue);
        client.put(put);
        put = new Put(schema);
        put.setObject("id", 2);
        put.setObject("name", "name0");
        put.setObject("age", initValue);
        client.put(put);
        put = new Put(schema);
        put.setObject("id", 3);
        put.setObject("name", "name0");
        put.setObject("age", null);
        client.put(put);
        put = new Put(schema);
        put.setObject("id", 4);
        put.setObject("name", "name0");
        put.setObject("age", null);
        client.put(put);
        put = new Put(schema);
        put.setObject("id", 5);
        put.setObject("name", "name0");
        put.setObject("age", null);
        client.put(put);
        put = new Put(schema);
        put.setObject("id", 6);
        put.setObject("name", "name0");
        put.setObject("age", null);
        client.put(put);
        client.flush();
    }

    /** Operation: greater or equal. check and put without checkValue. */
    @Test
    public void testGreaterOrEqual001() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_greater_or_equal_001";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text, age int ,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            try {
                initData(client, schema);
                {
                    Put put = new Put(schema);
                    put.setObject("id", 0);
                    put.setObject("name", "name1");
                    put.setObject("age", -1); // less, will not update
                    client.checkAndPut("age", CheckCompareOp.GREATER_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 1);
                    put.setObject("name", "name1");
                    put.setObject("age", 0); // equal
                    client.checkAndPut("age", CheckCompareOp.GREATER_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 2);
                    put.setObject("name", "name1");
                    put.setObject("age", 1); // greater
                    client.checkAndPut("age", CheckCompareOp.GREATER_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 3);
                    put.setObject("name", "name1");
                    put.setObject("age", -1); // coalesce(null as 0), same as less, will not update
                    client.checkAndPut("age", CheckCompareOp.GREATER_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 4);
                    put.setObject("name", "name1");
                    put.setObject("age", 0); // coalesce(null as 0), same as equal
                    client.checkAndPut("age", CheckCompareOp.GREATER_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 5);
                    put.setObject("name", "name1");
                    put.setObject("age", 1); // coalesce(null as 0), same as greater
                    client.checkAndPut("age", CheckCompareOp.GREATER_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 6);
                    put.setObject("name", "name1");
                    put.setObject(
                            "age", null); // coalesce(null as 0), new value is null, will not update
                    client.checkAndPut("age", CheckCompareOp.GREATER_OR_EQUAL, null, 0, put);
                    client.flush();

                    Record record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(0, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(0, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(1, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 3).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(null, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 4).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(0, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 5).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(1, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 6).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(null, record.getObject("age"));
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** Operation: greater or equal. check and delete without checkValue */
    @Test
    public void testGreaterOrEqual002() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseFixedFe(true);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_greater_or_equal_002";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text, age int ,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            try {
                initData(client, schema);
                {
                    Put put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 0);
                    put.setObject("age", -1); // less, will not delete
                    client.checkAndPut("age", CheckCompareOp.GREATER_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 1);
                    put.setObject("age", 0); // equal
                    client.checkAndPut("age", CheckCompareOp.GREATER_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 2);
                    put.setObject("age", 1); // greater
                    client.checkAndPut("age", CheckCompareOp.GREATER_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 3);
                    put.setObject("age", -1); // coalesce(null as 0), same as less, will not delete
                    client.checkAndPut("age", CheckCompareOp.GREATER_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 4);
                    put.setObject("age", 0); // coalesce(null as 0), same as equal
                    client.checkAndPut("age", CheckCompareOp.GREATER_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 5);
                    put.setObject("age", 1); // coalesce(null as 0), same as greater
                    client.checkAndPut("age", CheckCompareOp.GREATER_OR_EQUAL, null, 0, put);
                    client.flush();

                    Record record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
                    Assert.assertNotNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
                    Assert.assertNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).build()).get();
                    Assert.assertNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 3).build()).get();
                    Assert.assertNotNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 4).build()).get();
                    Assert.assertNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 5).build()).get();
                    Assert.assertNull(record);
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** Operation: greater. check and put without checkValue */
    @Test
    public void testGreater001() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseFixedFe(true);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_greater_001";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text, age int ,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            try {
                initData(client, schema);
                {
                    Put put = new Put(schema);
                    put.setObject("id", 0);
                    put.setObject("name", "name1");
                    put.setObject("age", -1); // less, will not update
                    client.checkAndPut("age", CheckCompareOp.GREATER, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 1);
                    put.setObject("name", "name1");
                    put.setObject("age", 0); // equal, will not update
                    client.checkAndPut("age", CheckCompareOp.GREATER, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 2);
                    put.setObject("name", "name1");
                    put.setObject("age", 1); // greater
                    client.checkAndPut("age", CheckCompareOp.GREATER, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 3);
                    put.setObject("name", "name1");
                    put.setObject("age", -1); // coalesce(null as 0), same as less, will not update
                    client.checkAndPut("age", CheckCompareOp.GREATER, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 4);
                    put.setObject("name", "name1");
                    put.setObject("age", 0); // coalesce(null as 0), same as equal, will not update
                    client.checkAndPut("age", CheckCompareOp.GREATER, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 5);
                    put.setObject("name", "name1");
                    put.setObject("age", 1); // coalesce(null as 0), same as greater
                    client.checkAndPut("age", CheckCompareOp.GREATER, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 6);
                    put.setObject("name", "name1");
                    put.setObject(
                            "age", null); // coalesce(null as 0), new value is null, will not update
                    client.checkAndPut("age", CheckCompareOp.GREATER, null, 0, put);
                    client.flush();

                    Record record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(0, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(0, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(1, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 3).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(null, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 4).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(null, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 5).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(1, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 6).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(null, record.getObject("age"));
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** Operation: greater. check and delete without checkValue */
    @Test
    public void testGreater002() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseFixedFe(true);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_greater_002";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text, age int ,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            try {
                initData(client, schema);
                {
                    Put put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 0);
                    put.setObject("age", -1); // less, will not delete
                    client.checkAndPut("age", CheckCompareOp.GREATER, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 1);
                    put.setObject("age", 0); // equal, will not delete
                    client.checkAndPut("age", CheckCompareOp.GREATER, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 2);
                    put.setObject("age", 1); // greater
                    client.checkAndPut("age", CheckCompareOp.GREATER, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 3);
                    put.setObject("age", -1); // coalesce(null as 0), same as less, will not delete
                    client.checkAndPut("age", CheckCompareOp.GREATER, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 4);
                    put.setObject("age", 0); // coalesce(null as 0), same as equal, will not delete
                    client.checkAndPut("age", CheckCompareOp.GREATER, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 5);
                    put.setObject("age", 1); // coalesce(null as 0), same as greater
                    client.checkAndPut("age", CheckCompareOp.GREATER, null, 0, put);
                    client.flush();

                    Record record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
                    Assert.assertNotNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
                    Assert.assertNotNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).build()).get();
                    Assert.assertNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 3).build()).get();
                    Assert.assertNotNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 4).build()).get();
                    Assert.assertNotNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 5).build()).get();
                    Assert.assertNull(record);
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** Operation: less or equal. check and put without checkValue. */
    @Test
    public void testLessOrEqual001() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_lessor_or_equal_001";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text, age int ,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            try {
                initData(client, schema);
                {
                    Put put = new Put(schema);
                    put.setObject("id", 0);
                    put.setObject("name", "name1");
                    put.setObject("age", -1); // less
                    client.checkAndPut("age", CheckCompareOp.LESS_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 1);
                    put.setObject("name", "name1");
                    put.setObject("age", 0); // equal
                    client.checkAndPut("age", CheckCompareOp.LESS_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 2);
                    put.setObject("name", "name1");
                    put.setObject("age", 1); // greater, will not update
                    client.checkAndPut("age", CheckCompareOp.LESS_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 3);
                    put.setObject("name", "name1");
                    put.setObject("age", -1); // coalesce(null as 0), same as less
                    client.checkAndPut("age", CheckCompareOp.LESS_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 4);
                    put.setObject("name", "name1");
                    put.setObject("age", 0); // coalesce(null as 0), same as equal
                    client.checkAndPut("age", CheckCompareOp.LESS_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 5);
                    put.setObject("name", "name1");
                    put.setObject(
                            "age", 1); // coalesce(null as 0), same as greater, will not update
                    client.checkAndPut("age", CheckCompareOp.LESS_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 6);
                    put.setObject("name", "name1");
                    put.setObject(
                            "age", null); // coalesce(null as 0), new value is null, will not update
                    client.checkAndPut("age", CheckCompareOp.LESS_OR_EQUAL, null, 0, put);
                    client.flush();

                    Record record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(-1, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(0, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(0, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 3).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(-1, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 4).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(0, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 5).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(null, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 6).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(null, record.getObject("age"));
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** Operation: less or equal. check and delete without checkValue */
    @Test
    public void testLessOrEqual002() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseFixedFe(true);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_lessor_or_equal_002";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text, age int ,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            try {
                initData(client, schema);
                {
                    Put put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 0);
                    put.setObject("age", -1); // less
                    client.checkAndPut("age", CheckCompareOp.LESS_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 1);
                    put.setObject("age", 0); // equal
                    client.checkAndPut("age", CheckCompareOp.LESS_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 2);
                    put.setObject("age", 1); // greater, will not delete
                    client.checkAndPut("age", CheckCompareOp.LESS_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 3);
                    put.setObject("age", -1); // coalesce(null as 0), same as less
                    client.checkAndPut("age", CheckCompareOp.LESS_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 4);
                    put.setObject("age", 0); // coalesce(null as 0), same as equal
                    client.checkAndPut("age", CheckCompareOp.LESS_OR_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 5);
                    put.setObject(
                            "age", 1); // coalesce(null as 0), same as greater, will not delete
                    client.checkAndPut("age", CheckCompareOp.LESS_OR_EQUAL, null, 0, put);
                    client.flush();

                    Record record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
                    Assert.assertNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
                    Assert.assertNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).build()).get();
                    Assert.assertNotNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 3).build()).get();
                    Assert.assertNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 4).build()).get();
                    Assert.assertNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 5).build()).get();
                    Assert.assertNotNull(record);
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** Operation: less. check and put without checkValue */
    @Test
    public void testLess001() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseFixedFe(true);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_less_001";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text, age int ,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            try {
                initData(client, schema);
                {
                    Put put = new Put(schema);
                    put.setObject("id", 0);
                    put.setObject("name", "name1");
                    put.setObject("age", -1); // less
                    client.checkAndPut("age", CheckCompareOp.LESS, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 1);
                    put.setObject("name", "name1");
                    put.setObject("age", 0); // equal, will not update
                    client.checkAndPut("age", CheckCompareOp.LESS, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 2);
                    put.setObject("name", "name1");
                    put.setObject("age", 1); // greater, will not update
                    client.checkAndPut("age", CheckCompareOp.LESS, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 3);
                    put.setObject("name", "name1");
                    put.setObject("age", -1); // coalesce(null as 0), same as less
                    client.checkAndPut("age", CheckCompareOp.LESS, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 4);
                    put.setObject("name", "name1");
                    put.setObject("age", 0); // coalesce(null as 0), same as equal, will not update
                    client.checkAndPut("age", CheckCompareOp.LESS, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 5);
                    put.setObject("name", "name1");
                    put.setObject(
                            "age", 1); // coalesce(null as 0), same as greater, will not update
                    client.checkAndPut("age", CheckCompareOp.LESS, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 6);
                    put.setObject("name", "name1");
                    put.setObject(
                            "age", null); // coalesce(null as 0), new value is null, will not update
                    client.checkAndPut("age", CheckCompareOp.LESS, null, 0, put);
                    client.flush();

                    Record record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(-1, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(0, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(0, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 3).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(-1, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 4).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(null, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 5).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(null, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 6).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(null, record.getObject("age"));
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** Operation: less. check and delete without checkValue */
    @Test
    public void testLess002() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseFixedFe(true);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_less_002";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text, age int ,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            try {
                initData(client, schema);
                {
                    Put put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 0);
                    put.setObject("age", -1); // less
                    client.checkAndPut("age", CheckCompareOp.LESS, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 1);
                    put.setObject("age", 0); // equal, will not delete
                    client.checkAndPut("age", CheckCompareOp.LESS, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 2);
                    put.setObject("age", 1); // greater, will not delete
                    client.checkAndPut("age", CheckCompareOp.LESS, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 3);
                    put.setObject("age", -1); // coalesce(null as 0), same as less
                    client.checkAndPut("age", CheckCompareOp.LESS, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 4);
                    put.setObject("age", 0); // coalesce(null as 0), same as equal, will not delete
                    client.checkAndPut("age", CheckCompareOp.LESS, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 5);
                    put.setObject(
                            "age", 1); // coalesce(null as 0), same as greater, will not delete
                    client.checkAndPut("age", CheckCompareOp.LESS, null, 0, put);
                    client.flush();

                    Record record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
                    Assert.assertNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
                    Assert.assertNotNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).build()).get();
                    Assert.assertNotNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 3).build()).get();
                    Assert.assertNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 4).build()).get();
                    Assert.assertNotNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 5).build()).get();
                    Assert.assertNotNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 6).build()).get();
                    Assert.assertNotNull(record);
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** Operation: equal. check and put without checkValue. */
    @Test
    public void testEqual001() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_equal_001";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text, age int ,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            try {
                initData(client, schema);
                {
                    Put put = new Put(schema);
                    put.setObject("id", 0);
                    put.setObject("name", "name1");
                    put.setObject("age", -1); // less, will not update
                    client.checkAndPut("age", CheckCompareOp.EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 1);
                    put.setObject("name", "name1");
                    put.setObject("age", 0); // equal
                    client.checkAndPut("age", CheckCompareOp.EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 2);
                    put.setObject("name", "name1");
                    put.setObject("age", 1); // greater, will not update
                    client.checkAndPut("age", CheckCompareOp.EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 3);
                    put.setObject("name", "name1");
                    put.setObject("age", -1); // coalesce(null as 0), same as less, will not update
                    client.checkAndPut("age", CheckCompareOp.EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 4);
                    put.setObject("name", "name1");
                    put.setObject("age", 0); // coalesce(null as 0), same as equal
                    client.checkAndPut("age", CheckCompareOp.EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 5);
                    put.setObject("name", "name1");
                    put.setObject(
                            "age", 1); // coalesce(null as 0), same as greater, will not update
                    client.checkAndPut("age", CheckCompareOp.EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 6);
                    put.setObject("name", "name1");
                    put.setObject(
                            "age", null); // coalesce(null as 0), new value is null, will not update
                    client.checkAndPut("age", CheckCompareOp.EQUAL, null, 0, put);
                    client.flush();

                    Record record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(0, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(0, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(0, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 3).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(null, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 4).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(0, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 5).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(null, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 6).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(null, record.getObject("age"));
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** Operation: equal. check and delete without checkValue */
    @Test
    public void testEqual002() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseFixedFe(true);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_equal_002";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text, age int ,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            try {
                initData(client, schema);
                {
                    Put put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 0);
                    put.setObject("age", -1); // less, will not delete
                    client.checkAndPut("age", CheckCompareOp.EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 1);
                    put.setObject("age", 0); // equal
                    client.checkAndPut("age", CheckCompareOp.EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 2);
                    put.setObject("age", 1); // greater, will not delete
                    client.checkAndPut("age", CheckCompareOp.EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 3);
                    put.setObject("age", -1); // coalesce(null as 0), same as less, will not delete
                    client.checkAndPut("age", CheckCompareOp.EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 4);
                    put.setObject("age", 0); // coalesce(null as 0), same as equal
                    client.checkAndPut("age", CheckCompareOp.EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 5);
                    put.setObject(
                            "age", 1); // coalesce(null as 0), same as greater, will not delete
                    client.checkAndPut("age", CheckCompareOp.EQUAL, null, 0, put);
                    client.flush();

                    Record record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
                    Assert.assertNotNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
                    Assert.assertNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).build()).get();
                    Assert.assertNotNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 3).build()).get();
                    Assert.assertNotNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 4).build()).get();
                    Assert.assertNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 5).build()).get();
                    Assert.assertNotNull(record);
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** Operation: not equal. check and put without checkValue */
    @Test
    public void testNotEqual001() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseFixedFe(true);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_not_equal_001";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text, age int ,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            try {
                initData(client, schema);
                {
                    Put put = new Put(schema);
                    put.setObject("id", 0);
                    put.setObject("name", "name1");
                    put.setObject("age", -1); // less
                    client.checkAndPut("age", CheckCompareOp.NOT_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 1);
                    put.setObject("name", "name1");
                    put.setObject("age", 0); // equal, will not update
                    client.checkAndPut("age", CheckCompareOp.NOT_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 2);
                    put.setObject("name", "name1");
                    put.setObject("age", 1); // greater
                    client.checkAndPut("age", CheckCompareOp.NOT_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 3);
                    put.setObject("name", "name1");
                    put.setObject("age", -1); // coalesce(null as 0), same as less
                    client.checkAndPut("age", CheckCompareOp.NOT_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 4);
                    put.setObject("name", "name1");
                    put.setObject("age", 0); // coalesce(null as 0), same as equal, will not update
                    client.checkAndPut("age", CheckCompareOp.NOT_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 5);
                    put.setObject("name", "name1");
                    put.setObject("age", 1); // coalesce(null as 0), same as greater
                    client.checkAndPut("age", CheckCompareOp.NOT_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 6);
                    put.setObject("name", "name1");
                    put.setObject(
                            "age", null); // coalesce(null as 0), new value is null, will not update
                    client.checkAndPut("age", CheckCompareOp.NOT_EQUAL, null, 0, put);
                    client.flush();

                    Record record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(-1, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(0, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(1, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 3).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(-1, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 4).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(null, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 5).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(1, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 6).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(null, record.getObject("age"));
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** Operation: not equal. check and delete without checkValue */
    @Test
    public void testNotEqual002() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseFixedFe(true);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_not_equal_002";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text, age int ,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            try {
                initData(client, schema);
                {
                    Put put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 0);
                    put.setObject("age", -1); // less
                    client.checkAndPut("age", CheckCompareOp.NOT_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 1);
                    put.setObject("age", 0); // equal, will not delete
                    client.checkAndPut("age", CheckCompareOp.NOT_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 2);
                    put.setObject("age", 1); // greater
                    client.checkAndPut("age", CheckCompareOp.NOT_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 3);
                    put.setObject("age", -1); // coalesce(null as 0), same as less
                    client.checkAndPut("age", CheckCompareOp.NOT_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 4);
                    put.setObject("age", 0); // coalesce(null as 0), same as equal, will not delete
                    client.checkAndPut("age", CheckCompareOp.NOT_EQUAL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 5);
                    put.setObject("age", 1); // coalesce(null as 0), same as greater
                    client.checkAndPut("age", CheckCompareOp.NOT_EQUAL, null, 0, put);
                    client.flush();

                    Record record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
                    Assert.assertNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
                    Assert.assertNotNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).build()).get();
                    Assert.assertNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 3).build()).get();
                    Assert.assertNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 4).build()).get();
                    Assert.assertNotNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 5).build()).get();
                    Assert.assertNull(record);
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** Operation: is null. check and put without checkValue */
    @Test
    public void testIsNull001() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseFixedFe(true);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_is_null_001";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text, age int ,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            try {
                initData(client, schema);
                {
                    Put put = new Put(schema);
                    put.setObject("id", 0);
                    put.setObject("name", "name1");
                    put.setObject("age", -1); // not null, will not update
                    client.checkAndPut("age", CheckCompareOp.IS_NULL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 3);
                    put.setObject("name", "name1");
                    put.setObject("age", -1); // is null, do update
                    client.checkAndPut("age", CheckCompareOp.IS_NULL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 6);
                    put.setObject("name", "name1");
                    put.setObject("age", null); // is null, do update
                    client.checkAndPut("age", CheckCompareOp.IS_NULL, null, 0, put);
                    client.flush();

                    Record record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(0, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 3).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(-1, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 6).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(null, record.getObject("age"));
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** Operation: is null. check and delete without checkValue */
    @Test
    public void testIsNull002() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseFixedFe(true);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_is_null_002";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text, age int ,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            try {
                initData(client, schema);
                {
                    Put put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 0);
                    put.setObject("age", -1); // not null, will not delete
                    client.checkAndPut("age", CheckCompareOp.IS_NULL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 3);
                    put.setObject("age", -1); // is null, do delete
                    client.checkAndPut("age", CheckCompareOp.IS_NULL, null, 0, put);
                    client.flush();

                    Record record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
                    Assert.assertNotNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 3).build()).get();
                    Assert.assertNull(record);
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** Operation: is not null. check and put without checkValue */
    @Test
    public void testIsNotNull001() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseFixedFe(true);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_is_not_null_001";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text, age int ,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            try {
                initData(client, schema);
                {
                    Put put = new Put(schema);
                    put.setObject("id", 0);
                    put.setObject("name", "name1");
                    put.setObject("age", -1); // not null, do update
                    client.checkAndPut("age", CheckCompareOp.IS_NOT_NULL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 3);
                    put.setObject("name", "name1");
                    put.setObject("age", -1); // is null, will not update
                    client.checkAndPut("age", CheckCompareOp.IS_NOT_NULL, null, 0, put);
                    put = new Put(schema);
                    put.setObject("id", 6);
                    put.setObject("name", "name1");
                    put.setObject("age", null); // is null, will not update
                    client.checkAndPut("age", CheckCompareOp.IS_NOT_NULL, null, 0, put);
                    client.flush();

                    Record record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(-1, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 3).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(null, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 6).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(null, record.getObject("age"));
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** Operation: is not null. check and delete without checkValue */
    @Test
    public void testIsNotNull002() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseFixedFe(true);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_check_and_put_is_not_null_002";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text, age int ,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            try {
                initData(client, schema);
                {
                    Put put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 0);
                    put.setObject("age", -1); // not null, do delete
                    client.checkAndPut("age", CheckCompareOp.IS_NOT_NULL, null, 0, put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 3);
                    put.setObject("age", -1); // is null, will not delete
                    client.checkAndPut("age", CheckCompareOp.IS_NOT_NULL, null, 0, put);
                    client.flush();

                    Record record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
                    Assert.assertNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 3).build()).get();
                    Assert.assertNotNull(record);
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    private Object[] makeTestDataForDataTypes(String name) {
        switch (name) {
            case "int":
                return new Object[] {-1, 0, 1};
            case "smallint":
                return new Object[] {(short) -1, (short) 0, (short) 1};
            case "bigint":
                return new Object[] {-1L, 0L, 1L};
            case "decimal":
                // numeric(6,5)
                return new Object[] {
                    new BigDecimal("-1.00000"), new BigDecimal("0.00000"), new BigDecimal("1.00000")
                };
            case "bool":
                return new Object[] {false, true, true};
            case "float4":
                return new Object[] {-1.0f, 0.0f, 1.0f};
            case "float8":
                return new Object[] {-1.0, 0.0, 1.0};
            case "timestamp":
            case "timestamptz":
                return new Object[] {
                    Timestamp.valueOf("2024-12-12 12:12:12"),
                    Timestamp.valueOf("2024-12-12 12:12:13"),
                    Timestamp.valueOf("2024-12-12 12:12:14")
                };
            case "date":
                return new Object[] {
                    Date.valueOf("1900-01-02"),
                    Date.valueOf("1900-01-03"),
                    Date.valueOf("1900-01-04")
                };
            case "time":
                return new Object[] {
                    Time.valueOf("12:12:12"), Time.valueOf("12:12:13"), Time.valueOf("12:12:14")
                };
            case "json":
            case "jsonb":
                return new Object[] {
                    "{\"a\":\"" + -1 + "\"}", "{\"a\":\"" + 0 + "\"}", "{\"a\":\"" + 1 + "\"}"
                };
            case "bytea":
                return new Object[] {new byte[] {1}, new byte[] {2}, new byte[] {3}};
            case "char":
                // char(5)
                return new Object[] {"a    ", "b    ", "c    "};
            case "varchar":
            case "text":
                return new Object[] {"aaa", "aab", "aac"};
            default:
                throw new IllegalArgumentException("not a fixed plan supports type: " + name);
        }
    }

    @DataProvider(name = "typeCaseData")
    public Object[][] createTypesData() {
        DataTypeTestUtil.TypeCaseData[] typeToTest;
        // 只测试fixed plan支持的类型
        typeToTest = FIXED_PLAN_TYPE_DATA;
        Object[][] ret = new Object[typeToTest.length][];
        for (int i = 0; i < typeToTest.length; ++i) {
            ret[i] = new Object[] {typeToTest[i]};
        }
        return ret;
    }

    /** Operation: greater or equal. check and put without checkValue. */
    @Test(dataProvider = "typeCaseData")
    public void testGreaterOrEqualDataTypes001(DataTypeTestUtil.TypeCaseData typeCaseData)
            throws Exception {
        if (properties == null) {
            return;
        }
        String typeName = typeCaseData.getName();
        HoloConfig config = buildConfig();
        // json, jsonb not support compare
        // array type could not use fixed plan
        if (typeName.equals("roaringbitmap")
                || typeName.equals("bytea")
                || typeName.equals("json")
                || typeName.equals("jsonb")
                || typeName.startsWith("_")) {
            return;
        }
        // todo: remove it when instance support time do checkandput
        if (typeName.equals("time") || typeName.equals("timetz")) {
            return;
        }
        config.setAppName("testGreaterOrEqualDataTypes001");
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName =
                    "holo_client_check_and_put_greater_or_equal_type_" + typeName + "_001";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text, age "
                            + typeCaseData.getColumnType()
                            + " ,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);
            Object[] testData = makeTestDataForDataTypes(typeName);
            try {
                initData(client, schema, testData[1]);
                {
                    Put put = new Put(schema);
                    put.setObject("id", 0);
                    put.setObject("name", "name1");
                    put.setObject("age", testData[0]); // less, will not update
                    client.checkAndPut(
                            "age", CheckCompareOp.GREATER_OR_EQUAL, null, testData[1], put);
                    put = new Put(schema);
                    put.setObject("id", 1);
                    put.setObject("name", "name1");
                    put.setObject("age", testData[1]); // equal
                    client.checkAndPut(
                            "age", CheckCompareOp.GREATER_OR_EQUAL, null, testData[1], put);
                    put = new Put(schema);
                    put.setObject("id", 2);
                    put.setObject("name", "name1");
                    put.setObject("age", testData[2]); // greater
                    client.checkAndPut(
                            "age", CheckCompareOp.GREATER_OR_EQUAL, null, testData[1], put);
                    put = new Put(schema);
                    put.setObject("id", 3);
                    put.setObject("name", "name1");
                    put.setObject(
                            "age",
                            testData[0]); // coalesce(null as 0), same as less, will not update
                    client.checkAndPut(
                            "age", CheckCompareOp.GREATER_OR_EQUAL, null, testData[1], put);
                    put = new Put(schema);
                    put.setObject("id", 4);
                    put.setObject("name", "name1");
                    put.setObject("age", testData[1]); // coalesce(null as 0), same as equal
                    client.checkAndPut(
                            "age", CheckCompareOp.GREATER_OR_EQUAL, null, testData[1], put);
                    put = new Put(schema);
                    put.setObject("id", 5);
                    put.setObject("name", "name1");
                    put.setObject("age", testData[2]); // coalesce(null as 0), same as greater
                    client.checkAndPut(
                            "age", CheckCompareOp.GREATER_OR_EQUAL, null, testData[1], put);
                    put = new Put(schema);
                    put.setObject("id", 6);
                    put.setObject("name", "name1");
                    put.setObject(
                            "age", null); // coalesce(null as 0), new value is null, will not update
                    client.checkAndPut(
                            "age", CheckCompareOp.GREATER_OR_EQUAL, null, testData[1], put);
                    client.flush();

                    Record record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(testData[1], record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(testData[1], record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(testData[2], record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 3).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(null, record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 4).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(testData[1], record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 5).build()).get();
                    Assert.assertEquals("name1", record.getObject("name"));
                    Assert.assertEquals(testData[2], record.getObject("age"));
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 6).build()).get();
                    Assert.assertEquals("name0", record.getObject("name"));
                    Assert.assertEquals(null, record.getObject("age"));
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** Operation: greater or equal. check and delete without checkValue */
    @Test(dataProvider = "typeCaseData")
    public void testGreaterOrEqualDataTypes002(DataTypeTestUtil.TypeCaseData typeCaseData)
            throws Exception {
        if (properties == null) {
            return;
        }
        String typeName = typeCaseData.getName();
        HoloConfig config = buildConfig();
        // json, jsonb not support compare
        // array type could not use fixed plan
        if (typeName.equals("roaringbitmap")
                || typeName.equals("bytea")
                || typeName.equals("json")
                || typeName.equals("jsonb")
                || typeName.startsWith("_")) {
            return;
        }
        // todo: remove it when instance support time do checkandput
        if (typeName.equals("time") || typeName.equals("timetz")) {
            return;
        }
        config.setAppName("testGreaterOrEqualDataTypes002");
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName =
                    "holo_client_check_and_put_greater_or_equal_type_" + typeName + "_002";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text, age "
                            + typeCaseData.getColumnType()
                            + " ,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);
            Object[] testData = makeTestDataForDataTypes(typeName);
            try {
                initData(client, schema, testData[1]);
                {
                    Put put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 0);
                    put.setObject("age", testData[0]); // less, will not delete
                    client.checkAndPut(
                            "age", CheckCompareOp.GREATER_OR_EQUAL, null, testData[1], put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 1);
                    put.setObject("age", testData[1]); // equal
                    client.checkAndPut(
                            "age", CheckCompareOp.GREATER_OR_EQUAL, null, testData[1], put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 2);
                    put.setObject("age", testData[2]); // greater
                    client.checkAndPut(
                            "age", CheckCompareOp.GREATER_OR_EQUAL, null, testData[1], put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 3);
                    put.setObject(
                            "age",
                            testData[0]); // coalesce(null as 0), same as less, will not delete
                    client.checkAndPut(
                            "age", CheckCompareOp.GREATER_OR_EQUAL, null, testData[1], put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 4);
                    put.setObject("age", testData[1]); // coalesce(null as 0), same as equal
                    client.checkAndPut(
                            "age", CheckCompareOp.GREATER_OR_EQUAL, null, testData[1], put);
                    put = new Put(schema);
                    put.getRecord().setType(Put.MutationType.DELETE);
                    put.setObject("id", 5);
                    put.setObject("age", testData[2]); // coalesce(null as 0), same as greater
                    client.checkAndPut(
                            "age", CheckCompareOp.GREATER_OR_EQUAL, null, testData[1], put);
                    client.flush();

                    Record record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
                    Assert.assertNotNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
                    Assert.assertNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 2).build()).get();
                    Assert.assertNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 3).build()).get();
                    Assert.assertNotNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 4).build()).get();
                    Assert.assertNull(record);
                    record =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", 5).build()).get();
                    Assert.assertNull(record);
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }
}
