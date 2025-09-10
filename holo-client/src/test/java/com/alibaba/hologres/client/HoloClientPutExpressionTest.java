package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.exception.HoloClientWithDetailsException;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.expression.Expression;
import com.alibaba.hologres.client.model.expression.RecordWithExpression;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.Timestamp;

/** HoloClientPutExpressionTest. */
public class HoloClientPutExpressionTest extends HoloClientTestBase {

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
        return config;
    }

    /** check failed. */
    @Test
    public void testPutRecordWithExpressionNegative() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            HoloVersion version = ConnectionUtil.getHoloVersion(conn);
            if (version.compareTo(Expression.INSERT_SUPPORT_VERSION) < 0) {
                return;
            }
            String tableName = "holo_client_put_record_with_expr_negative";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text,modify_time timestamptz,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);
            // insertOrIgnore不支持表达式.
            assertThrowsWithMessage(
                    HoloClientException.class,
                    "RecordWithExpression not supports writeMode insertOrIgnore.",
                    () -> {
                        HoloConfig config1 = buildConfig();
                        config1.setOnConflictAction(OnConflictAction.INSERT_OR_IGNORE);
                        try (HoloClient client1 = new HoloClient(config1)) {
                            Record record =
                                    new RecordWithExpression.Builder(schema)
                                            .setConflictUpdateSet(
                                                    "name = excluded.name, modify_time=excluded.modify_time")
                                            .setConflictWhere(
                                                    "excluded.modify_time > old.modify_time")
                                            .build();
                            record.setObject(0, 0);
                            record.setObject(1, "name0");
                            record.setObject(2, "2020-01-01 00:00:00");
                            Put put = new Put(record);
                            client1.put(put);
                        }
                    });

            // 表达式中列名在表中不存在
            assertThrowsWithMessage(
                    HoloClientWithDetailsException.class,
                    "Column name1 not found in schema",
                    () -> {
                        Record record =
                                new RecordWithExpression.Builder(schema)
                                        .setConflictUpdateSet("name = excluded.name1")
                                        .build();
                        record.setObject(0, 0);
                        record.setObject(1, "name0");
                        record.setObject(2, "2020-01-01 00:00:00");
                        Put put = new Put(record);
                        client.put(put);
                    });

            // conflict where中有limit, 非预期字段
            assertThrowsWithMessage(
                    HoloClientWithDetailsException.class,
                    "Unsupported expression syntax, conflict update:name = excluded.name,  conflcit where:excluded.modify_time > old.modify_time limit 10",
                    () -> {
                        Record record =
                                new RecordWithExpression.Builder(schema)
                                        .setConflictUpdateSet("name = excluded.name")
                                        .setConflictWhere(
                                                "excluded.modify_time > old.modify_time limit 10")
                                        .build();
                        record.setObject(0, 0);
                        record.setObject(1, "name0");
                        record.setObject(2, "2020-01-01 00:00:00");
                        Put put = new Put(record);
                        client.put(put);
                    });
            // 写入字段中没有包含更新字段
            assertThrowsWithMessage(
                    HoloClientWithDetailsException.class,
                    "Column name in the conflict update must be set to a non-conflicting value in the record",
                    () -> {
                        Record record =
                                new RecordWithExpression.Builder(schema)
                                        .setConflictUpdateSet("name = old.name || 'abc'")
                                        .setConflictWhere("excluded.modify_time > old.modify_time")
                                        .build();
                        record.setObject(0, 0);
                        record.setObject(2, "2020-01-01 00:00:00");
                        Put put = new Put(record);
                        client.put(put);
                    });
            // sql注入一个drop table
            assertThrowsWithMessage(
                    HoloClientWithDetailsException.class,
                    "net.sf.jsqlparser.parser.ParseException: Encountered unexpected token: \"drop\" \"DROP\"\n",
                    () -> {
                        Record record =
                                new RecordWithExpression.Builder(schema)
                                        .setConflictUpdateSet("name = old.name || 'abc'")
                                        .setConflictWhere(
                                                "excluded.modify_time > old.modify_time; drop table "
                                                        + tableName
                                                        + ";")
                                        .build();
                        record.setObject(0, 0);
                        record.setObject(1, "name0");
                        record.setObject(2, "2020-01-01 00:00:00");
                        Put put = new Put(record);
                        client.put(put);
                    });

            if (version.compareTo(Expression.DELETE_SUPPORT_VERSION) < 0) {
                return;
            }
            // delete 传入了confictUpdateSet
            assertThrowsWithMessage(
                    HoloClientException.class,
                    "RecordWithExpression not supports delete with conflictUpdateSet",
                    () -> {
                        Record record =
                                new RecordWithExpression.Builder(schema)
                                        .setConflictUpdateSet("name = old.name || 'abc'")
                                        .setConflictWhere(
                                                "excluded.modify_time > old.modify_time;"
                                                        + tableName
                                                        + ";")
                                        .build();
                        record.setObject(0, 0);
                        record.setObject(1, "name0");
                        record.setObject(2, "2020-01-01 00:00:00");
                        record.setType(Put.MutationType.DELETE);
                        Put put = new Put(record);
                        client.put(put);
                    });
        }
    }

    /** INSERT. */
    @Test(dataProvider = "useLegacyPutHandler")
    public void testPutRecordWithExpression001(boolean useLegacyPutHandler, boolean update)
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
            HoloVersion version = ConnectionUtil.getHoloVersion(conn);
            if (version.compareTo(Expression.INSERT_SUPPORT_VERSION) < 0) {
                return;
            }
            String tableName = "holo_client_put_record_with_expr_negative_001";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text,modify_time timestamptz, address text,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            Record record =
                    new RecordWithExpression.Builder(schema)
                            .setConflictUpdateSet(
                                    "name = CONCAT(old.name, excluded.name), modify_time=excluded.modify_time")
                            .setConflictWhere("excluded.modify_time > old.modify_time")
                            .build();
            record.setObject(0, 0);
            record.setObject(1, "name0");
            record.setObject(2, "2020-01-01 00:00:00");
            record.setObject(3, "address0");
            client.put(new Put(record));

            record =
                    new RecordWithExpression.Builder(schema)
                            .setConflictUpdateSet(
                                    "name = CONCAT(old.name, excluded.name), modify_time=excluded.modify_time")
                            .setConflictWhere("excluded.modify_time > old.modify_time")
                            .build();
            record.setObject(0, 0);
            record.setObject(1, "name1");
            record.setObject(2, "2020-01-01 00:00:00");
            client.put(new Put(record));

            record =
                    new RecordWithExpression.Builder(schema)
                            .setConflictWhere("excluded.modify_time > old.modify_time")
                            .build();
            record.setObject(0, 0);
            record.setObject(1, "name2");
            record.setObject(2, "2020-01-01 01:00:00");
            client.put(new Put(record));

            record =
                    new RecordWithExpression.Builder(schema)
                            .setConflictUpdateSet(
                                    "name = CONCAT(old.name, excluded.name), modify_time=excluded.modify_time")
                            .build();
            record.setObject(0, 0);
            record.setObject(1, "name3");
            record.setObject(2, "2020-01-01 01:00:00");
            client.put(new Put(record));

            record =
                    new RecordWithExpression.Builder(schema)
                            .setConflictUpdateSet(
                                    "name = CONCAT(old.name, excluded.name), modify_time=excluded.modify_time")
                            .setConflictWhere("excluded.modify_time > old.modify_time")
                            .build();
            record.setObject(0, 0);
            record.setObject(1, "name4");
            record.setObject(2, "2020-01-01 02:00:00");
            client.put(new Put(record));
            client.flush();

            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
            Assert.assertEquals("name2name3name4", r.getObject(1));
            Assert.assertEquals(Timestamp.valueOf("2020-01-01 02:00:00.0"), r.getObject(2));
            if (update) {
                Assert.assertEquals("address0", r.getObject(3));
            } else {
                Assert.assertNull(r.getObject(3));
            }
            execute(conn, new String[] {dropSql});
        }
    }

    /** DELETE. */
    @Test
    public void testPutRecordWithExpression002() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setForceFlushInterval(100000);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            HoloVersion version = ConnectionUtil.getHoloVersion(conn);
            if (version.compareTo(Expression.DELETE_SUPPORT_VERSION) < 0) {
                return;
            }
            String tableName = "holo_client_put_record_with_expr_negative_002";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text,modify_time timestamptz, address text,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            Record record = new RecordWithExpression.Builder(schema).build();
            record.setObject(0, 0);
            record.setObject(1, "name0");
            record.setObject(2, "2020-01-01 00:00:00");
            record.setObject(3, "address0");
            client.put(new Put(record));
            client.flush();
            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
            Assert.assertEquals(r.getObject(1), "name0");
            Assert.assertEquals(r.getObject(2), Timestamp.valueOf("2020-01-01 00:00:00.0"));
            Assert.assertEquals(r.getObject(3), "address0");

            record =
                    new RecordWithExpression.Builder(schema)
                            .setConflictWhere("modify_time >= '2020-01-01 00:00:00'::timestamptz")
                            .build();
            record.setType(Put.MutationType.DELETE);
            record.setObject(0, 0);
            record.setObject(1, "name1");
            record.setObject(2, "2020-01-01 00:00:00");
            client.put(new Put(record));
            client.flush();
            r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
            Assert.assertNull(r);

            execute(conn, new String[] {dropSql});
        }
    }
}
