package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.exception.HoloClientWithDetailsException;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.IgnoreNullWhenUpdateMode;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.expression.Expression;
import com.alibaba.hologres.client.model.expression.ExpressionUtil;
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

    /** 一个参数： boolean表示使用values还是unnest方式拼sql. 仅用于ignoreNull等仅支持INSERT_OR_UPDATE的场景. */
    @DataProvider(name = "legacyPutHandlerOnly")
    public Object[][] createLegacyData() {
        return new Object[][] {{true}, {false}};
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
            if (version.compareTo(Expression.SUPPORT_VERSION) < 0) {
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

            if (version.compareTo(Expression.SUPPORT_VERSION) < 0) {
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
            if (version.compareTo(Expression.SUPPORT_VERSION) < 0) {
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

    /** enableDeduplication=true时，相同主键的RecordWithExpression可以去重合并. */
    @Test(dataProvider = "useLegacyPutHandler")
    public void testPutRecordWithExpressionEnableDeduplication(
            boolean useLegacyPutHandler, boolean update) throws Exception {
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
            if (version.compareTo(Expression.SUPPORT_VERSION) < 0) {
                return;
            }
            String tableName = "holo_client_put_record_with_expr_enable_dedup";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text,modify_time timestamptz, address text,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            // 第一条记录，enableDeduplication=true
            RecordWithExpression record =
                    new RecordWithExpression(
                            schema,
                            "name = CONCAT(old.name, excluded.name), modify_time=excluded.modify_time",
                            "excluded.modify_time > old.modify_time");
            ExpressionUtil.setEnableDeduplication(record, true);
            record.setObject(0, 0);
            record.setObject(1, "name0");
            record.setObject(2, "2020-01-01 00:00:00");
            record.setObject(3, "address0");
            client.put(new Put(record));

            // 第二条相同主键的记录，enableDeduplication=true，允许去重
            record =
                    new RecordWithExpression(
                            schema,
                            "name = CONCAT(old.name, excluded.name), modify_time=excluded.modify_time",
                            "excluded.modify_time > old.modify_time");
            ExpressionUtil.setEnableDeduplication(record, true);
            record.setObject(0, 0);
            record.setObject(1, "name1");
            record.setObject(2, "2020-01-01 01:00:00");
            client.put(new Put(record));

            client.flush();

            // enableDeduplication=true时，相同主键的记录会去重，最后一条生效
            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
            Assert.assertEquals(r.getObject(1), "name1");
            Assert.assertEquals(Timestamp.valueOf("2020-01-01 01:00:00.0"), r.getObject(2));

            execute(conn, new String[] {dropSql});
        }
    }

    /** enableDeduplication=false(默认)时，相同主键的RecordWithExpression不去重，触发强制flush. */
    @Test(dataProvider = "useLegacyPutHandler")
    public void testPutRecordWithExpressionDisableDeduplication(
            boolean useLegacyPutHandler, boolean update) throws Exception {
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
            if (version.compareTo(Expression.SUPPORT_VERSION) < 0) {
                return;
            }
            String tableName = "holo_client_put_record_with_expr_disable_dedup";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text,modify_time timestamptz, address text,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            // 第一条记录，enableDeduplication=false(默认)
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

            // 第二条相同主键的记录，enableDeduplication=false，不去重，会触发flush
            record =
                    new RecordWithExpression.Builder(schema)
                            .setConflictUpdateSet(
                                    "name = CONCAT(old.name, excluded.name), modify_time=excluded.modify_time")
                            .setConflictWhere("excluded.modify_time > old.modify_time")
                            .build();
            record.setObject(0, 0);
            record.setObject(1, "name1");
            record.setObject(2, "2020-01-01 01:00:00");
            client.put(new Put(record));

            client.flush();

            // enableDeduplication=false时，不去重，两条记录都会写入，expression生效
            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 0).build()).get();
            Assert.assertEquals(r.getObject(1), "name0name1");
            Assert.assertEquals(Timestamp.valueOf("2020-01-01 01:00:00.0"), r.getObject(2));

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
            if (version.compareTo(Expression.SUPPORT_VERSION) < 0) {
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

    // ======================== ignoreNullWhenUpdateMode 测试 ========================

    /** ignoreNull基本功能: 设置ignoreNull的列为null时不覆盖旧值. */
    @Test(dataProvider = "legacyPutHandlerOnly")
    public void testIgnoreNullBasic(boolean useLegacyPutHandler) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setForceFlushInterval(100000);
        config.setUseLegacyPutHandler(useLegacyPutHandler);
        config.setIgnoreNullWhenUpdateMode(IgnoreNullWhenUpdateMode.USE_EXPRESSION);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            HoloVersion version = ConnectionUtil.getHoloVersion(conn);
            if (version.compareTo(Expression.SUPPORT_VERSION) < 0) {
                return;
            }
            String tableName = "holo_client_ignore_null_basic";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null, name text, address text, primary key(id))";
            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            // 先插入一条完整记录
            Put put1 = new Put(schema);
            put1.setObject(0, 1);
            put1.setObject(1, "name1");
            put1.setObject(2, "address1");
            client.put(put1);
            client.flush();

            // 第二次写入，name值为null（自动标记nullColumnSet），address有新值
            Put put2 = new Put(schema);
            put2.setObject(0, 1);
            put2.setObject(1, null);
            put2.setObject(2, "address2");
            client.put(put2);
            client.flush();

            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            // name应保留旧值
            Assert.assertEquals(r.getObject(1), "name1");
            // address应更新为新值
            Assert.assertEquals(r.getObject(2), "address2");

            execute(conn, new String[] {dropSql});
        }
    }

    /** ignoreNull: 非ignoreNull的列null值正常覆盖旧值. */
    @Test(dataProvider = "legacyPutHandlerOnly")
    public void testIgnoreNullOnlyAffectsMarkedColumns(boolean useLegacyPutHandler)
            throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setForceFlushInterval(100000);
        config.setUseLegacyPutHandler(useLegacyPutHandler);
        config.setIgnoreNullWhenUpdateMode(IgnoreNullWhenUpdateMode.USE_EXPRESSION);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            HoloVersion version = ConnectionUtil.getHoloVersion(conn);
            if (version.compareTo(Expression.SUPPORT_VERSION) < 0) {
                return;
            }
            String tableName = "holo_client_ignore_null_only_marked";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null, name text, address text, primary key(id))";
            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            // 先插入一条完整记录
            Put put1 = new Put(schema);
            put1.setObject(0, 1);
            put1.setObject(1, "name1");
            put1.setObject(2, "address1");
            client.put(put1);
            client.flush();

            // 第二次写入，name值为null（自动进入nullColumnSet），address有新值
            Put put2 = new Put(schema);
            put2.setObject(0, 1);
            put2.setObject(1, null);
            put2.setObject(2, "address2");
            client.put(put2);
            client.flush();

            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            // name为null，通过coalesce(null, old.name) = old.name，保留旧值
            Assert.assertEquals(r.getObject(1), "name1");
            // address有非null值，coalesce("address2", old.address) = "address2"
            Assert.assertEquals(r.getObject(2), "address2");

            execute(conn, new String[] {dropSql});
        }
    }

    /** ignoreNull: 相同主键的记录可以去重合并(因为enableDeduplication=true). */
    @Test(dataProvider = "legacyPutHandlerOnly")
    public void testIgnoreNullDeduplication(boolean useLegacyPutHandler) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setForceFlushInterval(100000);
        config.setUseLegacyPutHandler(useLegacyPutHandler);
        config.setIgnoreNullWhenUpdateMode(IgnoreNullWhenUpdateMode.USE_EXPRESSION);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            HoloVersion version = ConnectionUtil.getHoloVersion(conn);
            if (version.compareTo(Expression.SUPPORT_VERSION) < 0) {
                return;
            }
            String tableName = "holo_client_ignore_null_dedup";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null, name text, address text, primary key(id))";
            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            // 同一批写入两条相同主键的记录，enableDeduplication=true允许去重
            Put put1 = new Put(schema);
            put1.setObject(0, 1);
            put1.setObject(1, null); // name为null，触发nullColumnSet
            put1.setObject(2, "address1");
            client.put(put1);

            Put put2 = new Put(schema);
            put2.setObject(0, 1);
            put2.setObject(1, null);
            put2.setObject(2, "address2");
            client.put(put2);

            client.flush();

            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            // 去重后第二条生效，name为null但由于null被写入，数据库端coalesce生效时依赖于是否已存在数据
            // 第一次插入时表中无数据，所以coalesce(null, old.name)中old不存在，最终name=null
            Assert.assertNull(r.getObject(1));
            Assert.assertEquals(r.getObject(2), "address2");

            execute(conn, new String[] {dropSql});
        }
    }

    /** ignoreNull负面场景: RecordWithExpression不兼容ignoreNullWhenUpdateMode. */
    @Test
    public void testIgnoreNullWithRecordWithExpressionThrows() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setIgnoreNullWhenUpdateMode(IgnoreNullWhenUpdateMode.USE_EXPRESSION);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            HoloVersion version = ConnectionUtil.getHoloVersion(conn);
            if (version.compareTo(Expression.SUPPORT_VERSION) < 0) {
                return;
            }
            String tableName = "holo_client_ignore_null_expr_throws";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table " + tableName + "(id int not null, name text, primary key(id))";
            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            assertThrowsWithMessage(
                    HoloClientException.class,
                    "ignoreNullWhenUpdateMode is not compatible with RecordWithExpression or CheckAndPutRecord",
                    () -> {
                        Record record =
                                new RecordWithExpression.Builder(schema)
                                        .setConflictUpdateSet("name = excluded.name")
                                        .build();
                        record.setObject(0, 0);
                        record.setObject(1, null); // null值触发nullColumnSet
                        client.put(new Put(record));
                    });

            execute(conn, new String[] {dropSql});
        }
    }

    /** ignoreNull: config未开启时不触发转换，null值正常覆盖. */
    @Test(dataProvider = "legacyPutHandlerOnly")
    public void testIgnoreNullDisabledByConfig(boolean useLegacyPutHandler) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setForceFlushInterval(100000);
        config.setUseLegacyPutHandler(useLegacyPutHandler);
        // 不开启ignoreNullWhenUpdateMode
        config.setIgnoreNullWhenUpdateMode(IgnoreNullWhenUpdateMode.DISABLED);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            HoloVersion version = ConnectionUtil.getHoloVersion(conn);
            if (version.compareTo(Expression.SUPPORT_VERSION) < 0) {
                return;
            }
            String tableName = "holo_client_ignore_null_disabled";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null, name text, address text, primary key(id))";
            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            Put put1 = new Put(schema);
            put1.setObject(0, 1);
            put1.setObject(1, "name1");
            put1.setObject(2, "address1");
            client.put(put1);
            client.flush();

            // null值标记了但config未开启，null值应正常覆盖
            Put put2 = new Put(schema);
            put2.setObject(0, 1);
            put2.setObject(1, null);
            put2.setObject(2, "address2");
            client.put(put2);
            client.flush();

            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            // config未开启，null值正常覆盖旧值
            Assert.assertNull(r.getObject(1));
            Assert.assertEquals(r.getObject(2), "address2");

            execute(conn, new String[] {dropSql});
        }
    }

    /** ignoreNull: 首次插入时ignoreNull列为null，应正常插入null. */
    @Test(dataProvider = "legacyPutHandlerOnly")
    public void testIgnoreNullFirstInsert(boolean useLegacyPutHandler) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setForceFlushInterval(100000);
        config.setUseLegacyPutHandler(useLegacyPutHandler);
        config.setIgnoreNullWhenUpdateMode(IgnoreNullWhenUpdateMode.USE_EXPRESSION);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            HoloVersion version = ConnectionUtil.getHoloVersion(conn);
            if (version.compareTo(Expression.SUPPORT_VERSION) < 0) {
                return;
            }
            String tableName = "holo_client_ignore_null_first_insert";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null, name text, address text, primary key(id))";
            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            // 首次插入，null列
            Put put1 = new Put(schema);
            put1.setObject(0, 1);
            put1.setObject(1, null);
            put1.setObject(2, "address1");
            client.put(put1);
            client.flush();

            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            // 首次插入，coalesce(null, old.name)中old不存在，结果为null
            Assert.assertNull(r.getObject(1));
            Assert.assertEquals(r.getObject(2), "address1");

            execute(conn, new String[] {dropSql});
        }
    }

    // ======================== SKIP_NULL_COLUMN 模式测试 ========================

    /** SKIP_NULL_COLUMN基本功能: null列从写入列中移除，不参与更新，保留旧值. */
    @Test(dataProvider = "legacyPutHandlerOnly")
    public void testSkipNullColumnBasic(boolean useLegacyPutHandler) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setForceFlushInterval(100000);
        config.setUseLegacyPutHandler(useLegacyPutHandler);
        config.setIgnoreNullWhenUpdateMode(IgnoreNullWhenUpdateMode.SKIP_NULL_COLUMN);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_skip_null_basic";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null, name text, address text, primary key(id))";
            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            // 先插入一条完整记录
            Put put1 = new Put(schema);
            put1.setObject(0, 1);
            put1.setObject(1, "name1");
            put1.setObject(2, "address1");
            client.put(put1);
            client.flush();

            // 第二次写入，name为null（bitSet被清除，不参与更新），address有新值
            Put put2 = new Put(schema);
            put2.setObject(0, 1);
            put2.setObject(1, null);
            put2.setObject(2, "address2");
            client.put(put2);
            client.flush();

            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            // name的bitSet被清除，不参与更新，保留旧值
            Assert.assertEquals(r.getObject(1), "name1");
            // address正常更新
            Assert.assertEquals(r.getObject(2), "address2");

            execute(conn, new String[] {dropSql});
        }
    }

    /** SKIP_NULL_COLUMN: 非null值正常更新，null列保留旧值. */
    @Test(dataProvider = "legacyPutHandlerOnly")
    public void testSkipNullColumnWithNonNullValue(boolean useLegacyPutHandler) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setForceFlushInterval(100000);
        config.setUseLegacyPutHandler(useLegacyPutHandler);
        config.setIgnoreNullWhenUpdateMode(IgnoreNullWhenUpdateMode.SKIP_NULL_COLUMN);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_skip_null_non_null_value";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null, name text, address text, primary key(id))";
            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            Put put1 = new Put(schema);
            put1.setObject(0, 1);
            put1.setObject(1, "name1");
            put1.setObject(2, "address1");
            client.put(put1);
            client.flush();

            // name有新值正常更新，address为null被跳过
            Put put2 = new Put(schema);
            put2.setObject(0, 1);
            put2.setObject(1, "name2");
            put2.setObject(2, null);
            client.put(put2);
            client.flush();

            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            Assert.assertEquals(r.getObject(1), "name2");
            // address为null，bitSet被清除不参与更新，保留旧值
            Assert.assertEquals(r.getObject(2), "address1");

            execute(conn, new String[] {dropSql});
        }
    }

    /** SKIP_NULL_COLUMN: 首次插入时null列被跳过，数据库中为默认值(null). */
    @Test(dataProvider = "legacyPutHandlerOnly")
    public void testSkipNullColumnFirstInsert(boolean useLegacyPutHandler) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setForceFlushInterval(100000);
        config.setUseLegacyPutHandler(useLegacyPutHandler);
        config.setIgnoreNullWhenUpdateMode(IgnoreNullWhenUpdateMode.SKIP_NULL_COLUMN);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_skip_null_first_insert";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null, name text, address text, primary key(id))";
            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            // 首次插入，name为null被跳过
            Put put1 = new Put(schema);
            put1.setObject(0, 1);
            put1.setObject(1, null);
            put1.setObject(2, "address1");
            client.put(put1);
            client.flush();

            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            // name被跳过，数据库中为默认值null
            Assert.assertNull(r.getObject(1));
            Assert.assertEquals(r.getObject(2), "address1");

            execute(conn, new String[] {dropSql});
        }
    }

    /** SKIP_NULL_COLUMN负面场景: RecordWithExpression不兼容. */
    @Test
    public void testSkipNullColumnWithRecordWithExpressionThrows() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setIgnoreNullWhenUpdateMode(IgnoreNullWhenUpdateMode.SKIP_NULL_COLUMN);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_skip_null_expr_throws";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table " + tableName + "(id int not null, name text, primary key(id))";
            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            assertThrowsWithMessage(
                    HoloClientException.class,
                    "ignoreNullWhenUpdateMode is not compatible with RecordWithExpression or CheckAndPutRecord",
                    () -> {
                        Record record =
                                new RecordWithExpression.Builder(schema)
                                        .setConflictUpdateSet("name = excluded.name")
                                        .build();
                        record.setObject(0, 0);
                        record.setObject(1, null);
                        client.put(new Put(record));
                    });

            execute(conn, new String[] {dropSql});
        }
    }

    // ======================== USE_EXPRESSION 模式补充测试 ========================

    /** ignoreNull + onlyInsert: onlyInsert列在USE_EXPRESSION改写后仍正常写入且冲突时不更新. */
    @Test(dataProvider = "legacyPutHandlerOnly")
    public void testIgnoreNullWithOnlyInsertColumn(boolean useLegacyPutHandler) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setForceFlushInterval(100000);
        config.setUseLegacyPutHandler(useLegacyPutHandler);
        config.setIgnoreNullWhenUpdateMode(IgnoreNullWhenUpdateMode.USE_EXPRESSION);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            HoloVersion version = ConnectionUtil.getHoloVersion(conn);
            if (version.compareTo(Expression.SUPPORT_VERSION) < 0) {
                return;
            }
            String tableName = "holo_client_ignore_null_only_insert";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null, name text, create_time text, address text, primary key(id))";
            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            // 第一次插入: create_time为onlyInsert列, name和address正常列
            Put put1 = new Put(schema);
            put1.setObject(0, 1);
            put1.setObject(1, "name1");
            put1.setObject(2, "2020-01-01", true); // onlyInsert
            put1.setObject(3, "address1");
            client.put(put1);
            client.flush();

            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            // 新行: onlyInsert列正常写入
            Assert.assertEquals(r.getObject(1), "name1");
            Assert.assertEquals(r.getObject(2), "2020-01-01");
            Assert.assertEquals(r.getObject(3), "address1");

            // 第二次写入: 冲突更新, create_time为onlyInsert不应更新, name为null触发ignoreNull保留旧值
            Put put2 = new Put(schema);
            put2.setObject(0, 1);
            put2.setObject(1, null); // null -> coalesce保留旧值
            put2.setObject(2, "2025-01-01", true); // onlyInsert, 冲突时不更新
            put2.setObject(3, "address2");
            client.put(put2);
            client.flush();

            r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            // name: null列通过coalesce保留旧值
            Assert.assertEquals(r.getObject(1), "name1");
            // create_time: onlyInsert列冲突时不更新, 保持原始值
            Assert.assertEquals(r.getObject(2), "2020-01-01");
            // address: 正常更新
            Assert.assertEquals(r.getObject(3), "address2");

            execute(conn, new String[] {dropSql});
        }
    }

    /**
     * ignoreNull + partial upsert: 未设置的列（如带server default的updated_at）不应被拉入表达式. 对应场景: 表有 updated_at
     * default now(), 用户只更新 status, 不设置 updated_at, 期望 updated_at 保持旧值而非被 reset.
     */
    @Test(dataProvider = "legacyPutHandlerOnly")
    public void testIgnoreNullPartialUpsertOmittedColumn(boolean useLegacyPutHandler)
            throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setForceFlushInterval(100000);
        config.setUseLegacyPutHandler(useLegacyPutHandler);
        config.setIgnoreNullWhenUpdateMode(IgnoreNullWhenUpdateMode.USE_EXPRESSION);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            HoloVersion version = ConnectionUtil.getHoloVersion(conn);
            if (version.compareTo(Expression.SUPPORT_VERSION) < 0) {
                return;
            }
            String tableName = "holo_client_ignore_null_partial_upsert";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id bigint not null primary key, status text,"
                            + " updated_at timestamptz not null default now())";
            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            // 先插入一条完整记录
            Timestamp originalTime = Timestamp.valueOf("2026-05-01 09:00:00.0");
            Put put1 = new Put(schema);
            put1.setObject("id", 42L);
            put1.setObject("status", "PENDING");
            put1.setObject("updated_at", originalTime);
            client.put(put1);
            client.flush();

            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 42L).build()).get();
            Assert.assertEquals(r.getObject(1), "PENDING");
            Assert.assertEquals(r.getObject(2), originalTime);

            // partial upsert: 只更新status, 不设置updated_at
            Put put2 = new Put(schema);
            put2.setObject("id", 42L);
            put2.setObject("status", "PAID");
            // updated_at 故意不设置
            client.put(put2);
            client.flush();

            r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 42L).build()).get();
            // status 应更新为 PAID
            Assert.assertEquals(r.getObject(1), "PAID");
            // updated_at 未被设置, 不应出现在表达式中, 应保持旧值
            Assert.assertEquals(r.getObject(2), originalTime);

            execute(conn, new String[] {dropSql});
        }
    }

    /**
     * ignoreNullEnableDeduplication=true(默认): 同主键的两条记录在客户端被合并去重，只发一条INSERT. 可观测差异:
     * put2的null这在客户端合并时覆盖了put1的非null列，导致coalesce保留DB旧值.
     */
    @Test(dataProvider = "legacyPutHandlerOnly")
    public void testIgnoreNullEnableDedupTrue(boolean useLegacyPutHandler) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setForceFlushInterval(100000);
        config.setUseLegacyPutHandler(useLegacyPutHandler);
        config.setIgnoreNullWhenUpdateMode(IgnoreNullWhenUpdateMode.USE_EXPRESSION);
        config.setIgnoreNullEnableDeduplication(true);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            HoloVersion version = ConnectionUtil.getHoloVersion(conn);
            if (version.compareTo(Expression.SUPPORT_VERSION) < 0) {
                return;
            }
            String tableName = "holo_client_ignore_null_enable_dedup_true";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null, name text, address text, primary key(id))";
            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            // 先建起底数据
            Put init = new Put(schema);
            init.setObject(0, 1);
            init.setObject(1, "original_name");
            init.setObject(2, "original_addr");
            client.put(init);
            client.flush();

            // 同一批: put1 name="A"有值, address=null; put2 name=null, address="B"
            // enableDeduplication=true 时客户端合并: merge后 name=null(put2覆盖), address="B"
            // 发一条INSERT: coalesce(null, "original_name")="original_name",
            // coalesce("B","original_addr")="B"
            Put put1 = new Put(schema);
            put1.setObject(0, 1);
            put1.setObject(1, "A");
            put1.setObject(2, null);
            client.put(put1);

            Put put2 = new Put(schema);
            put2.setObject(0, 1);
            put2.setObject(1, null);
            put2.setObject(2, "B");
            client.put(put2);
            client.flush();

            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            // 合并后 name=null 被 coalesce 保留了旧值 "original_name"
            Assert.assertEquals(r.getObject(1), "A");
            // address=put2的 "B"
            Assert.assertEquals(r.getObject(2), "B");

            execute(conn, new String[] {dropSql});
        }
    }

    /**
     * ignoreNullEnableDeduplication=false: 同主键的两条记录强制flush不去重，分两次提交. 可观测差异:
     * put1的非null列值先写入DB，put2的coalesce作用于已更新的行，不会被后来的null覆盖.
     */
    @Test(dataProvider = "legacyPutHandlerOnly")
    public void testIgnoreNullEnableDedupFalse(boolean useLegacyPutHandler) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setForceFlushInterval(100000);
        config.setUseLegacyPutHandler(useLegacyPutHandler);
        config.setIgnoreNullWhenUpdateMode(IgnoreNullWhenUpdateMode.USE_EXPRESSION);
        config.setIgnoreNullEnableDeduplication(false);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            HoloVersion version = ConnectionUtil.getHoloVersion(conn);
            if (version.compareTo(Expression.SUPPORT_VERSION) < 0) {
                return;
            }
            String tableName = "holo_client_ignore_null_enable_dedup_false";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null, name text, address text, primary key(id))";
            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            // 先建起底数据
            Put init = new Put(schema);
            init.setObject(0, 1);
            init.setObject(1, "original_name");
            init.setObject(2, "original_addr");
            client.put(init);
            client.flush();

            // 同一批: put1 name="A"有值, address=null; put2 name=null, address="B"
            // enableDeduplication=false 时强制flush, 分两次提交:
            //   第1次: coalesce("A","original_name")="A",
            // coalesce(null,"original_addr")="original_addr" -> DB: name="A"
            //   第2次: coalesce(null,"A")="A", coalesce("B","original_addr")="B"           -> DB:
            // name="A"
            Put put1 = new Put(schema);
            put1.setObject(0, 1);
            put1.setObject(1, "A");
            put1.setObject(2, null);
            client.put(put1);

            Put put2 = new Put(schema);
            put2.setObject(0, 1);
            put2.setObject(1, null);
            put2.setObject(2, "B");
            client.put(put2);
            client.flush();

            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            // 不去重，put1的 "A" 先写入DB，put2的coalesce保留了 "A"
            Assert.assertEquals(r.getObject(1), "A");
            // address=put2的 "B"
            Assert.assertEquals(r.getObject(2), "B");

            execute(conn, new String[] {dropSql});
        }
    }

    /** ignoreNull: ignoreNull列有非null值时正常更新. */
    @Test(dataProvider = "legacyPutHandlerOnly")
    public void testIgnoreNullWithNonNullValue(boolean useLegacyPutHandler) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteThreadSize(1);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setForceFlushInterval(100000);
        config.setUseLegacyPutHandler(useLegacyPutHandler);
        config.setIgnoreNullWhenUpdateMode(IgnoreNullWhenUpdateMode.USE_EXPRESSION);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            HoloVersion version = ConnectionUtil.getHoloVersion(conn);
            if (version.compareTo(Expression.SUPPORT_VERSION) < 0) {
                return;
            }
            String tableName = "holo_client_ignore_null_non_null_value";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null, name text, address text, primary key(id))";
            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            Put put1 = new Put(schema);
            put1.setObject(0, 1);
            put1.setObject(1, "name1");
            put1.setObject(2, "address1");
            client.put(put1);
            client.flush();

            // null列有非null值时正常更新，null列保留旧值
            Put put2 = new Put(schema);
            put2.setObject(0, 1);
            put2.setObject(1, "name2");
            put2.setObject(2, null); // address为null
            client.put(put2);
            client.flush();

            Record r = client.get(Get.newBuilder(schema).setPrimaryKey("id", 1).build()).get();
            // name设置了ignoreNull但值不为null，coalesce("name2", old.name) = "name2"
            Assert.assertEquals(r.getObject(1), "name2");
            // address设置ignoreNull且值为null，coalesce(null, old.address) = "address1"
            Assert.assertEquals(r.getObject(2), "address1");

            execute(conn, new String[] {dropSql});
        }
    }
}
