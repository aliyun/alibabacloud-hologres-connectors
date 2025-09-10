package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.ExecutionPool;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * HoloClient Tester.
 *
 * @version 1.0
 * @since
 *     <pre>12月 2, 2020</pre>
 */
public class HoloClientGetTest extends HoloClientTestBase {

    /** boolean表示使用where pk or 还是where pk in (select unnest)方式拼sql. */
    @DataProvider(name = "useLegacyGetHandler")
    public Object[][] createData() {
        Object[][] ret = new Object[2][];
        ret[0] = new Object[] {true};
        ret[1] = new Object[] {false};
        return ret;
    }

    /** get bytea Method: put(Put put). */
    @Test(dataProvider = "useLegacyGetHandler")
    public void testGet001(boolean useLegacyGetHandler) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseLegacyGetHandler(useLegacyGetHandler);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
        config.setWriteMaxIntervalMs(3000L);
        config.setEnableDefaultForNotNullColumn(false);
        config.setAppName("testGet001");
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "test_schema.holo_client_get_001";
            String createSchema = "create schema if not exists test_schema";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id bytea not null,name text not null,address text,primary key(id))";

            execute(conn, new String[] {createSchema, dropSql, createSql});

            try {
                TableSchema schema = client.getTableSchema(tableName);

                Put put = new Put(schema);
                put.setObject(0, new byte[] {1, 2});
                put.setObject(1, "name0");
                put.setObject(2, "address0");
                client.put(put);

                client.flush();

                Record r =
                        client.get(
                                        Get.newBuilder(schema)
                                                .setPrimaryKey("id", new byte[] {1, 2})
                                                .build())
                                .get();
                Assert.assertEquals("name0", r.getObject("name"));
                Assert.assertEquals("address0", r.getObject("address"));

            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** get long int short Method: put(Put put). */
    @Test(dataProvider = "useLegacyGetHandler")
    public void testGet002(boolean useLegacyGetHandler) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseLegacyGetHandler(useLegacyGetHandler);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
        config.setWriteMaxIntervalMs(3000L);
        config.setEnableDefaultForNotNullColumn(false);
        config.setAppName("testGet002");
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "test_schema.holo_client_get_002";
            String createSchema = "create schema if not exists test_schema";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text,i2 int2,i4 int4,i8 int8, primary key(id))";

            execute(conn, new String[] {createSchema, dropSql, createSql});

            try {
                TableSchema schema = client.getTableSchema(tableName);

                Put put = new Put(schema);
                put.setObject(0, 0);
                put.setObject(1, "name0");
                put.setObject(2, 5);
                put.setObject(3, 6);
                put.setObject(4, 7);
                client.put(put);

                client.flush();

                Record r =
                        client.get(
                                        Get.newBuilder(schema)
                                                .setPrimaryKey("id", 0)
                                                .withSelectedColumns(
                                                        new String[] {"i8", "i4", "i2"})
                                                .build())
                                .get();

                Assert.assertNull(r.getObject("name"));

                Object i2 = r.getObject("i2");
                Assert.assertEquals((short) 5, i2);

                Object i4 = r.getObject("i4");
                Assert.assertEquals(6, i4);

                Object i8 = r.getObject("i8");
                Assert.assertEquals(7L, i8);

            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** get when fail Method: put(Put put). */
    @Test(dataProvider = "useLegacyGetHandler")
    public void testGet003(boolean useLegacyGetHandler) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseLegacyGetHandler(useLegacyGetHandler);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
        config.setWriteMaxIntervalMs(3000L);
        config.setEnableDefaultForNotNullColumn(false);
        config.setAppName("testGet002");
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "test_schema.holo_client_get_003";
            String createSchema = "create schema if not exists test_schema";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text,i2 int2,i4 int4,i8 int8, primary key(id))";

            execute(conn, new String[] {createSchema, dropSql, createSql});

            try {
                TableSchema schema = client.getTableSchema(tableName);

                Put put = new Put(schema);
                put.setObject(0, 0);
                put.setObject(1, "name0");
                put.setObject(2, 5);
                put.setObject(3, 6);
                put.setObject(4, 7);
                client.put(put);

                client.flush();

                execute(conn, new String[] {dropSql});
                {
                    Assert.assertThrows(
                            HoloClientException.class,
                            () -> {
                                try {
                                    Record r =
                                            client.get(
                                                            Get.newBuilder(schema)
                                                                    .setPrimaryKey("id", 0)
                                                                    .withSelectedColumns(
                                                                            new String[] {
                                                                                "i8", "i4", "i2"
                                                                            })
                                                                    .build())
                                                    .get();
                                } catch (ExecutionException e) {
                                    throw e.getCause();
                                }
                            });
                }
                execute(conn, new String[] {createSql});

                put = new Put(schema);
                put.setObject(0, 0);
                put.setObject(1, "name0");
                put.setObject(2, 5);
                put.setObject(3, 6);
                put.setObject(4, 7);
                client.put(put);

                client.flush();
                {
                    Record r =
                            client.get(
                                            Get.newBuilder(schema)
                                                    .setPrimaryKey("id", 0)
                                                    .withSelectedColumns(
                                                            new String[] {"i8", "i4", "i2"})
                                                    .build())
                                    .get();
                    Assert.assertNull(r.getObject("name"));

                    Object i2 = r.getObject("i2");
                    Assert.assertEquals((short) 5, i2);

                    Object i4 = r.getObject("i4");
                    Assert.assertEquals(6, i4);

                    Object i8 = r.getObject("i8");
                    Assert.assertEquals(7L, i8);
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** get waiting timeout Method: put(Put put). */
    @Test(dataProvider = "useLegacyGetHandler")
    public void testGet004(boolean useLegacyGetHandler) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseLegacyGetHandler(useLegacyGetHandler);
        // 1ms 便于测试
        config.setReadTimeoutMilliseconds(1);
        config.setAppName("testGet004");
        config.setReadRetryCount(3);
        config.setReadThreadSize(1);
        config.setWriteBatchSize(1);
        try (Connection conn = buildConnection();
                ExecutionPool pool = ExecutionPool.buildOrGet("testGet004", config, false)) {
            String tableName = "test_schema.holo_client_get_004";
            String createSchema = "create schema if not exists test_schema";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table " + tableName + "(id int not null,name text, primary key(id))";

            execute(conn, new String[] {createSchema, dropSql, createSql});

            try {
                AtomicBoolean failed = new AtomicBoolean(false);
                ExecutorService es =
                        new ThreadPoolExecutor(
                                20,
                                20,
                                0L,
                                TimeUnit.MILLISECONDS,
                                new LinkedBlockingQueue<>(20),
                                r -> {
                                    Thread t = new Thread(r);
                                    return t;
                                },
                                new ThreadPoolExecutor.AbortPolicy());
                // 多线程单连接执行get, 部分get的等待时间可能大于1ms
                Runnable runnable =
                        () -> {
                            try {
                                HoloClient client = new HoloClient(config);
                                client.setPool(pool);
                                TableSchema schema = client.getTableSchema(tableName, true);
                                Put put = new Put(schema);
                                put.setObject(0, 0);
                                put.setObject(1, "name0");
                                client.put(put);
                                client.flush();
                                Record r =
                                        client.get(
                                                        Get.newBuilder(schema)
                                                                .setPrimaryKey("id", 0)
                                                                .build())
                                                .get();
                                Assert.assertEquals("name0", r.getObject("name"));
                            } catch (ExecutionException
                                    | HoloClientException
                                    | InterruptedException e) {
                                e.printStackTrace();
                                Assert.assertTrue(
                                        e.getMessage()
                                                .contains(
                                                        "get waiting timeout before submit to holo"));
                                // 其他error message则测试失败
                                if (!e.getMessage()
                                        .contains("get waiting timeout before submit to holo")) {
                                    failed.set(true);
                                }
                            }
                        };
                for (int i = 0; i < 20; ++i) {
                    es.execute(runnable);
                }
                es.shutdown();
                while (!es.awaitTermination(5000L, TimeUnit.MILLISECONDS)) {}

                if (failed.get()) {
                    Assert.fail();
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
        synchronized (this) {
            this.wait(5000L);
        }
    }

    /** times of get > 5, to test statement name is saved Method: put(Put put). */
    @Test(dataProvider = "useLegacyGetHandler")
    public void testGet005(boolean useLegacyGetHandler) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseLegacyGetHandler(useLegacyGetHandler);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
        config.setWriteMaxIntervalMs(3000L);
        config.setEnableDefaultForNotNullColumn(false);
        config.setAppName("testGet005");
        config.setWriteBatchSize(128);
        config.setReadThreadSize(10);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "test_schema.holo_client_get_005";
            String createSchema = "create schema if not exists test_schema";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text not null,address text,primary key(id));";
            execute(conn, new String[] {createSchema, dropSql, createSql});
            try {
                TableSchema schema = client.getTableSchema(tableName);
                for (int i = 0; i < 10000; i++) {
                    Put put = new Put(schema);
                    put.setObject(0, i);
                    put.setObject(1, "name0");
                    put.setObject(2, "address0");
                    client.put(put);
                }
                client.flush();
                for (int i = 1; i < 100; i++) {
                    Record r =
                            client.get(Get.newBuilder(schema).setPrimaryKey("id", i).build()).get();
                    Assert.assertEquals("name0", r.getObject("name"));
                    Assert.assertEquals("address0", r.getObject("address"));
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** get multi pk Method: put(Put put). */
    @Test(dataProvider = "useLegacyGetHandler")
    public void testGet006(boolean useLegacyGetHandler) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseLegacyGetHandler(useLegacyGetHandler);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
        config.setAppName("testGet006");
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "test_schema.holo_client_get_006";
            String createSchema = "create schema if not exists test_schema";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(key1 int, key2 text, key3 int2, key4 int8, col text, primary key(key1, key2, key3, key4))";

            execute(conn, new String[] {createSchema, dropSql, createSql});

            int recordNum = 100;
            try {
                TableSchema schema = client.getTableSchema(tableName);

                List<Get> gets = new ArrayList<>();
                for (int i = 0; i < recordNum; i++) {
                    Put put = new Put(schema);
                    put.setObject(0, i);
                    put.setObject(1, "name" + i);
                    put.setObject(2, i + 1);
                    put.setObject(3, i + 2);
                    put.setObject(4, "res" + i);
                    client.put(put);
                    client.flush();

                    gets.add(
                            Get.newBuilder(schema)
                                    .setPrimaryKey("key1", i)
                                    .setPrimaryKey("key2", "name" + i)
                                    .setPrimaryKey("key3", i + 1)
                                    .setPrimaryKey("key4", i + 2)
                                    .withSelectedColumns(new String[] {"col"})
                                    .build());
                }

                List<CompletableFuture<Record>> futs = client.get(gets);
                Assert.assertEquals(futs.size(), gets.size());
                for (int i = 0; i < gets.size(); i++) {
                    Record r = futs.get(i).get();
                    Assert.assertEquals("res" + i, r.getObject("col"));
                }

            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** get Method: test BatchQueueOfferTimeoutMs. */
    @Test
    public void testGet007() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
        config.setWriteMaxIntervalMs(3000L);
        config.setEnableDefaultForNotNullColumn(false);
        config.setReadBatchQueueSize(1);
        config.setReadBatchQueueOfferTimeoutMs(10);
        config.setAppName("testGet007");
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "test_schema.holo_client_get_007";
            String createSchema = "create schema if not exists test_schema";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text not null,address text,primary key(id))";

            execute(conn, new String[] {createSchema, dropSql, createSql});

            try {
                TableSchema schema = client.getTableSchema(tableName);

                int maxCount = 10;
                for (int i = 0; i < maxCount; i++) {
                    Put put = new Put(schema);
                    put.setObject(0, i);
                    put.setObject(1, "name0");
                    put.setObject(2, "address0");
                    client.put(put);
                }
                client.flush();

                List<CompletableFuture<Record>> rl = new ArrayList<>();
                for (int i = 0; i < maxCount; i++) {
                    CompletableFuture<Record> result = new CompletableFuture<>();
                    client.get(Get.newBuilder(schema).setPrimaryKey("id", i).build())
                            .handleAsync(
                                    (r, throwable) -> {
                                        try {
                                            // caught an error
                                            if (throwable != null) {
                                                result.completeExceptionally(throwable);
                                            } else {
                                                if (r == null) {
                                                    result.complete(null);
                                                } else {
                                                    result.complete(r);
                                                }
                                            }
                                        } catch (Throwable e) {
                                            result.completeExceptionally(e);
                                        }
                                        return null;
                                    });
                    rl.add(result);
                }
                for (CompletableFuture<Record> f : rl) {
                    try {
                        f.get();
                    } catch (Exception e) {
                        e.printStackTrace();
                        Assert.assertTrue(
                                e.getMessage()
                                        .contains(
                                                "The get request queue is full. Consider increasing the readThreadSize."));
                        break;
                    }
                }

            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** table id change: a dim table is replaced during its use is a common use case. */
    @Test
    public void testGet008() throws Exception {
        if (properties == null) {
            return;
        }
        boolean[] useFixedfes = new boolean[] {false, true};
        for (boolean useFixed : useFixedfes) {
            HoloConfig config = buildConfig();
            config.setAppName("testGet008");
            config.setReadRetryCount(3);
            config.setReadThreadSize(1);
            config.setWriteBatchSize(1);
            config.setUseFixedFe(useFixed);
            try (Connection conn = buildConnection()) {
                String tableName = "holo_client_get_008";
                String tableName1 = "holo_client_get_008_1";
                String dropSql = "drop table if exists " + tableName;
                String dropSql1 = "drop table if exists " + tableName1;
                String createSql =
                        "create table "
                                + tableName
                                + "(id int not null,name text, primary key(id))";
                String createSql1 =
                        "create table "
                                + tableName1
                                + "(id int not null,name text, primary key(id))";
                String renameSql =
                        String.format(
                                "begin; drop table %s; alter table %s rename to %s; end;",
                                tableName, tableName1, tableName);

                execute(conn, new String[] {dropSql, dropSql1, createSql, createSql1});

                try {
                    try (HoloClient client = new HoloClient(config)) {
                        TableSchema schema = client.getTableSchema(tableName, true);
                        for (int i = 0; i < 100; i++) {
                            Put put = new Put(schema);
                            put.setObject(0, i);
                            put.setObject(1, "name0");
                            client.put(put);
                        }
                        client.flush();
                        TableSchema schema1 = client.getTableSchema(tableName1, true);
                        for (int i = 0; i < 100; i++) {
                            Put put = new Put(schema1);
                            put.setObject(0, i);
                            put.setObject(1, "name1");
                            client.put(put);
                        }
                        client.flush();
                    }
                    AtomicBoolean failed = new AtomicBoolean(false);
                    ExecutorService es =
                            new ThreadPoolExecutor(
                                    2,
                                    2,
                                    0L,
                                    TimeUnit.MILLISECONDS,
                                    new LinkedBlockingQueue<>(20),
                                    r -> {
                                        Thread t = new Thread(r);
                                        return t;
                                    },
                                    new ThreadPoolExecutor.AbortPolicy());
                    // 多线程单连接执行get, 部分get的等待时间可能大于1ms
                    Runnable runnable =
                            () -> {
                                try {
                                    int count = 0;
                                    int count1 = 0;
                                    HoloClient client = new HoloClient(config);
                                    TableSchema schema = client.getTableSchema(tableName, true);
                                    for (int i = 0; i < 100; i++) {
                                        Record r =
                                                client.get(
                                                                Get.newBuilder(schema)
                                                                        .setPrimaryKey("id", i)
                                                                        .build())
                                                        .get();
                                        if (r.getObject("name").equals("name0")) {
                                            count++;
                                        } else if (r.getObject("name").equals("name1")) {
                                            count1++;
                                        }
                                        Thread.sleep(50);
                                    }
                                    LOG.info(
                                            "before rename count: "
                                                    + count
                                                    + ", after rename count: "
                                                    + count1);
                                    Assert.assertEquals(count + count1, 100);
                                    if (count + count1 != 100) {
                                        failed.set(true);
                                    }
                                } catch (Exception e) {
                                    failed.set(true);
                                    e.printStackTrace();
                                }
                            };
                    Runnable rename =
                            () -> {
                                try {
                                    Thread.sleep(3000L);
                                    execute(conn, new String[] {renameSql});
                                } catch (Exception e) {
                                    LOG.error("testGet008", e);
                                    // 走到这里表示测试失败，如果进入重试不会走到这里
                                    failed.set(true);
                                }
                            };
                    es.execute(runnable);
                    es.execute(rename);
                    es.shutdown();
                    while (!es.awaitTermination(5000L, TimeUnit.MILLISECONDS)) {}

                    if (failed.get()) {
                        Assert.fail();
                    }
                } finally {
                    execute(conn, new String[] {dropSql, dropSql1});
                }
            }
            synchronized (this) {
                this.wait(5000L);
            }
        }
    }

    /** get partition table. */
    @Test(dataProvider = "useLegacyGetHandler")
    public void testGet009(boolean useLegacyGetHandler) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseLegacyGetHandler(useLegacyGetHandler);
        config.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
        config.setWriteMaxIntervalMs(3000L);
        config.setDynamicPartition(true);
        config.setEnableDefaultForNotNullColumn(false);
        config.setUseFixedFe(true);
        // 调整前,即使已经发现分区不存在,get仍然会被collector收集,尝试查询分区父表且抛异常重试
        config.setReadRetryCount(10);
        config.setAppName("testGet009");
        long start = System.currentTimeMillis();
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "test_schema.holo_client_get_009";
            String createSchema = "create schema if not exists test_schema";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text not null,address text, ds text, primary key(id, ds)) partition by list (ds)";

            execute(conn, new String[] {createSchema, dropSql, createSql});

            try {
                TableSchema schema = client.getTableSchema(tableName);
                Put put = new Put(schema);
                put.setObject(0, 1);
                put.setObject(1, "name0");
                put.setObject(2, "address0");
                put.setObject(3, "20250209");
                client.put(put);
                client.flush();

                Record r =
                        client.get(
                                        Get.newBuilder(schema)
                                                .setPrimaryKey("id", 1)
                                                .setPrimaryKey("ds", "20250209")
                                                .build())
                                .get();
                Assert.assertEquals("name0", r.getObject("name"));
                Assert.assertEquals("address0", r.getObject("address"));
                Assert.assertEquals("20250209", r.getObject("ds"));

                String notExistPartValue = "20250211";
                r =
                        client.get(
                                        Get.newBuilder(schema)
                                                .setPrimaryKey("id", 2)
                                                .setPrimaryKey("ds", notExistPartValue)
                                                .build())
                                .get();
                Assert.assertNull(r);

                List<Get> gets = new ArrayList<>();
                gets.add(
                        Get.newBuilder(schema)
                                .setPrimaryKey("id", 1)
                                .setPrimaryKey("ds", "20250209")
                                .build());
                gets.add(
                        Get.newBuilder(schema)
                                .setPrimaryKey("id", 2)
                                .setPrimaryKey("ds", notExistPartValue)
                                .build());
                List<CompletableFuture<Record>> rs = client.get(gets);
                Assert.assertEquals("name0", rs.get(0).get().getObject("name"));
                Assert.assertEquals("address0", rs.get(0).get().getObject("address"));
                Assert.assertEquals("20250209", rs.get(0).get().getObject("ds"));
                Assert.assertNull(rs.get(1).get());
                long duration = System.currentTimeMillis() - start;
                if (duration > 60 * 1000L) {
                    throw new RuntimeException("testGet009 timeout because retry inner.");
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }
}
