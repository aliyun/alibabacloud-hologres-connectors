package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.RecordScanner;
import com.alibaba.hologres.client.model.TableSchema;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;

/** HoloClientPrefixScanTest. */
public class HoloClientScanTest extends HoloClientTestBase {

    /** scan Method: put(Put put). */
    @Test
    public void testPrefixScan001() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
        config.setWriteMaxIntervalMs(3000L);
        config.setEnableDefaultForNotNullColumn(false);
        config.setAppName("testPrefixScan001");
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "test_schema.holo_client_prefix_scan_001";
            String createSchema = "create schema if not exists test_schema";
            String dropSql = "drop table if exists " + tableName;
            // now prefix scan just support "row" or "row,column" table
            String createSql =
                    "create table "
                            + tableName
                            + "(pk1 int, pk2 int, pk3 int, pk4 int, name text not null, address text, primary key(pk1, pk2, pk3, "
                            + "pk4)); "
                            + "call set_table_property('"
                            + tableName
                            + "', 'distribution_key', 'pk1,pk3');"
                            + "call set_table_property('"
                            + tableName
                            + "', 'orientation', 'row,column');";

            execute(conn, new String[] {createSchema, dropSql, createSql});

            try {
                TableSchema schema = client.getTableSchema(tableName);

                for (int i = 0; i < 4; i++) {
                    Put put = new Put(schema);
                    put.setObject(0, 1);
                    put.setObject(1, 2);
                    put.setObject(2, 3);
                    put.setObject(3, i);
                    put.setObject(4, "name0");
                    put.setObject(5, "address0");
                    client.put(put);
                }
                client.flush();
                try (RecordScanner scanner =
                        client.scan(
                                Scan.newBuilder(schema)
                                        .setSortKeys(SortKeys.NONE)
                                        .addEqualFilter("pk1", 1)
                                        .addEqualFilter("pk2", 2)
                                        .addEqualFilter("pk3", 3)
                                        .build())) {
                    int count = 0;
                    // prefix scan 的返回是按照primary key排序的
                    while (scanner.next()) {
                        Record r = scanner.getRecord();
                        System.out.println(count + ": " + r);
                        Assert.assertEquals(1, r.getObject("pk1"));
                        Assert.assertEquals(2, r.getObject("pk2"));
                        Assert.assertEquals(3, r.getObject("pk3"));
                        Assert.assertEquals(count, r.getObject("pk4"));
                        Assert.assertEquals("name0", r.getObject("name"));
                        Assert.assertEquals("address0", r.getObject("address"));
                        ++count;
                    }
                    Assert.assertEquals(4, count);
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** scan when fail Method: put(Put put). */
    @Test
    public void testPrefixScan002() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
        config.setWriteMaxIntervalMs(3000L);
        config.setEnableDefaultForNotNullColumn(false);
        config.setAppName("testPrefixScan001");
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "test_schema.holo_client_prefix_scan_002";
            String createSchema = "create schema if not exists test_schema";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(pk1 int, pk2 int, pk3 int, pk4 int, name text not null, address text, primary key(pk1, pk2, pk3, "
                            + "pk4)); "
                            + "call set_table_property('"
                            + tableName
                            + "', 'distribution_key', 'pk1,pk3');"
                            + "call set_table_property('"
                            + tableName
                            + "', 'orientation', 'row,column');";

            execute(conn, new String[] {createSchema, dropSql, createSql});

            try {
                {
                    TableSchema schema = client.getTableSchema(tableName);

                    for (int i = 0; i < 4; i++) {
                        Put put = new Put(schema);
                        put.setObject(0, 1);
                        put.setObject(1, 2);
                        put.setObject(2, 3);
                        put.setObject(3, i);
                        put.setObject(4, "name0");
                        put.setObject(5, "address0");
                        client.put(put);
                    }
                    client.flush();

                    execute(conn, new String[] {dropSql});
                    {
                        Assert.assertThrows(
                                HoloClientException.class,
                                () -> {
                                    RecordScanner scanner =
                                            client.scan(
                                                    Scan.newBuilder(schema)
                                                            .setSortKeys(SortKeys.NONE)
                                                            .addEqualFilter("pk1", 1)
                                                            .addEqualFilter("pk2", 2)
                                                            .addEqualFilter("pk3", 3)
                                                            .build());
                                    if (scanner.next()) {
                                        scanner.getRecord();
                                    }
                                });
                    }
                }
                execute(conn, new String[] {createSql});
                TableSchema schema = client.getTableSchema(tableName);

                for (int i = 0; i < 4; i++) {
                    Put put = new Put(schema);
                    put.setObject(0, 1);
                    put.setObject(1, 2);
                    put.setObject(2, 3);
                    put.setObject(3, i);
                    put.setObject(4, "name0");
                    put.setObject(5, "address0");
                    client.put(put);
                }
                client.flush();

                try (RecordScanner scanner =
                        client.scan(
                                Scan.newBuilder(schema)
                                        .setSortKeys(SortKeys.NONE)
                                        .addEqualFilter("pk1", 1)
                                        .addEqualFilter("pk2", 2)
                                        .addEqualFilter("pk3", 3)
                                        .addRangeFilter("pk4", 0, 3)
                                        .build())) {
                    int count = 0;
                    while (scanner.next()) {
                        Record r = scanner.getRecord();
                        Assert.assertEquals(1, r.getObject("pk1"));
                        Assert.assertEquals(2, r.getObject("pk2"));
                        Assert.assertEquals(3, r.getObject("pk3"));
                        Assert.assertEquals(count, r.getObject("pk4"));
                        Assert.assertEquals("name0", r.getObject("name"));
                        Assert.assertEquals("address0", r.getObject("address"));
                        ++count;
                    }
                    Assert.assertEquals(count, 3);
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** not a prefix scan Method: put(Put put). */
    @Test
    public void testPrefixScan003() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
        config.setWriteMaxIntervalMs(3000L);
        config.setEnableDefaultForNotNullColumn(false);
        config.setAppName("testPrefixScan003");
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "test_schema.holo_client_prefix_scan_003";
            String createSchema = "create schema if not exists test_schema";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(pk1 int, pk2 int, pk3 int, pk4 int, name text not null, address text, primary key(pk1, pk2, pk3, "
                            + "pk4)); "
                            + "call set_table_property('"
                            + tableName
                            + "', 'distribution_key', 'pk1,pk3');"
                            + "call set_table_property('"
                            + tableName
                            + "', 'orientation', 'row,column');";

            execute(conn, new String[] {createSchema, dropSql, createSql});

            try {
                TableSchema schema = client.getTableSchema(tableName);

                for (int i = 0; i < 4; i++) {
                    Put put = new Put(schema);
                    put.setObject(0, 1);
                    put.setObject(1, 2);
                    put.setObject(2, 3);
                    put.setObject(3, i);
                    put.setObject(4, "name0");
                    put.setObject(5, "address0");
                    client.put(put);
                }
                client.flush();

                try (RecordScanner scanner =
                        client.scan(
                                Scan.newBuilder(schema)
                                        .setSortKeys(SortKeys.NONE)
                                        .addEqualFilter("pk1", 1)
                                        .addEqualFilter("pk2", 2)
                                        .build())) {
                    if (scanner.next()) {
                        scanner.getRecord();
                    }
                } catch (HoloClientException e) {
                    e.printStackTrace();
                    if (e.getCode() != ExceptionCode.NOT_SUPPORTED) {
                        Assert.fail();
                    }
                }

                try (RecordScanner scanner =
                        client.scan(
                                Scan.newBuilder(schema)
                                        .setSortKeys(SortKeys.NONE)
                                        .addEqualFilter("pk1", 1)
                                        .addEqualFilter("pk2", 2)
                                        .addEqualFilter("pk3", 3)
                                        .addRangeFilter("pk4", 0, 3)
                                        .build()); ) {
                    int count = 0;
                    while (scanner.next()) {
                        Record r = scanner.getRecord();
                        System.out.println(count + ": " + r);
                        Assert.assertEquals(1, r.getObject("pk1"));
                        Assert.assertEquals(2, r.getObject("pk2"));
                        Assert.assertEquals(3, r.getObject("pk3"));
                        Assert.assertEquals(count, r.getObject("pk4"));
                        Assert.assertEquals("name0", r.getObject("name"));
                        Assert.assertEquals("address0", r.getObject("address"));
                        ++count;
                    }
                    Assert.assertEquals(count, 3);
                }

            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** times of prefix scan > 5, to test statement name is saved Method: put(Put put). */
    @Test
    public void testPrefixScan005() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
        config.setWriteMaxIntervalMs(3000L);
        config.setEnableDefaultForNotNullColumn(false);
        config.setAppName("testPrefixScan005");
        config.setWriteBatchSize(128);
        config.setReadThreadSize(10);
        config.setScanTimeoutSeconds(10);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "test_schema.holo_client_prefix_scan_005";
            String createSchema = "create schema if not exists test_schema";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(pk1 int, pk2 int, pk3 int, pk4 int, name text not null, address text, primary key(pk1, pk2, pk3, "
                            + "pk4)); "
                            + "call set_table_property('"
                            + tableName
                            + "', 'distribution_key', 'pk1,pk3');"
                            + "call set_table_property('"
                            + tableName
                            + "', 'orientation', 'row,column');";

            execute(conn, new String[] {createSchema, dropSql, createSql});

            try {
                TableSchema schema = client.getTableSchema(tableName);

                for (int i = 0; i < 100; i++) {
                    Put put = new Put(schema);
                    put.setObject(0, 1);
                    put.setObject(1, 2);
                    put.setObject(2, 3);
                    put.setObject(3, i);
                    put.setObject(4, "name0");
                    put.setObject(5, "address0");
                    client.put(put);
                }
                client.flush();

                try (RecordScanner scanner =
                        client.scan(
                                Scan.newBuilder(schema)
                                        .setSortKeys(SortKeys.NONE)
                                        .addEqualFilter("pk1", 1)
                                        .addEqualFilter("pk2", 2)
                                        .addEqualFilter("pk3", 3)
                                        .addRangeFilter("pk4", 0, 10)
                                        .build())) {
                    int count = 0;
                    // 非prefix scan时顺序是无法保证的，会抛出异常
                    while (scanner.next()) {
                        Record r = scanner.getRecord();
                        System.out.println(count + ": " + r);
                        Assert.assertEquals(1, r.getObject("pk1"));
                        Assert.assertEquals(2, r.getObject("pk2"));
                        Assert.assertEquals(3, r.getObject("pk3"));
                        Assert.assertEquals(count, r.getObject("pk4"));
                        Assert.assertEquals("name0", r.getObject("name"));
                        Assert.assertEquals("address0", r.getObject("address"));
                        ++count;
                    }
                    Assert.assertEquals(count, 10);
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** prefix scan and fetchsize < expected rows count. Method: Scan. */
    @Test
    public void testPrefixScan006() throws Exception {
        if (properties == null) {
            return;
        }

        HoloConfig config = buildConfig();
        config.setScanFetchSize(5);
        config.setUseFixedFe(false);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "test_schema.test_prefix_scan_with_fetch_size";
            String createSchema = "create schema if not exists test_schema";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(pk1 text, pk2 int, name text not null, address text, primary key(pk1, pk2)); "
                            + "call set_table_property('"
                            + tableName
                            + "', 'distribution_key', 'pk1');"
                            + "call set_table_property('"
                            + tableName
                            + "', 'orientation', 'row,column');";

            execute(conn, new String[] {createSchema, dropSql, createSql});

            try {
                TableSchema schema = client.getTableSchema(tableName);
                Set<Integer> sets = new HashSet<>();
                for (int i = 0; i < 12; i++) {
                    Put put = new Put(schema);
                    put.setObject(0, "123456");
                    put.setObject(1, i);
                    sets.add(i);
                    put.setObject(2, "name0");
                    put.setObject(3, "address0");
                    client.put(put);
                }
                client.flush();

                try (RecordScanner scanner =
                        client.scan(
                                Scan.newBuilder(schema)
                                        .setSortKeys(SortKeys.NONE)
                                        .addEqualFilter("pk1", "123456")
                                        .build())) {
                    int count = 0;
                    while (scanner.next()) {
                        Record r = scanner.getRecord();
                        Assert.assertEquals(r.getObject("pk1"), "123456");
                        sets.remove((Integer) r.getObject("pk2"));
                        Assert.assertEquals(r.getObject("name"), "name0");
                        Assert.assertEquals(r.getObject("address"), "address0");
                        ++count;
                        if (count > 12) {
                            throw new RuntimeException(
                                    String.format(
                                            "the number of records %s is incorrect, exceeds the real number %s",
                                            count, 12));
                        }
                    }
                    Assert.assertTrue(sets.isEmpty());
                    Assert.assertEquals(count, 12);
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** scan, order by. Method: Scan. */
    @Test
    public void testScan001() throws Exception {
        if (properties == null) {
            return;
        }

        HoloConfig config = buildConfig();
        config.setScanFetchSize(5);
        config.setUseFixedFe(false);
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "test_schema.test_scan_order_by_001";
            String createSchema = "create schema if not exists test_schema";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(pk1 text, \"PK2\" int, \"c1:asc\" text not null, \"c2:desc\" text not null, \"C:3\" text not null, address text, primary key(pk1, \"PK2\")) "
                            + "with (clustering_key='\"c1:asc\",\"c2:desc\":desc,\"C:3\"');";

            execute(conn, new String[] {createSchema, dropSql, createSql});

            try {
                TableSchema schema = client.getTableSchema(tableName);
                Assert.assertEquals(
                        schema.getClusteringKey(),
                        new String[] {"\"c1:asc\":asc", "\"c2:desc\":desc", "\"C:3\":asc"});
                for (int i = 0; i < 3; i++) {
                    Put put = new Put(schema);
                    put.setObject(0, "123456");
                    put.setObject(1, i);
                    put.setObject(2, "name" + (2 - i));
                    put.setObject(3, "name" + (2 - i));
                    put.setObject(4, "name" + (2 - i));
                    put.setObject(5, "address0");
                    client.put(put);
                }
                client.flush();

                try (RecordScanner scanner =
                        client.scan(
                                Scan.newBuilder(schema)
                                        .setSortKeys(SortKeys.PRIMARY_KEY)
                                        .build())) {
                    int count = 0;
                    while (scanner.next()) {
                        Record r = scanner.getRecord();
                        Assert.assertEquals(r.getObject("pk1"), "123456");
                        Assert.assertEquals(r.getObject("PK2"), count);
                        Assert.assertEquals(r.getObject("c1:asc"), "name" + (2 - count));
                        Assert.assertEquals(r.getObject("c2:desc"), "name" + (2 - count));
                        Assert.assertEquals(r.getObject("C:3"), "name" + (2 - count));
                        Assert.assertEquals(r.getObject("address"), "address0");
                        ++count;
                    }
                    Assert.assertEquals(count, 3);
                }
                try (RecordScanner scanner =
                        client.scan(
                                Scan.newBuilder(schema)
                                        .setSortKeys(SortKeys.CLUSTERING_KEY)
                                        .build())) {
                    int count = 0;
                    while (scanner.next()) {
                        Record r = scanner.getRecord();
                        Assert.assertEquals(r.getObject("pk1"), "123456");
                        Assert.assertEquals(r.getObject("PK2"), 2 - count);
                        Assert.assertEquals(r.getObject("c1:asc"), "name" + count);
                        Assert.assertEquals(r.getObject("c2:desc"), "name" + count);
                        Assert.assertEquals(r.getObject("C:3"), "name" + count);
                        Assert.assertEquals(r.getObject("address"), "address0");
                        ++count;
                    }
                    Assert.assertEquals(count, 3);
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }
}
