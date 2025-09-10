package com.alibaba.hologres.client;

import com.alibaba.hologres.client.copy.CopyFormat;
import com.alibaba.hologres.client.copy.CopyUtil;
import com.alibaba.hologres.client.copy.out.CopyOutInputStream;
import com.alibaba.hologres.client.copy.out.CopyOutWrapper;
import com.alibaba.hologres.client.copy.out.arrow.ArrowReader;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.DataTypeTestUtil;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.PgConnection;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.hologres.client.utils.DataTypeTestUtil.FIXED_PLAN_TYPE_DATA_WITH_RECORD;

/** CopyOutTest. 通过copy以arrow格式从hologres中读取数据. */
public class CopyOutTest extends HoloClientTestBase {

    @DataProvider(name = "createDataForCopyOut")
    public Object[][] createDataForCopyOut() {
        DataTypeTestUtil.TypeCaseDataWithRecord[] typeToTest = FIXED_PLAN_TYPE_DATA_WITH_RECORD;
        // 只测试fixed plan支持的类型
        Object[][] ret = new Object[typeToTest.length * 2][];
        for (int i = 0; i < typeToTest.length; ++i) {
            ret[2 * i] = new Object[] {typeToTest[i], true};
            ret[2 * i + 1] = new Object[] {typeToTest[i], false};
        }
        return ret;
    }

    @Test(dataProvider = "createDataForCopyOut")
    public void arrowReaderDateTypeTest(
            DataTypeTestUtil.TypeCaseDataWithRecord typeCaseData, boolean enableCompressArrow)
            throws HoloClientException {
        if (properties == null) {
            return;
        }
        if (enableCompressArrow && holoVersion.compareTo(new HoloVersion(3, 0, 24)) < 0) {
            return;
        }
        boolean isJsonb = typeCaseData.getName().equals("jsonb");
        HoloConfig config = buildConfig();
        config.setUseFixedFe(false);
        config.setBinlogReadBatchSize(128);

        final int totalCount = 10;
        final int nullPkId = 5;
        String typeName = typeCaseData.getName();

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_type_arrow_reader_" + typeName;
            String dropSql1 = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id "
                            + typeCaseData.getColumnType()
                            + ", pk int primary key) with (binlog_level='replica',table_group='tg_1')\n";

            execute(conn, new String[] {dropSql1});
            execute(conn, new String[] {createSql});
            try {
                TableSchema schema = client.getTableSchema(tableName, true);

                for (int i = 0; i < totalCount; ++i) {
                    Record record = Record.build(schema);
                    if (i == nullPkId) {
                        record.setObject(0, null);
                    } else {
                        record.setObject(
                                0,
                                typeCaseData
                                        .getSupplier()
                                        .apply(i, conn.unwrap(BaseConnection.class)));
                    }
                    record.setObject(1, i);
                    client.put(new Put(record));
                }
                client.flush();

                try (CopyOutWrapper copyOut =
                        new CopyOutWrapper(
                                conn,
                                tableName,
                                Arrays.stream(schema.getColumnSchema())
                                        .map(Column::getName)
                                        .collect(Collectors.toList()),
                                enableCompressArrow ? CopyFormat.ARROW_LZ4 : CopyFormat.ARROW,
                                Collections.emptyList(),
                                "",
                                1024 * 10 * 10)) {
                    int count = 0;
                    while (copyOut.hasNextBatch()) {
                        List<Record> records = copyOut.getRecords();
                        for (Record r : records) {
                            int pk = (int) r.getObject(1);
                            if (pk == nullPkId) {
                                Assert.assertNull(r.getObject(0));
                            } else {
                                typeCaseData.getPredicate().run(pk, r);
                            }
                            count++;
                        }
                    }
                    Assert.assertFalse(copyOut.hasNextBatch());
                    Assert.assertFalse(copyOut.hasNextBatch());
                    Assert.assertEquals(count, totalCount);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } finally {
                execute(conn, new String[] {dropSql1});
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /** arrowReaderDateTypeTest. 表中没有数据. */
    @Test()
    public void arrowReaderDateTypeTest001() throws HoloClientException {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setUseFixedFe(false);
        config.setBinlogReadBatchSize(128);
        boolean[] enableCompressArrows =
                new boolean[] {false, holoVersion.compareTo(new HoloVersion(3, 0, 24)) >= 0};

        for (boolean enableCompressArrow : enableCompressArrows) {
            try (Connection conn = buildConnection();
                    HoloClient client = new HoloClient(config)) {
                String tableName = "holo_client_type_arrow_reader_no_data";
                String dropSql1 = "drop table if exists " + tableName;
                String createSql =
                        "create table "
                                + tableName
                                + "(id int"
                                + ", pk int primary key) with (binlog_level='replica',table_group='tg_1')\n";

                execute(conn, new String[] {dropSql1});
                execute(conn, new String[] {createSql});
                try {
                    TableSchema schema = client.getTableSchema(tableName, true);

                    PgConnection pgConn = conn.unwrap(PgConnection.class);

                    HoloVersion version = ConnectionUtil.getHoloVersion(pgConn);
                    ConnectionUtil.checkMeta(
                            pgConn, version, TableName.valueOf(tableName).getFullName(), 120);
                    CopyManager copyAPI = pgConn.getCopyAPI();
                    String copySql =
                            CopyUtil.buildCopyOutSql(
                                    schema,
                                    Collections.emptyList(),
                                    enableCompressArrow ? CopyFormat.ARROW_LZ4 : CopyFormat.ARROW,
                                    "",
                                    Collections.emptySet());
                    LOG.info("copySql : {}", copySql);

                    try (CopyOutInputStream is = new CopyOutInputStream(copyAPI.copyOut(copySql))) {
                        try (ArrowReader ais = new ArrowReader(is, false)) {
                            Assert.assertFalse(ais.nextBatch());
                            // nextBatch 重复调用也不应该报错, 类似 ResultSet.next
                            Assert.assertFalse(ais.nextBatch());
                            Assert.assertFalse(ais.nextBatch());
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } finally {
                    // execute(conn, new String[] {dropSql1});
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** test buildCopyOutSql. */
    @Test
    public void testCopyOutUtil001() throws Exception {
        if (properties == null) {
            return;
        }
        try (Connection conn = buildConnection()) {

            String tableName = "\"holo_client_copy_out_util_001\"";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text not null,address text,primary key(id))";
            try {
                execute(conn, new String[] {dropSql});
                execute(conn, new String[] {createSql});
                TableName tn = TableName.valueOf(tableName);
                TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);
                // id , name
                List<String> columns = new ArrayList<>();
                columns.add("id");
                columns.add("name");
                List<Integer> shards = new ArrayList<>();
                shards.add(0);
                shards.add(1);
                String filter = "where id > 0";

                // with shards, arrow = true, with filter
                Assert.assertEquals(
                        CopyUtil.buildCopyOutSql(
                                schema, shards, CopyFormat.ARROW, filter, Collections.emptySet()),
                        "copy (select id,name,address from \"public\".\"holo_client_copy_out_util_001\" where hg_shard_id in (0,1) and id > 0) to stdout with (format arrow)");
                // without shards, arrow = true, with filter
                Assert.assertEquals(
                        CopyUtil.buildCopyOutSql(
                                schema,
                                Collections.emptyList(),
                                CopyFormat.ARROW,
                                filter,
                                Collections.emptySet()),
                        "copy (select id,name,address from \"public\".\"holo_client_copy_out_util_001\" where id > 0) to stdout with (format arrow)");

                // without shards, arrow = true, lz4 = true, without filter
                Assert.assertEquals(
                        CopyUtil.buildCopyOutSql(
                                schema,
                                Collections.emptyList(),
                                CopyFormat.ARROW_LZ4,
                                "",
                                Collections.emptySet()),
                        "copy (select id,name,address from \"public\".\"holo_client_copy_out_util_001\") to stdout with (format arrow_lz4)");

                // with shards, arrow = true, with filter
                Assert.assertEquals(
                        CopyUtil.buildCopyOutSql(
                                tableName,
                                columns,
                                shards,
                                CopyFormat.ARROW,
                                filter,
                                Collections.emptySet()),
                        "copy (select id,name from \"holo_client_copy_out_util_001\" where hg_shard_id in (0,1) and id > 0) to stdout with (format arrow)");
                // without shards, arrow = true, with filter
                Assert.assertEquals(
                        CopyUtil.buildCopyOutSql(
                                tableName,
                                columns,
                                Collections.emptyList(),
                                CopyFormat.ARROW,
                                filter,
                                Collections.emptySet()),
                        "copy (select id,name from \"holo_client_copy_out_util_001\" where id > 0) to stdout with (format arrow)");

                // without shards, arrow = false, without filter
                Assert.assertEquals(
                        CopyUtil.buildCopyOutSql(
                                tableName,
                                columns,
                                Collections.emptyList(),
                                CopyFormat.ARROW_LZ4,
                                "",
                                Collections.emptySet()),
                        "copy (select id,name from \"holo_client_copy_out_util_001\") to stdout with (format arrow_lz4)");

                // with jsonb columns
                Assert.assertEquals(
                        CopyUtil.buildCopyOutSql(
                                tableName,
                                columns,
                                Collections.emptyList(),
                                CopyFormat.ARROW_LZ4,
                                "",
                                Collections.singleton("name")),
                        "copy (select id,name::text from \"holo_client_copy_out_util_001\") to stdout with (format arrow_lz4)");
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    @Test
    public void copyOutWrapperTest() throws HoloClientException {
        if (properties == null) {
            return;
        }
        boolean[] enableCompressArrows =
                new boolean[] {false, holoVersion.compareTo(new HoloVersion(3, 0, 24)) >= 0};

        for (boolean enableCompressArrow : enableCompressArrows) {
            HoloConfig config = buildConfig();
            config.setUseFixedFe(false);
            config.setBinlogReadBatchSize(128);

            final int totalCount = 10;
            final int nullPkId = 5;

            try (Connection conn = buildConnection();
                    HoloClient client = new HoloClient(config)) {
                String tableName = "holo_client_copy_out_wrapper" + enableCompressArrow;
                String dropSql1 = "drop table if exists " + tableName;
                String createSql =
                        "create table "
                                + tableName
                                + "(id int, name text, address text, primary key(id))";

                execute(conn, new String[] {dropSql1});
                execute(conn, new String[] {createSql});
                try {
                    TableSchema schema = client.getTableSchema(tableName, true);

                    for (int i = 0; i < totalCount; ++i) {
                        Record record = Record.build(schema);
                        record.setObject(0, i);
                        if (i == nullPkId) {
                            record.setObject(1, null);
                        } else {
                            record.setObject(1, "name_" + i);
                        }
                        record.setObject(2, "address_" + i);

                        client.put(new Put(record));
                    }
                    client.flush();

                    List<String> columns = new ArrayList<>();
                    columns.add("id");
                    columns.add("name");
                    columns.add("address");
                    try (CopyOutWrapper copyOutWrapper =
                            new CopyOutWrapper(
                                    conn,
                                    tableName,
                                    columns,
                                    enableCompressArrow ? CopyFormat.ARROW : CopyFormat.ARROW_LZ4,
                                    Collections.emptyList(),
                                    "",
                                    1024 * 1024 * 20)) {
                        int count = 0;
                        while (copyOutWrapper.hasNextBatch()) {
                            List<Record> records = copyOutWrapper.getRecords();
                            for (Record r : records) {
                                int pk = (int) r.getObject(0);
                                if (pk == nullPkId) {
                                    Assert.assertNull(r.getObject(1));
                                } else {
                                    Assert.assertEquals("name_" + pk, r.getObject(1));
                                }
                                Assert.assertEquals("address_" + pk, r.getObject(2));
                                count++;
                            }
                        }

                        Assert.assertEquals(count, totalCount);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } finally {
                    execute(conn, new String[] {dropSql1});
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void copyOutMultiJsonbColumnsTest() throws HoloClientException {
        if (properties == null) {
            return;
        }
        boolean[] enableCompressArrows =
                new boolean[] {false, holoVersion.compareTo(new HoloVersion(3, 0, 24)) >= 0};

        for (boolean enableCompressArrow : enableCompressArrows) {
            HoloConfig config = buildConfig();
            config.setUseFixedFe(false);
            config.setBinlogReadBatchSize(128);

            final int totalCount = 10;
            final int nullPkId = 5;

            try (Connection conn = buildConnection();
                    HoloClient client = new HoloClient(config)) {
                String tableName = "holo_client_copy_out_multi_jsonb" + enableCompressArrow;
                String dropSql1 = "drop table if exists " + tableName;
                String createSql =
                        "create table "
                                + tableName
                                + "(id int, name text, address jsonb, address1 jsonb,address2 jsonb, primary key(id))";

                execute(conn, new String[] {dropSql1});
                execute(conn, new String[] {createSql});
                try {
                    TableSchema schema = client.getTableSchema(tableName, true);

                    for (int i = 0; i < totalCount; ++i) {
                        Record record = Record.build(schema);
                        record.setObject(0, i);
                        if (i == nullPkId) {
                            record.setObject(1, null);
                        } else {
                            record.setObject(1, "name_" + i);
                        }
                        record.setObject(2, "{\"a\": \"b\"}");
                        record.setObject(3, "{\"a\": \"c\"}");
                        record.setObject(4, "{\"a\": \"d\"}");

                        client.put(new Put(record));
                    }
                    client.flush();

                    List<String> columns = new ArrayList<>();
                    columns.add("id");
                    columns.add("name");
                    columns.add("address");
                    columns.add("address1");
                    columns.add("address2");
                    try (CopyOutWrapper copyOutWrapper =
                            new CopyOutWrapper(
                                    conn,
                                    tableName,
                                    columns,
                                    enableCompressArrow ? CopyFormat.ARROW : CopyFormat.ARROW_LZ4,
                                    Collections.emptyList(),
                                    "",
                                    1024 * 1024 * 20)) {
                        int count = 0;
                        while (copyOutWrapper.hasNextBatch()) {
                            List<Record> records = copyOutWrapper.getRecords();
                            for (Record r : records) {
                                int pk = (int) r.getObject(0);
                                if (pk == nullPkId) {
                                    Assert.assertNull(r.getObject(1));
                                } else {
                                    Assert.assertEquals("name_" + pk, r.getObject(1));
                                }
                                Assert.assertEquals("{\"a\": \"b\"}", r.getObject(2));
                                Assert.assertEquals("{\"a\": \"c\"}", r.getObject(3));
                                Assert.assertEquals("{\"a\": \"d\"}", r.getObject(4));
                                count++;
                            }
                        }

                        Assert.assertEquals(count, totalCount);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } finally {
                    execute(conn, new String[] {dropSql1});
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void copyOutConstArrayTest() throws HoloClientException {
        if (properties == null) {
            return;
        }
        boolean[] enableCompressArrows =
                new boolean[] {false, holoVersion.compareTo(new HoloVersion(3, 0, 24)) >= 0};

        for (boolean enableCompressArrow : enableCompressArrows) {
            HoloConfig config = buildConfig();
            try (Connection conn = buildConnection();
                    HoloClient client = new HoloClient(config)) {
                String tableName = "holo_client_copy_out_const_array_" + enableCompressArrow;
                String dropSql1 = "drop table if exists " + tableName;
                String createSql =
                        "create table "
                                + tableName
                                + "(f0 text, f1 text, f2 bigint, f3 int, f4 float4, f5 float8, f6 timestamptz, f7 int[], f8 int[], f9 int[], f10 text[], f11 bigint[], f12 float4[], f13 float8[], f14 bool[])";
                String prepareData =
                        "insert into "
                                + tableName
                                + "  select i, 1, 2, 3, 4, 5, '1990-11-11', '{7, 7, 7}', array[i+1, i+2, i+3], '{9, 99, 999}', '{10, 10, 10}', '{11, 11, 11}', '{12, 12, 12}', '{13, 13, 13}','{true, true, true}' from generate_series(1, 10000)i";
                String flushSql =
                        "select hg_admin_command('flush', 'table_name=" + tableName + "')";
                execute(conn, new String[] {dropSql1, createSql, prepareData});
                tryExecute(conn, new String[] {flushSql});
                try {
                    List<String> columns =
                            Arrays.stream(client.getTableSchema(tableName, true).getColumnSchema())
                                    .map(Column::getName)
                                    .collect(Collectors.toList());

                    try (CopyOutWrapper copyOutWrapper =
                            new CopyOutWrapper(
                                    conn,
                                    tableName,
                                    columns,
                                    enableCompressArrow ? CopyFormat.ARROW : CopyFormat.ARROW_LZ4,
                                    Collections.emptyList(),
                                    "",
                                    1024 * 1024 * 20)) {
                        int sum = 0;
                        while (copyOutWrapper.hasNextBatch()) {
                            List<Record> records = copyOutWrapper.getRecords();
                            for (Record r : records) {
                                int f0 = Integer.parseInt((String) r.getObject("f0"));
                                sum += f0;
                                Assert.assertEquals("1", (String) r.getObject("f1"));
                                Assert.assertEquals(2, (long) r.getObject("f2"));
                                Assert.assertEquals(3L, (int) r.getObject("f3"));
                                Assert.assertEquals(4.0, (float) r.getObject("f4"));
                                Assert.assertEquals(5.0d, (double) r.getObject("f5"));
                                Assert.assertEquals(
                                        "1990-11-11 00:00:00.0", String.valueOf(r.getObject("f6")));
                                Assert.assertEquals(
                                        new Integer[] {7, 7, 7}, (Integer[]) r.getObject("f7"));
                                Assert.assertEquals(
                                        new Integer[] {f0 + 1, f0 + 2, f0 + 3},
                                        (Integer[]) r.getObject("f8"));
                                Assert.assertEquals(
                                        new Integer[] {9, 99, 999}, (Integer[]) r.getObject("f9"));
                                Assert.assertEquals(
                                        new String[] {"10", "10", "10"},
                                        (String[]) r.getObject("f10"));
                                Assert.assertEquals(
                                        new Long[] {11L, 11L, 11L}, (Long[]) r.getObject("f11"));
                                Assert.assertEquals(
                                        new Float[] {12.0f, 12.0f, 12.0f},
                                        (Float[]) r.getObject("f12"));
                                Assert.assertEquals(
                                        new Double[] {13.0, 13.0, 13.0},
                                        (Double[]) r.getObject("f13"));
                                Assert.assertEquals(
                                        new Boolean[] {true, true, true},
                                        (Boolean[]) r.getObject("f14"));
                            }
                        }
                        Assert.assertEquals(sum, 50005000);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } finally {
                    execute(conn, new String[] {dropSql1});
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
