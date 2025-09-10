/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.copy.CopyFormat;
import com.alibaba.hologres.client.copy.CopyMode;
import com.alibaba.hologres.client.copy.CopyUtil;
import com.alibaba.hologres.client.copy.in.CopyInOutputStream;
import com.alibaba.hologres.client.copy.in.CopyInWrapper;
import com.alibaba.hologres.client.copy.in.RecordBinaryOutputStream;
import com.alibaba.hologres.client.copy.in.RecordOutputStream;
import com.alibaba.hologres.client.copy.in.RecordTextOutputStream;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.RecordChecker;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.PgConnection;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Objects;

import static com.alibaba.hologres.client.utils.DataTypeTestUtil.EXCEPTION_ALL_TYPE_DATA;
import static com.alibaba.hologres.client.utils.DataTypeTestUtil.FIXED_PLAN_TYPE_DATA;
import static com.alibaba.hologres.client.utils.DataTypeTestUtil.TypeCaseData;

/** Fixed Copy测试用例. */
public class CopyTest extends HoloClientTestBase {

    @DataProvider(name = "typeCaseData")
    public Object[][] createData() {
        // type, format[csv, binary, binaryrow], stream mode[on, off], usefixedfe [on, off], index
        Object[][] ret = new Object[FIXED_PLAN_TYPE_DATA.length * 7][];
        for (int i = 0; i < FIXED_PLAN_TYPE_DATA.length; ++i) {
            // binary, stream, fixedfe
            ret[7 * i + 0] =
                    new Object[] {FIXED_PLAN_TYPE_DATA[i], CopyFormat.BINARY, true, true, i};
            // binaryrow, stream, fixedfe
            ret[7 * i + 1] =
                    new Object[] {FIXED_PLAN_TYPE_DATA[i], CopyFormat.BINARYROW, true, true, i};
            // csv, stream, fixedfe
            ret[7 * i + 2] = new Object[] {FIXED_PLAN_TYPE_DATA[i], CopyFormat.CSV, true, true, i};
            // binary, stream, fe
            ret[7 * i + 3] =
                    new Object[] {FIXED_PLAN_TYPE_DATA[i], CopyFormat.BINARY, true, false, i};
            // binaryrow, stream, fe
            ret[7 * i + 4] =
                    new Object[] {FIXED_PLAN_TYPE_DATA[i], CopyFormat.BINARYROW, true, false, i};
            // csv, stream, fe
            ret[7 * i + 5] = new Object[] {FIXED_PLAN_TYPE_DATA[i], CopyFormat.CSV, true, false, i};
            // csv, bulkload, fe
            ret[7 * i + 6] =
                    new Object[] {FIXED_PLAN_TYPE_DATA[i], CopyFormat.CSV, false, false, i};
        }
        return ret;
    }

    @DataProvider(name = "exceptionTypeCaseData")
    public Object[][] createExceptionData() {
        Object[][] ret = new Object[EXCEPTION_ALL_TYPE_DATA.length][];
        for (int i = 0; i < EXCEPTION_ALL_TYPE_DATA.length; ++i) {
            ret[i] = new Object[] {EXCEPTION_ALL_TYPE_DATA[i], true};
        }
        return ret;
    }

    /** data type test. */
    @Test(dataProvider = "typeCaseData")
    public void testCopy001(
            TypeCaseData typeCaseData,
            CopyFormat format,
            boolean streamMode,
            boolean useFixedFe,
            int index)
            throws Exception {

        if (properties == null) {
            return;
        }

        final int totalCount = 10;
        final int nullPkId = 5;
        String typeName = typeCaseData.getName();

        if (format == CopyFormat.BINARYROW && Objects.equals(typeName, "jsonb")) {
            return;
        } else if (useFixedFe
                && format != CopyFormat.BINARYROW
                && Objects.equals(typeName, "roaringbitmap")) {
            return;
        }
        if (useFixedFe && holoVersion.compareTo(new HoloVersion("3.1.0")) < 0) {
            return;
        } else if (format == CopyFormat.BINARYROW
                && holoVersion.compareTo(new HoloVersion("3.3.1")) < 0) {
            return;
        }
        try (Connection conn = buildConnection()) {
            String tableName =
                    "\"holo_client_copy_type_001_"
                            + typeName
                            + "_"
                            + format
                            + "_"
                            + streamMode
                            + "_"
                            + useFixedFe
                            + "_"
                            + index
                            + "\"";
            String forceReplaySql = "set hg_experimental_force_sync_replay = on";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id "
                            + typeCaseData.getColumnType()
                            + ", pk int primary key)";
            try {
                LOG.info("current type {}, format {}, streamMode {}", typeName, format, streamMode);
                execute(conn, new String[] {forceReplaySql});
                execute(conn, new String[] {dropSql});
                execute(conn, new String[] {createSql});

                PgConnection pgConn = conn.unwrap(PgConnection.class);
                TableName tn = TableName.valueOf(tableName);

                HoloVersion version = ConnectionUtil.getHoloVersion(pgConn);
                ConnectionUtil.checkMeta(pgConn, version, tn.getFullName(), 120);

                TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);

                try (Connection copyConn = buildConnection(useFixedFe);
                        CopyInWrapper copyIn =
                                new CopyInWrapper(
                                        copyConn,
                                        schema,
                                        format,
                                        CopyMode.STREAM,
                                        OnConflictAction.INSERT_OR_REPLACE,
                                        1024 * 1024 * 10)) {

                    // 插入10条，id=5的插空值
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
                        copyIn.putRecord(record);
                    }
                }

                int count = 0;
                try (Statement stat = conn.createStatement()) {
                    LOG.info("current type:{}", typeName);
                    String sql = "select * from " + tableName;
                    if ("roaringbitmap".equals(typeName)) {
                        sql = "select rb_cardinality(id), pk from " + tableName;
                    }

                    try (ResultSet rs = stat.executeQuery(sql)) {
                        while (rs.next()) {
                            int i = rs.getInt(2);
                            if (i == nullPkId) {
                                Assert.assertNull(rs.getObject(1));
                            } else {
                                typeCaseData.getPredicate().run(i, rs);
                            }
                            ++count;
                        }
                    }
                    Assert.assertEquals(count, totalCount);
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** buildCopyInSql from record test. */
    @Test
    public void testCopy002() throws Exception {
        if (properties == null) {
            return;
        }
        boolean useFixedFe = false;
        CopyFormat[] formatList =
                new CopyFormat[] {CopyFormat.BINARY, CopyFormat.CSV, CopyFormat.BINARYROW};
        try (Connection conn = buildConnection()) {
            for (CopyFormat format : formatList) {
                useFixedFe = format == CopyFormat.BINARYROW;
                HoloVersion version = ConnectionUtil.getHoloVersion(conn);
                if (useFixedFe && version.compareTo(new HoloVersion("3.1.0")) < 0) {
                    continue;
                } else if (format == CopyFormat.BINARYROW
                        && version.compareTo(new HoloVersion("3.3.1")) < 0) {
                    continue;
                }
                String tableName = "\"holo_client_copy_sql_002_" + format + "\"";
                String forceReplaySql = "set hg_experimental_force_sync_replay = on";
                String dropSql = "drop table if exists " + tableName;
                String createSql =
                        "create table "
                                + tableName
                                + "(id int not null,name text not null,address text,primary key(id))";
                try {
                    execute(conn, new String[] {forceReplaySql});
                    execute(conn, new String[] {dropSql});
                    execute(conn, new String[] {createSql});

                    try (Connection copyConn = buildConnection(useFixedFe)) {
                        TableName tn = TableName.valueOf(tableName);
                        ConnectionUtil.checkMeta(conn, version, tn.getFullName(), 120);

                        TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);
                        CopyInWrapper copyIn = null;

                        for (int i = 0; i < 10; ++i) {
                            Record record = new Record(schema);
                            record.setObject(0, i);
                            record.setObject(1, "name0");
                            if (copyIn == null) {
                                copyIn =
                                        new CopyInWrapper(
                                                copyConn,
                                                record,
                                                format,
                                                CopyMode.STREAM,
                                                OnConflictAction.INSERT_OR_UPDATE,
                                                1024 * 1024 * 10);
                            }
                            RecordChecker.check(record);
                            // this record does not contain the third field address
                            copyIn.putRecord(record);
                        }
                        copyIn.close();
                    }

                    int count = 0;
                    try (Statement stat = conn.createStatement()) {
                        try (ResultSet rs =
                                stat.executeQuery("select * from " + tableName + " order by id")) {
                            while (rs.next()) {
                                Assert.assertEquals(count, rs.getInt(1));
                                Assert.assertEquals("name0", rs.getString(2));
                                ++count;
                            }
                            Assert.assertEquals(10, count);
                        }
                    }
                } finally {
                    execute(conn, new String[] {dropSql});
                }
            }
        }
    }

    /** empty input test. */
    @Test
    public void testCopy003() throws Exception {
        if (properties == null) {
            return;
        }
        boolean[] binaryList = new boolean[] {false, true};
        try (Connection conn = buildConnection()) {
            for (boolean binary : binaryList) {
                String tableName = "\"holo_client_copy_sql_003_" + binary + "\"";
                String dropSql = "drop table if exists " + tableName;
                String createSql =
                        "create table "
                                + tableName
                                + "(id int not null,name text not null,address text,primary key(id))";
                try {
                    execute(conn, new String[] {dropSql});
                    execute(conn, new String[] {createSql});
                    try (Connection pgConn = buildConnection().unwrap(PgConnection.class)) {
                        HoloVersion version = ConnectionUtil.getHoloVersion(pgConn);
                        TableName tn = TableName.valueOf(tableName);
                        ConnectionUtil.checkMeta(pgConn, version, tn.getFullName(), 120);

                        TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);
                        CopyManager copyManager =
                                new CopyManager(pgConn.unwrap(PgConnection.class));
                        String copySql = null;
                        OutputStream os = null;
                        RecordOutputStream ros = null;
                        copySql =
                                CopyUtil.buildCopyInSql(
                                        schema,
                                        binary ? CopyFormat.BINARY : CopyFormat.CSV,
                                        OnConflictAction.INSERT_OR_UPDATE);
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
                        // close不能抛异常
                        ros.close();
                    }
                } finally {
                    execute(conn, new String[] {dropSql});
                }
            }
        }
    }

    /** update test. */
    @Test
    public void testCopy004() throws Exception {
        if (properties == null) {
            return;
        }
        boolean[] streamModeArray = new boolean[] {false, true};
        try (Connection conn = buildConnection()) {
            for (boolean streamMode : streamModeArray) {
                String tableName = "\"holo_client_copy_sql_004_" + streamMode + "\"";
                String dropSql = "drop table if exists " + tableName;
                String createSql =
                        "create table "
                                + tableName
                                + "(id int not null,name text not null,address text,primary key(id))";
                try {
                    execute(conn, new String[] {dropSql});
                    execute(conn, new String[] {createSql});

                    for (int time = 1; time <= 2; time++) {
                        try (Connection pgConn = buildConnection().unwrap(PgConnection.class)) {
                            if (!streamMode) {
                                try (Statement stat = pgConn.createStatement()) {
                                    stat.execute(
                                            "set hg_experimental_copy_enable_on_conflict = on");
                                } catch (SQLException e) {
                                    if (!e.getMessage()
                                            .contains("unrecognized configuration parameter")) {
                                        throw e;
                                    }
                                    LOG.info("need greater holo version, skip.");
                                    continue;
                                }
                            }
                            HoloVersion version = ConnectionUtil.getHoloVersion(pgConn);
                            TableName tn = TableName.valueOf(tableName);
                            ConnectionUtil.checkMeta(pgConn, version, tn.getFullName(), 120);

                            TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);
                            CopyInWrapper copyIn = null;

                            for (int i = 0; i < 10; ++i) {
                                Record record = new Record(schema);
                                record.setObject(0, i);
                                record.setObject(1, "name_" + time);
                                record.setObject(2, "address_" + time);
                                if (copyIn == null) {
                                    copyIn =
                                            new CopyInWrapper(
                                                    pgConn,
                                                    record,
                                                    CopyFormat.CSV,
                                                    streamMode
                                                            ? CopyMode.STREAM
                                                            : CopyMode.BULK_LOAD_ON_CONFLICT,
                                                    OnConflictAction.INSERT_OR_UPDATE,
                                                    1024 * 1024 * 10);
                                }
                                RecordChecker.check(record);
                                // this record does not contain the third field address
                                copyIn.putRecord(record);
                            }
                            copyIn.close();
                        }

                        int count = 0;
                        try (Statement stat = conn.createStatement()) {
                            try (ResultSet rs =
                                    stat.executeQuery(
                                            "select * from " + tableName + " order by id")) {
                                while (rs.next()) {
                                    Assert.assertEquals(count, rs.getInt(1));
                                    Assert.assertEquals("name_" + time, rs.getString(2));
                                    Assert.assertEquals("address_" + time, rs.getString(3));
                                    ++count;
                                }
                                Assert.assertEquals(10, count);
                            }
                        }
                    }
                } finally {
                    execute(conn, new String[] {dropSql});
                }
            }
        }
    }

    // binaryrow format, varchar,bpchar negative test
    @Test
    public void testCopy005() throws Exception {
        if (properties == null) {
            return;
        }
        boolean[] useFixedFeList = {false, true};

        try (Connection conn = buildConnection()) {
            HoloVersion version = ConnectionUtil.getHoloVersion(conn);
            if (version.compareTo(new HoloVersion("3.3.1")) < 0) {
                return;
            }
            for (boolean useFixedFe : useFixedFeList) {
                String tableName = "\"holo_client_copy_sql_005\"";
                String forceReplaySql = "set hg_experimental_force_sync_replay = on";
                String dropSql = "drop table if exists " + tableName;
                String createSql =
                        "create table "
                                + tableName
                                + "(id int not null, col1 varchar(4), col2 char(4), primary key(id))";
                try {
                    execute(conn, new String[] {forceReplaySql});
                    execute(conn, new String[] {dropSql});
                    execute(conn, new String[] {createSql});
                    TableName tn = TableName.valueOf(tableName);
                    ConnectionUtil.checkMeta(conn, version, tn.getFullName(), 120);
                    TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);

                    // 超出char(4), varchar(4)长度，但超出部分为尾部空格
                    Record[] records = new Record[1];
                    records[0] = new Record(schema);
                    records[0].setObject(0, 0);
                    records[0].setObject(1, "abcd  ");
                    records[0].setObject(2, "abcd  ");
                    try (Connection copyConn = buildConnection(useFixedFe);
                            CopyInWrapper copyIn =
                                    new CopyInWrapper(
                                            copyConn,
                                            schema,
                                            CopyFormat.BINARYROW,
                                            CopyMode.STREAM,
                                            OnConflictAction.INSERT_OR_UPDATE,
                                            1024 * 1024 * 10)) {
                        for (int i = 0; i < records.length; ++i) {
                            copyIn.putRecord(records[i]);
                        }
                    }

                    int count = 0;
                    try (Statement stat = conn.createStatement()) {
                        try (ResultSet rs =
                                stat.executeQuery("select * from " + tableName + " order by id")) {
                            while (rs.next()) {
                                Assert.assertEquals(count, rs.getInt(1));
                                Assert.assertEquals("abcd", rs.getString(2));
                                Assert.assertEquals("abcd", rs.getString(3));

                                ++count;
                            }
                            Assert.assertEquals(1, count);
                        }
                    }

                    records = new Record[1];
                    records[0] = new Record(schema);
                    records[0].setObject(0, 0);
                    records[0].setObject(1, "abcde");
                    records[0].setObject(2, "abcd");
                    boolean hasException = false;

                    try (Connection copyConn = buildConnection(useFixedFe);
                            CopyInWrapper copyIn =
                                    new CopyInWrapper(
                                            copyConn,
                                            schema,
                                            CopyFormat.BINARYROW,
                                            CopyMode.STREAM,
                                            OnConflictAction.INSERT_OR_UPDATE,
                                            1024 * 1024 * 10)) {
                        // 插入10条，id=5的插空值
                        for (int i = 0; i < records.length; ++i) {
                            copyIn.putRecord(records[i]);
                        }
                    } catch (IOException e) {
                        hasException = true;
                        Assert.assertTrue(
                                e.getCause()
                                        .getMessage()
                                        .contains("value too long for type character varying(4)"));
                        Assert.assertTrue(
                                ((SQLException) e.getCause())
                                        .getSQLState()
                                        .equals("22001")); // ERRCODE_STRING_DATA_RIGHT_TRUNCATION
                    }
                    Assert.assertTrue(hasException);

                    records = new Record[1];
                    records[0] = new Record(schema);
                    records[0].setObject(0, 0);
                    records[0].setObject(1, "abcd");
                    records[0].setObject(2, "abcde");
                    hasException = false;
                    try (Connection copyConn = buildConnection(useFixedFe);
                            CopyInWrapper copyIn =
                                    new CopyInWrapper(
                                            copyConn,
                                            schema,
                                            CopyFormat.BINARYROW,
                                            CopyMode.STREAM,
                                            OnConflictAction.INSERT_OR_UPDATE,
                                            1024 * 1024 * 10)) {

                        // 插入10条，id=5的插空值
                        for (int i = 0; i < records.length; ++i) {
                            copyIn.putRecord(records[i]);
                        }
                    } catch (IOException e) {
                        hasException = true;
                        Assert.assertTrue(
                                e.getCause()
                                        .getMessage()
                                        .contains("value too long for type character(4)"));
                        Assert.assertTrue(
                                ((SQLException) e.getCause())
                                        .getSQLState()
                                        .equals("22001")); // ERRCODE_STRING_DATA_RIGHT_TRUNCATION
                    }
                    Assert.assertTrue(hasException);

                    records = new Record[1];
                    records[0] = new Record(schema);
                    records[0].setObject(0, 0);
                    records[0].setObject(1, "f\u0000a");
                    records[0].setObject(2, "abcd");
                    hasException = false;
                    try (Connection copyConn = buildConnection(useFixedFe);
                            CopyInWrapper copyIn =
                                    new CopyInWrapper(
                                            copyConn,
                                            schema,
                                            CopyFormat.BINARYROW,
                                            CopyMode.STREAM,
                                            OnConflictAction.INSERT_OR_UPDATE,
                                            1024 * 1024 * 10)) {
                        // 插入10条，id=5的插空值
                        for (int i = 0; i < records.length; ++i) {
                            copyIn.putRecord(records[i]);
                        }
                    } catch (IOException e) {
                        hasException = true;
                        Assert.assertTrue(
                                e.getCause()
                                        .getMessage()
                                        .contains("invalid byte sequence for encoding"));
                        Assert.assertTrue(
                                ((SQLException) e.getCause())
                                        .getSQLState()
                                        .equals("22021")); // ERRCODE_CHARACTER_NOT_IN_REPERTOIRE
                    }
                    Assert.assertTrue(hasException);

                } finally {
                    execute(conn, new String[] {dropSql});
                }
            }
        }
    }

    /** copy partitioned table test. */
    @Test
    public void testCopy006() throws Exception {
        if (properties == null) {
            return;
        }
        if (holoVersion.compareTo(new HoloVersion("3.1.0")) < 0) {
            return;
        }
        try (Connection conn = buildConnection()) {
            String tableName = "holo_copy_test_partitioned_006";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,b text, ds text, primary key(id,ds)) partition by list(ds)";
            String createPartition1 =
                    "create table "
                            + tableName
                            + "_20250615 partition of "
                            + tableName
                            + " for values in ('20250615')";
            String createPartition2 =
                    "create table "
                            + tableName
                            + "_20250616 partition of "
                            + tableName
                            + " for values in ('20250616')";
            try {
                execute(
                        conn,
                        new String[] {dropSql, createSql, createPartition1, createPartition2});

                try (CopyInWrapper copyIn =
                        new CopyInWrapper(
                                conn,
                                tableName,
                                Arrays.asList("id", "b", "ds"),
                                CopyFormat.BINARY,
                                CopyMode.STREAM,
                                OnConflictAction.INSERT_OR_UPDATE,
                                1024 * 1024 * 10)) {
                    TableSchema schema = copyIn.getSchema();
                    for (int i = 0; i < 10; ++i) {
                        Record record = new Record(schema);
                        record.setObject(0, i);
                        record.setObject(1, "name_" + i);
                        record.setObject(2, (i % 2 == 0) ? "20250615" : "20250616");

                        RecordChecker.check(record);
                        // this record does not contain the third field address
                        copyIn.putRecord(record);
                        copyIn.flush();
                    }
                }

                int count = 0;
                try (Statement stat = conn.createStatement()) {
                    try (ResultSet rs =
                            stat.executeQuery("select * from " + tableName + " order by id")) {
                        while (rs.next()) {
                            Assert.assertEquals(count, rs.getInt(1));
                            Assert.assertEquals("name_" + count, rs.getString(2));
                            Assert.assertEquals(
                                    (count % 2 == 0) ? "20250615" : "20250616", rs.getString(3));
                            ++count;
                        }
                    }
                }
                Assert.assertEquals(10, count);
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** copy part columns. */
    @Test
    public void testCopy007() throws Exception {
        if (properties == null) {
            return;
        }
        try (Connection conn = buildConnection()) {
            String tableName = "holo_copy_test_007";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(no_use_1 int, no_use_2 int, id int not null, no_use_3 text, b text, ds date, primary key(id,ds))";

            try {
                execute(conn, new String[] {dropSql, createSql});

                // 传入columns且顺序和schema不一致, 内部会自动按holo的字段排序
                try (CopyInWrapper copyIn =
                        new CopyInWrapper(
                                conn,
                                tableName,
                                Arrays.asList("id", "ds", "b"),
                                CopyFormat.BINARY,
                                CopyMode.STREAM,
                                OnConflictAction.INSERT_OR_UPDATE,
                                1024 * 1024 * 10)) {

                    TableSchema schema = copyIn.getSchema();
                    for (int i = 0; i < 10; ++i) {
                        Put put = new Put(schema);
                        put.setObject("id", i);
                        put.setObject(
                                "ds",
                                (i % 2 == 0)
                                        ? Date.valueOf("2025-06-15")
                                        : Date.valueOf("2025-06-16"));
                        put.setObject("b", "name_" + i);

                        RecordChecker.check(put.getRecord());
                        // this record does not contain the third field address
                        copyIn.putRecord(put.getRecord());
                        copyIn.flush();
                    }
                }

                int count = 0;
                try (Statement stat = conn.createStatement()) {
                    try (ResultSet rs =
                            stat.executeQuery(
                                    "select id,b,ds from " + tableName + " order by id")) {
                        while (rs.next()) {
                            Assert.assertEquals(count, rs.getInt(1));
                            Assert.assertEquals("name_" + count, rs.getString(2));
                            Assert.assertEquals(
                                    (count % 2 == 0) ? "2025-06-15" : "2025-06-16",
                                    rs.getString(3));
                            ++count;
                        }
                    }
                }
                Assert.assertEquals(10, count);
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /** copy negative test. */
    @Test
    public void testCopy008() throws Exception {
        if (properties == null) {
            return;
        }
        try (Connection conn = buildConnection()) {
            String tableName = "holo_copy_test_008";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table " + tableName + "(a int, b int, c int, d text, e text, f text)";

            try {
                execute(conn, new String[] {dropSql, createSql});

                try {
                    CopyInWrapper copyIn =
                            new CopyInWrapper(
                                    conn,
                                    tableName,
                                    Arrays.asList("a", "d", "not_exist"),
                                    CopyFormat.BINARY,
                                    CopyMode.STREAM,
                                    OnConflictAction.INSERT_OR_UPDATE,
                                    1024 * 1024 * 10);
                    copyIn.getSchema();
                } catch (IOException e) {
                    Assert.assertTrue(e.getMessage().contains("column not_exist is not in schema"));
                }
                try (CopyInWrapper copyIn =
                        new CopyInWrapper(
                                conn,
                                tableName,
                                Arrays.asList("e", "d"),
                                CopyFormat.BINARY,
                                CopyMode.STREAM,
                                OnConflictAction.INSERT_OR_UPDATE,
                                1024 * 1024 * 10)) {
                    TableSchema schema = copyIn.getSchema();
                    for (int i = 0; i < 10; ++i) {
                        Put put = new Put(schema);
                        put.setObject("d", "dd_" + i);
                        put.setObject("f", "ff_" + i);

                        RecordChecker.check(put.getRecord());
                        // this record does not contain the third field address
                        copyIn.putRecord(put.getRecord());
                        copyIn.flush();
                    }
                } catch (IOException e) {
                    Assert.assertTrue(
                            e.getMessage()
                                    .contains(
                                            "Column e should be set, because it is included the following fields(Specified when initializing CopyInWrapper): [e, d]"));
                }
                try (CopyInWrapper copyIn =
                        new CopyInWrapper(
                                conn,
                                tableName,
                                Arrays.asList("d", "e"),
                                CopyFormat.BINARY,
                                CopyMode.STREAM,
                                OnConflictAction.INSERT_OR_UPDATE,
                                1024 * 1024 * 10)) {
                    TableSchema schema = copyIn.getSchema();
                    for (int i = 0; i < 10; ++i) {
                        Put put = new Put(schema);
                        put.setObject("d", "dd_" + i);
                        put.setObject("e", "ee_" + i);
                        put.setObject("f", "ff_" + i);

                        RecordChecker.check(put.getRecord());
                        // this record does not contain the third field address
                        copyIn.putRecord(put.getRecord());
                        copyIn.flush();
                    }
                } catch (IOException e) {
                    Assert.assertTrue(
                            e.getMessage()
                                    .contains(
                                            "Column f should not be set, only the following fields(Specified when initializing CopyInWrapper) can be set: [d, e]"));
                }
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    @Test(dataProvider = "typeCaseData")
    public void testRecordChecker001(
            TypeCaseData typeCaseData,
            CopyFormat format,
            boolean streamMode,
            boolean useFixedFe,
            int index)
            throws Exception {
        if (properties == null) {
            return;
        }
        if (format == CopyFormat.BINARYROW && Objects.equals(typeCaseData.getName(), "jsonb")) {
            return;
        } else if (useFixedFe
                && format != CopyFormat.BINARYROW
                && Objects.equals(typeCaseData.getName(), "roaringbitmap")) {
            return;
        }
        if (useFixedFe && holoVersion.compareTo(new HoloVersion("3.1.0")) < 0) {
            return;
        } else if (format == CopyFormat.BINARYROW
                && holoVersion.compareTo(new HoloVersion("3.3.1")) < 0) {
            return;
        } else if (!streamMode
                && (typeCaseData.getName().equals("varchar")
                        || typeCaseData.getName().equals("char"))) {
            return;
        }
        try (Connection conn = buildConnection()) {
            {
                String typeName = typeCaseData.getName();
                String tableName =
                        "\"holo_client_record_checker_sql_001_"
                                + typeCaseData.getName()
                                + "_"
                                + format
                                + "_"
                                + streamMode
                                + "_"
                                + useFixedFe
                                + "_"
                                + index
                                + "\"";
                String forceReplaySql = "set hg_experimental_force_sync_replay = on";
                String dropSql = "drop table if exists " + tableName;
                String createSql = "create table " + tableName;
                createSql += "(c0" + " " + typeCaseData.getColumnType();
                createSql += ",id int not null,primary key(id))";
                try {
                    LOG.info(
                            "current type {}, format {}, streamMode {}",
                            typeName,
                            format,
                            streamMode);
                    execute(conn, new String[] {forceReplaySql});
                    execute(conn, new String[] {dropSql});
                    execute(conn, new String[] {createSql});
                    PgConnection pgConn = conn.unwrap(PgConnection.class);

                    HoloVersion version = ConnectionUtil.getHoloVersion(pgConn);
                    TableName tn = TableName.valueOf(tableName);
                    ConnectionUtil.checkMeta(pgConn, version, tn.getFullName(), 120);

                    TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);
                    try (Connection copyConn = buildConnection(useFixedFe)) {
                        CopyInWrapper copyIn;
                        Record record = new Record(schema);
                        Object obj =
                                typeCaseData
                                        .getSupplier()
                                        .apply(0, conn.unwrap(BaseConnection.class));
                        record.setObject(0, obj);
                        record.setObject(1, 1);
                        RecordChecker.check(record);
                        copyIn =
                                new CopyInWrapper(
                                        copyConn,
                                        record,
                                        format,
                                        streamMode ? CopyMode.STREAM : CopyMode.BULK_LOAD,
                                        OnConflictAction.INSERT_OR_UPDATE,
                                        1024 * 1024 * 10);

                        copyIn.putRecord(record);
                        copyIn.close();
                    }
                    int count = 0;
                    try (Statement stat = conn.createStatement()) {
                        String sql = "select * from " + tableName;
                        if ("roaringbitmap".equals(typeCaseData.getColumnType())) {
                            sql = "select rb_cardinality(c0), id from " + tableName;
                        }
                        try (ResultSet rs = stat.executeQuery(sql)) {
                            while (rs.next()) {
                                typeCaseData.getPredicate().run(0, rs);
                                ++count;
                            }
                            Assert.assertEquals(1, count);
                        }
                    }

                } finally {
                    execute(conn, new String[] {dropSql});
                }
            }
        }
    }

    @Test(dataProvider = "exceptionTypeCaseData")
    public void testRecordWriter002(TypeCaseData typeCaseData, boolean binary) throws Exception {
        if (properties == null) {
            return;
        }
        try (Connection conn = buildConnection()) {
            {
                String tableName =
                        "\"holo_client_record_checker_sql_002_"
                                + typeCaseData.getName()
                                + "_"
                                + binary
                                + "\"";
                String forceReplaySql = "set hg_experimental_force_sync_replay = on";
                String dropSql = "drop table if exists " + tableName;
                String createSql = "create table " + tableName;

                createSql += "(c0" + " " + typeCaseData.getColumnType();

                createSql += ",id int not null,primary key(id))";
                try {
                    execute(conn, new String[] {forceReplaySql});
                    execute(conn, new String[] {dropSql});
                    execute(conn, new String[] {createSql});
                    PgConnection pgConn = conn.unwrap(PgConnection.class);

                    HoloVersion version = ConnectionUtil.getHoloVersion(pgConn);
                    TableName tn = TableName.valueOf(tableName);
                    ConnectionUtil.checkMeta(pgConn, version, tn.getFullName(), 120);

                    TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);

                    Record record = new Record(schema);
                    Object obj =
                            typeCaseData.getSupplier().apply(0, conn.unwrap(BaseConnection.class));
                    record.setObject(0, obj);
                    record.setObject(1, 1);
                    Assert.expectThrows(
                            HoloClientException.class, () -> RecordChecker.check(record));
                } finally {
                    execute(conn, new String[] {dropSql});
                }
            }
        }
    }

    /** test buildCopyInSql. */
    @Test
    public void testCopyUtil001() throws Exception {
        if (properties == null) {
            return;
        }
        try (Connection conn = buildConnection()) {

            String tableName = "\"holo_client_copy_util_001\"";
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

                // stream_mode = true (default), binary = true
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema, CopyFormat.BINARY, OnConflictAction.INSERT_OR_UPDATE),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(stream_mode true, format binary, on_conflict update)");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema, CopyFormat.BINARY, OnConflictAction.INSERT_OR_IGNORE),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(stream_mode true, format binary, on_conflict ignore)");
                // stream_mode = true (default), binary = false
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema, CopyFormat.CSV, OnConflictAction.INSERT_OR_REPLACE),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(stream_mode true, format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL 'N', on_conflict update)");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema, CopyFormat.CSV, OnConflictAction.INSERT_OR_IGNORE),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(stream_mode true, format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL 'N', on_conflict ignore)");
                // new interface
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema,
                                CopyFormat.CSV,
                                OnConflictAction.INSERT_OR_IGNORE,
                                CopyMode.STREAM),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(stream_mode true, format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL 'N', on_conflict ignore)");

                // stream_mode = false, don't care binary, don't care OnConflictAction default
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema,
                                CopyFormat.BINARY,
                                OnConflictAction.INSERT_OR_UPDATE,
                                false),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL 'N')");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema,
                                CopyFormat.BINARY,
                                OnConflictAction.INSERT_OR_IGNORE,
                                false),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL 'N')");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema, CopyFormat.CSV, OnConflictAction.INSERT_OR_UPDATE, false),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL 'N')");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema,
                                CopyFormat.CSV,
                                OnConflictAction.INSERT_OR_UPDATE,
                                CopyMode.BULK_LOAD),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL 'N')");

                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema,
                                CopyFormat.BINARY,
                                OnConflictAction.INSERT_OR_UPDATE,
                                CopyMode.BULK_LOAD_ON_CONFLICT),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL 'N', on_conflict update)");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema,
                                CopyFormat.BINARY,
                                OnConflictAction.INSERT_OR_IGNORE,
                                CopyMode.BULK_LOAD_ON_CONFLICT),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL 'N', on_conflict ignore)");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema,
                                CopyFormat.CSV,
                                OnConflictAction.INSERT_OR_UPDATE,
                                CopyMode.BULK_LOAD_ON_CONFLICT),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL 'N', on_conflict update)");

                Record record = new Record(schema);
                record.setObject(0, 0);
                record.setObject(1, "name0");
                // record不包含第三个字段address
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record, CopyFormat.BINARY, OnConflictAction.INSERT_OR_UPDATE),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(stream_mode true, format binary, on_conflict update)");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record, CopyFormat.BINARY, OnConflictAction.INSERT_OR_IGNORE),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(stream_mode true, format binary, on_conflict ignore)");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record, CopyFormat.CSV, OnConflictAction.INSERT_OR_REPLACE),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(stream_mode true, format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL 'N', on_conflict update)");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record, CopyFormat.CSV, OnConflictAction.INSERT_OR_IGNORE),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(stream_mode true, format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL 'N', on_conflict ignore)");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record,
                                CopyFormat.CSV,
                                OnConflictAction.INSERT_OR_IGNORE,
                                CopyMode.STREAM),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(stream_mode true, format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL 'N', on_conflict ignore)");

                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record,
                                CopyFormat.BINARY,
                                OnConflictAction.INSERT_OR_UPDATE,
                                false),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL 'N')");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record,
                                CopyFormat.BINARY,
                                OnConflictAction.INSERT_OR_IGNORE,
                                false),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL 'N')");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record, CopyFormat.CSV, OnConflictAction.INSERT_OR_UPDATE, false),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL 'N')");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record,
                                CopyFormat.CSV,
                                OnConflictAction.INSERT_OR_UPDATE,
                                CopyMode.BULK_LOAD),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL 'N')");

                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record,
                                CopyFormat.BINARY,
                                OnConflictAction.INSERT_OR_UPDATE,
                                CopyMode.BULK_LOAD_ON_CONFLICT),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL 'N', on_conflict update)");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record,
                                CopyFormat.BINARY,
                                OnConflictAction.INSERT_OR_IGNORE,
                                CopyMode.BULK_LOAD_ON_CONFLICT),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL 'N', on_conflict ignore)");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record,
                                CopyFormat.CSV,
                                OnConflictAction.INSERT_OR_UPDATE,
                                CopyMode.BULK_LOAD_ON_CONFLICT),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL 'N', on_conflict update)");
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }
}
