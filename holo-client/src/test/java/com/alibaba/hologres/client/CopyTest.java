/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.copy.CopyFormat;
import com.alibaba.hologres.client.copy.CopyMode;
import com.alibaba.hologres.client.copy.CopyUtil;
import com.alibaba.hologres.client.copy.in.CopyInOutputStream;
import com.alibaba.hologres.client.copy.in.CopyInStageWrapper;
import com.alibaba.hologres.client.copy.in.CopyInWrapper;
import com.alibaba.hologres.client.copy.in.RecordBinaryOutputStream;
import com.alibaba.hologres.client.copy.in.RecordOutputStream;
import com.alibaba.hologres.client.copy.in.RecordTextOutputStream;
import com.alibaba.hologres.client.copy.in.arrow.RecordArrowWriter;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.RecordChecker;
import org.postgresql.PGProperty;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.PgConnection;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.alibaba.hologres.client.copy.CopyUtil.buildInsertTableSelectFromStageSql;
import static com.alibaba.hologres.client.utils.DataTypeTestUtil.ALL_TYPE_DATA;
import static com.alibaba.hologres.client.utils.DataTypeTestUtil.EXCEPTION_ALL_TYPE_DATA;
import static com.alibaba.hologres.client.utils.DataTypeTestUtil.FIXED_PLAN_TYPE_DATA;
import static com.alibaba.hologres.client.utils.DataTypeTestUtil.TypeCaseData;

/** Fixed Copy测试用例. */
public class CopyTest extends HoloClientTestBase {

    private Object[][] buildTypeCaseDataCombinations() {
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

    @DataProvider(name = "typeCaseDataForStage")
    public Object[][] createStageTestData() {
        Object[][] ret = new Object[ALL_TYPE_DATA.length][];
        for (int i = 0; i < ALL_TYPE_DATA.length; ++i) {
            ret[i] = new Object[] {ALL_TYPE_DATA[i]};
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

    /** data type test - runs all type × format × mode × fixedFe combinations in parallel. */
    @Test
    public void testCopy001() throws Exception {
        if (properties == null) {
            return;
        }

        Object[][] combinations = buildTypeCaseDataCombinations();
        List<Callable<Void>> tasks = new ArrayList<>();

        for (Object[] combo : combinations) {
            TypeCaseData typeCaseData = (TypeCaseData) combo[0];
            CopyFormat format = (CopyFormat) combo[1];
            boolean streamMode = (boolean) combo[2];
            boolean useFixedFe = (boolean) combo[3];
            int index = (int) combo[4];
            String typeName = typeCaseData.getName();

            if (useFixedFe
                    && format != CopyFormat.BINARYROW
                    && Objects.equals(typeName, "roaringbitmap")) {
                continue;
            }
            if (useFixedFe && holoVersion.compareTo(new HoloVersion("3.1.0")) < 0) {
                continue;
            } else if (format == CopyFormat.BINARYROW
                    && holoVersion.compareTo(new HoloVersion("4.1.0")) < 0) {
                continue;
            }

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
            String createSql =
                    "create table "
                            + tableName
                            + "(id "
                            + typeCaseData.getColumnType()
                            + ", pk int primary key)";

            tasks.add(
                    tableTask(
                            tableName,
                            createSql,
                            conn -> {
                                LOG.info(
                                        "current type {}, format {}, streamMode {}",
                                        typeName,
                                        format,
                                        streamMode);
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
                                    for (int i = 0; i < 10; ++i) {
                                        Record record = Record.build(schema);
                                        if (i == 5) {
                                            record.setObject(0, null);
                                        } else {
                                            record.setObject(
                                                    0,
                                                    typeCaseData
                                                            .getSupplier()
                                                            .apply(
                                                                    i,
                                                                    conn.unwrap(
                                                                            BaseConnection.class)));
                                        }
                                        record.setObject(1, i);
                                        copyIn.putRecord(record);
                                    }
                                }

                                int count = 0;
                                try (Statement stat = conn.createStatement()) {
                                    String sql = "select * from " + tableName;
                                    if ("roaringbitmap".equals(typeName)) {
                                        sql = "select rb_cardinality(id), pk from " + tableName;
                                    }
                                    try (ResultSet rs = stat.executeQuery(sql)) {
                                        while (rs.next()) {
                                            int i = rs.getInt(2);
                                            if (i == 5) {
                                                Assert.assertNull(rs.getObject(1));
                                            } else {
                                                typeCaseData.getPredicate().run(i, rs);
                                            }
                                            ++count;
                                        }
                                    }
                                    Assert.assertEquals(count, 10);
                                }
                            }));
        }

        runParallelTasks(tasks);
    }

    /** data type test. */
    @Test(dataProvider = "typeCaseDataForStage")
    public void testCopyStageType(TypeCaseData typeCaseData) throws Exception {
        if (properties == null) {
            return;
        }
        if (holoVersion.compareTo(new HoloVersion("4.1.0")) < 0) {
            return;
        }
        final int totalCount = 10;
        final int nullPkId = 5;
        String typeName = typeCaseData.getName();

        Properties info = new Properties();
        info.setProperty(PGProperty.PREFER_QUERY_MODE.getName(), "simple");
        try (Connection conn = buildConnection(info)) {
            String stageName = "test_stage_" + System.currentTimeMillis();
            String createStageSql =
                    "call hologres.hg_create_internal_stage('"
                            + stageName
                            + "', 'test_group', 7200);";
            String dropStageSql = "call hologres.hg_drop_internal_stage('" + stageName + "');";
            String tableName = "\"holo_client_copy_stage_type_001_" + typeName + "\"";
            String forceReplaySql = "set hg_experimental_force_sync_replay = on";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id "
                            + typeCaseData.getColumnType()
                            + ", pk int primary key)";
            try {
                execute(conn, new String[] {forceReplaySql});
                tryExecute(conn, new String[] {dropStageSql});
                execute(conn, new String[] {dropSql, createSql, createStageSql});

                PgConnection pgConn = conn.unwrap(PgConnection.class);
                TableName tn = TableName.valueOf(tableName);

                HoloVersion version = ConnectionUtil.getHoloVersion(pgConn);
                ConnectionUtil.checkMeta(pgConn, version, tn.getFullName(), 120);

                TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);
                List<String> columnNames =
                        Arrays.stream(schema.getColumnSchema())
                                .map(Column::getName)
                                .collect(Collectors.toList());
                try (RecordArrowWriter arrowWriter =
                                new RecordArrowWriter(schema, columnNames, 8192);
                        CopyInStageWrapper<Record> copyIn =
                                new CopyInStageWrapper<>(
                                        buildConfig(), stageName, "test_file", arrowWriter)) {

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
                } catch (Exception e) {
                    LOG.error("", e);
                    throw e;
                }
                try (Statement stat = conn.createStatement()) {
                    String sql =
                            buildInsertTableSelectFromStageSql(
                                    schema,
                                    columnNames,
                                    Collections.singletonList(stageName),
                                    OnConflictAction.INSERT_OR_UPDATE);
                    LOG.info("insert sql : {}", sql);
                    stat.execute(sql);
                }

                int count = 0;
                try (Statement stat = conn.createStatement()) {
                    LOG.info("current type:{}", typeName);
                    String sql = "select * from " + tableName;
                    if ("roaringbitmap".equals(typeName)) {
                        sql = "select rb_cardinality(id), pk from " + tableName;
                    } else if ("geometry".equals(typeName) || "geography".equals(typeName)) {
                        sql = "select ST_AsText(id), pk from " + tableName;
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

    /**
     * copy stage with LZ4 compression: 使用 Arrow IPC V5 buffer-level LZ4 压缩写入 stage， 然后通过 INSERT
     * INTO SELECT FROM stage 读取数据，验证引擎端可正确解压。
     */
    @Test
    public void testCopyStageWithLz4Compression() throws Exception {
        if (properties == null) {
            return;
        }
        if (holoVersion.compareTo(new HoloVersion("4.2.5")) < 0) {
            return;
        }
        Properties info = new Properties();
        info.setProperty(PGProperty.PREFER_QUERY_MODE.getName(), "simple");
        try (Connection conn = buildConnection(info)) {
            long ts = System.currentTimeMillis();
            String stageName = "test_stage_lz4_" + ts;
            String createStageSql =
                    "call hologres.hg_create_internal_stage('"
                            + stageName
                            + "', 'test_group', 7200);";
            String dropStageSql = "call hologres.hg_drop_internal_stage('" + stageName + "');";
            String uncompressedStageName = "test_stage_no_lz4_" + ts;
            String dropUncompressedStageSql =
                    "call hologres.hg_drop_internal_stage('" + uncompressedStageName + "');";
            String tableName = "test_stage_lz4_001";
            String forceReplaySql = "set hg_experimental_force_sync_replay = on";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table " + tableName + "(a int, b text, c float4, primary key(a))";
            try {
                execute(conn, new String[] {forceReplaySql});
                tryExecute(conn, new String[] {dropStageSql});
                execute(conn, new String[] {dropSql, createSql, createStageSql});

                PgConnection pgConn = conn.unwrap(PgConnection.class);
                TableName tn = TableName.valueOf(tableName);
                HoloVersion version = ConnectionUtil.getHoloVersion(pgConn);
                ConnectionUtil.checkMeta(pgConn, version, tn.getFullName(), 120);
                TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);

                List<String> columnNames =
                        Arrays.stream(schema.getColumnSchema())
                                .map(Column::getName)
                                .collect(Collectors.toList());

                final int totalCount = 100;
                // enableCompression = true，使用 Arrow IPC V5 buffer-level LZ4 压缩
                try (RecordArrowWriter arrowWriter =
                                new RecordArrowWriter(schema, columnNames, 8192, true);
                        CopyInStageWrapper<Record> copyIn =
                                new CopyInStageWrapper<>(
                                        buildConfig(), stageName, "lz4_test_file", arrowWriter)) {
                    for (int i = 0; i < totalCount; ++i) {
                        Record record = Record.build(schema);
                        record.setObject(0, i);
                        record.setObject(1, "compressed_name_" + i);
                        record.setObject(2, 1.5f * i);
                        copyIn.putRecord(record);
                    }
                } catch (Exception e) {
                    LOG.error("copy in stage with lz4 failed", e);
                    throw e;
                }

                // 查询压缩 stage 的文件大小
                long compressedBytes = 0;
                try (Statement stat = conn.createStatement();
                        ResultSet rs =
                                stat.executeQuery(
                                        "select stage_bytes, file_count from hologres.hg_internal_stages where stage_name = '"
                                                + stageName
                                                + "'")) {
                    if (rs.next()) {
                        compressedBytes = rs.getLong("stage_bytes");
                        LOG.info(
                                "Compressed stage: name={}, bytes={}, file_count={}",
                                stageName,
                                compressedBytes,
                                rs.getInt("file_count"));
                    }
                }

                // 写入不压缩的数据到另一个 stage 做对比
                String createUncompressedStageSql =
                        "call hologres.hg_create_internal_stage('"
                                + uncompressedStageName
                                + "', 'test_group', 7200);";
                execute(conn, new String[] {createUncompressedStageSql});
                try (RecordArrowWriter arrowWriter2 =
                                new RecordArrowWriter(schema, columnNames, 8192, false);
                        CopyInStageWrapper<Record> copyIn2 =
                                new CopyInStageWrapper<>(
                                        buildConfig(),
                                        uncompressedStageName,
                                        "no_lz4_test_file",
                                        arrowWriter2)) {
                    for (int i = 0; i < totalCount; ++i) {
                        Record record = Record.build(schema);
                        record.setObject(0, i);
                        record.setObject(1, "compressed_name_" + i);
                        record.setObject(2, 1.5f * i);
                        copyIn2.putRecord(record);
                    }
                }

                long uncompressedBytes = 0;
                try (Statement stat = conn.createStatement();
                        ResultSet rs =
                                stat.executeQuery(
                                        "select stage_bytes, file_count from hologres.hg_internal_stages where stage_name = '"
                                                + uncompressedStageName
                                                + "'")) {
                    if (rs.next()) {
                        uncompressedBytes = rs.getLong("stage_bytes");
                        LOG.info(
                                "Uncompressed stage: name={}, bytes={}, file_count={}",
                                uncompressedStageName,
                                uncompressedBytes,
                                rs.getInt("file_count"));
                    }
                }

                double ratio =
                        uncompressedBytes > 0
                                ? (double) compressedBytes / uncompressedBytes * 100
                                : 0;
                LOG.info(
                        "Size comparison: compressed={} bytes, uncompressed={} bytes, ratio={}%",
                        compressedBytes, uncompressedBytes, String.format("%.1f", ratio));
                Assert.assertTrue(
                        compressedBytes < uncompressedBytes,
                        String.format(
                                "compressed size (%d) should be smaller than uncompressed (%d)",
                                compressedBytes, uncompressedBytes));

                // 使用 INSERT INTO table SELECT FROM stage 读取压缩数据
                try (Statement stat = conn.createStatement()) {
                    String sql =
                            buildInsertTableSelectFromStageSql(
                                    schema,
                                    columnNames,
                                    Collections.singletonList(stageName),
                                    OnConflictAction.INSERT_OR_UPDATE);
                    LOG.info("insert from lz4 stage sql: {}", sql);
                    stat.execute(sql);
                }

                // 验证数据
                int count = 0;
                try (Statement stat = conn.createStatement()) {
                    try (ResultSet rs =
                            stat.executeQuery("select a, b, c from " + tableName + " order by a")) {
                        while (rs.next()) {
                            int a = rs.getInt(1);
                            String b = rs.getString(2);
                            float c = rs.getFloat(3);
                            Assert.assertEquals(a, count, "column a mismatch");
                            Assert.assertEquals(b, "compressed_name_" + count, "column b mismatch");
                            Assert.assertEquals(c, 1.5f * count, 0.01f, "column c mismatch");
                            ++count;
                        }
                    }
                }
                Assert.assertEquals(count, totalCount, "total row count mismatch");
                LOG.info(
                        "testCopyStageWithLz4Compression passed: {} rows written and read back successfully",
                        totalCount);
            } finally {
                tryExecute(conn, new String[] {dropSql});
                tryExecute(conn, new String[] {dropStageSql});
                tryExecute(conn, new String[] {dropUncompressedStageSql});
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
                        && version.compareTo(new HoloVersion("4.1.0")) < 0) {
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
            if (version.compareTo(new HoloVersion("4.1.0")) < 0) {
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
                TableName tn = TableName.valueOf(tableName);
                TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);
                try (CopyInWrapper copyIn =
                        new CopyInWrapper(
                                conn,
                                schema,
                                Arrays.asList("id", "b", "ds"),
                                CopyFormat.BINARY,
                                CopyMode.STREAM,
                                OnConflictAction.INSERT_OR_UPDATE,
                                1024 * 1024 * 10)) {
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

    /** copy stage: 测试整体的执行情况,包括文件的自动拆分. */
    @Test
    public void testCopyStage() throws Exception {
        if (properties == null) {
            return;
        }
        if (holoVersion.compareTo(new HoloVersion("4.1.0")) < 0) {
            return;
        }
        Properties info = new Properties();
        info.setProperty(PGProperty.PREFER_QUERY_MODE.getName(), "simple");
        try (Connection conn = buildConnection(info);
                HoloClient client = new HoloClient(buildConfig())) {
            String stageName = "test_stage" + System.currentTimeMillis();
            String createStageSql =
                    "call hologres.hg_create_internal_stage('"
                            + stageName
                            + "', 'test_group', 7200);";
            String dropStageSql = "call hologres.hg_drop_internal_stage('" + stageName + "');";
            String tableName = "test_stage_001";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table " + tableName + "(a int, b text, c float4, primary key(a))";

            tryExecute(conn, new String[] {dropStageSql});
            execute(conn, new String[] {dropSql, createSql, createStageSql});

            TableSchema schema = client.getTableSchema(tableName);

            AtomicInteger id = new AtomicInteger(1);
            Runnable runnable =
                    () -> {
                        int task = id.getAndIncrement();
                        try (RecordArrowWriter arrowWriter =
                                        new RecordArrowWriter(
                                                schema,
                                                Arrays.asList("a", "b", "c"),
                                                1024 // maxBatchSize,每1024行数据组成一个arrow的RecordBatch
                                                );
                                CopyInStageWrapper<Record> copyIn =
                                        new CopyInStageWrapper<>(
                                                buildConfig(),
                                                stageName,
                                                "test_file_task_" + task,
                                                arrowWriter,
                                                1024 * 1024 // fileSizeLimit, 每个文件大小1MB
                                                )) {

                            int base = task * 10000000;
                            int count = 0;
                            String s = generateRandomString(1024);
                            while (count < 10 * 1024) {
                                int a = base + count++;
                                Put put = new Put(schema);
                                put.setObject("a", a);
                                put.setObject("b", "name_" + (a / 2) + s);
                                put.setObject("c", 1.7f * count);

                                RecordChecker.check(put.getRecord());
                                copyIn.putRecord(put.getRecord());
                            }
                            LOG.info(
                                    "copy in stage task({}) finished, have written records: {}",
                                    task,
                                    count);

                        } catch (IOException | HoloClientException e) {
                            throw new RuntimeException(e);
                        }
                    };
            ExecutorService es =
                    new ThreadPoolExecutor(
                            10,
                            10,
                            0L,
                            TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<>(100),
                            Thread::new,
                            new ThreadPoolExecutor.AbortPolicy());
            for (int i = 0; i < 10; ++i) {
                es.execute(runnable);
            }
            es.shutdown();
            while (!es.awaitTermination(5000L, TimeUnit.MILLISECONDS)) {}

            // 👇 在方法内“声明”一个 lambda 函数（赋值给局部变量）
            Function<String, String> extractTaskId =
                    (name) -> {
                        Pattern pattern = Pattern.compile(".*_task_(\\d+)_(\\d+)\\.arrow$");
                        Matcher matcher = pattern.matcher(name);
                        if (matcher.matches()) {
                            return matcher.group(1);
                        }
                        return null; // 或抛异常
                    };
            try (Statement stat = conn.createStatement()) {
                try (ResultSet rs =
                        stat.executeQuery(
                                "select * from hologres.hg_internal_stage_files where stage_name = '"
                                        + stageName
                                        + "'")) {
                    Map<String, Integer> taskToFileCount = new HashMap<>();
                    while (rs.next()) {
                        String fileName = rs.getString(2);
                        taskToFileCount.computeIfPresent(
                                extractTaskId.apply(fileName), (k, v) -> v + 1);
                    }
                    for (Map.Entry<String, Integer> entry : taskToFileCount.entrySet()) {
                        Assert.assertEquals(10, entry.getValue().intValue());
                    }
                }
            }
            try (Statement stat = conn.createStatement()) {
                try (ResultSet rs =
                        stat.executeQuery(
                                "select * from external_files(path='internal_stage://"
                                        + stageName
                                        + "') as (a int, b text, c float4)")) {
                    int count = 0;
                    while (rs.next()) {
                        ++count;
                        int a = rs.getInt(1);
                        Assert.assertTrue(rs.getString(2).startsWith("name_" + (a / 2)));
                    }
                    Assert.assertEquals(102400, count);
                }
            }
        }
    }

    static String CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private static final SecureRandom random = new SecureRandom();

    public static String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(CHARACTERS.charAt(random.nextInt(CHARACTERS.length())));
        }
        return sb.toString();
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
                TableName tn = TableName.valueOf(tableName);
                TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);
                // 传入columns且顺序和schema不一致, 内部会自动按holo的字段排序
                try (CopyInWrapper copyIn =
                        new CopyInWrapper(
                                conn,
                                schema,
                                Arrays.asList("id", "ds", "b"),
                                CopyFormat.BINARY,
                                CopyMode.STREAM,
                                OnConflictAction.INSERT_OR_UPDATE,
                                1024 * 1024 * 10)) {

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
                TableName tn = TableName.valueOf(tableName);
                TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);
                try {
                    CopyInWrapper copyIn =
                            new CopyInWrapper(
                                    conn,
                                    schema,
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
                                schema,
                                Arrays.asList("e", "d"),
                                CopyFormat.BINARY,
                                CopyMode.STREAM,
                                OnConflictAction.INSERT_OR_UPDATE,
                                1024 * 1024 * 10)) {
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
                                schema,
                                Arrays.asList("d", "e"),
                                CopyFormat.BINARY,
                                CopyMode.STREAM,
                                OnConflictAction.INSERT_OR_UPDATE,
                                1024 * 1024 * 10)) {
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

    @Test
    public void testRecordChecker001() throws Exception {
        if (properties == null) {
            return;
        }

        Object[][] combinations = buildTypeCaseDataCombinations();
        List<Callable<Void>> tasks = new ArrayList<>();

        for (Object[] combo : combinations) {
            TypeCaseData typeCaseData = (TypeCaseData) combo[0];
            CopyFormat format = (CopyFormat) combo[1];
            boolean streamMode = (boolean) combo[2];
            boolean useFixedFe = (boolean) combo[3];
            int index = (int) combo[4];
            String typeName = typeCaseData.getName();

            if (useFixedFe
                    && format != CopyFormat.BINARYROW
                    && Objects.equals(typeName, "roaringbitmap")) {
                continue;
            }
            if (useFixedFe && holoVersion.compareTo(new HoloVersion("3.1.0")) < 0) {
                continue;
            } else if (format == CopyFormat.BINARYROW
                    && holoVersion.compareTo(new HoloVersion("4.1.0")) < 0) {
                continue;
            } else if (!streamMode && (typeName.equals("varchar") || typeName.equals("char"))) {
                continue;
            }

            String tableName =
                    "\"holo_client_record_checker_sql_001_"
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
            String createSql =
                    "create table "
                            + tableName
                            + "(c0 "
                            + typeCaseData.getColumnType()
                            + ",id int not null,primary key(id))";

            tasks.add(
                    tableTask(
                            tableName,
                            createSql,
                            conn -> {
                                LOG.info(
                                        "current type {}, format {}, streamMode {}",
                                        typeName,
                                        format,
                                        streamMode);
                                PgConnection pgConn = conn.unwrap(PgConnection.class);
                                HoloVersion version = ConnectionUtil.getHoloVersion(pgConn);
                                TableName tn = TableName.valueOf(tableName);
                                ConnectionUtil.checkMeta(pgConn, version, tn.getFullName(), 120);
                                TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);

                                try (Connection copyConn = buildConnection(useFixedFe)) {
                                    Record record = new Record(schema);
                                    Object obj =
                                            typeCaseData
                                                    .getSupplier()
                                                    .apply(0, conn.unwrap(BaseConnection.class));
                                    record.setObject(0, obj);
                                    record.setObject(1, 1);
                                    RecordChecker.check(record);
                                    CopyInWrapper copyIn =
                                            new CopyInWrapper(
                                                    copyConn,
                                                    record,
                                                    format,
                                                    streamMode
                                                            ? CopyMode.STREAM
                                                            : CopyMode.BULK_LOAD,
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
                            }));
        }

        runParallelTasks(tasks);
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
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(stream_mode true, format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N', on_conflict update)");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema, CopyFormat.CSV, OnConflictAction.INSERT_OR_IGNORE),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(stream_mode true, format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N', on_conflict ignore)");
                // new interface
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema,
                                CopyFormat.CSV,
                                OnConflictAction.INSERT_OR_IGNORE,
                                CopyMode.STREAM),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(stream_mode true, format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N', on_conflict ignore)");

                // stream_mode = false, don't care binary, don't care OnConflictAction default
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema,
                                CopyFormat.BINARY,
                                OnConflictAction.INSERT_OR_UPDATE,
                                false),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N')");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema,
                                CopyFormat.BINARY,
                                OnConflictAction.INSERT_OR_IGNORE,
                                false),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N')");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema, CopyFormat.CSV, OnConflictAction.INSERT_OR_UPDATE, false),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N')");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema,
                                CopyFormat.CSV,
                                OnConflictAction.INSERT_OR_UPDATE,
                                CopyMode.BULK_LOAD),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N')");

                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema,
                                CopyFormat.BINARY,
                                OnConflictAction.INSERT_OR_UPDATE,
                                CopyMode.BULK_LOAD_ON_CONFLICT),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N', on_conflict update)");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema,
                                CopyFormat.BINARY,
                                OnConflictAction.INSERT_OR_IGNORE,
                                CopyMode.BULK_LOAD_ON_CONFLICT),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N', on_conflict ignore)");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                schema,
                                CopyFormat.CSV,
                                OnConflictAction.INSERT_OR_UPDATE,
                                CopyMode.BULK_LOAD_ON_CONFLICT),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name,address) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N', on_conflict update)");

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
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(stream_mode true, format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N', on_conflict update)");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record, CopyFormat.CSV, OnConflictAction.INSERT_OR_IGNORE),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(stream_mode true, format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N', on_conflict ignore)");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record,
                                CopyFormat.CSV,
                                OnConflictAction.INSERT_OR_IGNORE,
                                CopyMode.STREAM),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(stream_mode true, format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N', on_conflict ignore)");

                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record,
                                CopyFormat.BINARY,
                                OnConflictAction.INSERT_OR_UPDATE,
                                false),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N')");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record,
                                CopyFormat.BINARY,
                                OnConflictAction.INSERT_OR_IGNORE,
                                false),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N')");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record, CopyFormat.CSV, OnConflictAction.INSERT_OR_UPDATE, false),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N')");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record,
                                CopyFormat.CSV,
                                OnConflictAction.INSERT_OR_UPDATE,
                                CopyMode.BULK_LOAD),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N')");

                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record,
                                CopyFormat.BINARY,
                                OnConflictAction.INSERT_OR_UPDATE,
                                CopyMode.BULK_LOAD_ON_CONFLICT),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N', on_conflict update)");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record,
                                CopyFormat.BINARY,
                                OnConflictAction.INSERT_OR_IGNORE,
                                CopyMode.BULK_LOAD_ON_CONFLICT),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N', on_conflict ignore)");
                Assert.assertEquals(
                        CopyUtil.buildCopyInSql(
                                record,
                                CopyFormat.CSV,
                                OnConflictAction.INSERT_OR_UPDATE,
                                CopyMode.BULK_LOAD_ON_CONFLICT),
                        "copy \"public\".\"holo_client_copy_util_001\"(id,name) from stdin with(format csv, DELIMITER ',', ESCAPE '\\', QUOTE '\"', NULL '\\N', on_conflict update)");
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /**
     * test buildPartitionClause and buildInsertTableSelectFromStageSql with partition parameters.
     */
    @Test
    public void testCopyUtilPartitionClause() throws Exception {
        if (properties == null) {
            return;
        }
        try (Connection conn = buildConnection()) {
            String tableName = "holo_client_copy_util_partition_clause";
            String dropSql = "drop table if exists " + tableName;
            // 包含text和int类型的分区列,验证quoting逻辑
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null, name text, ds text, kind int, primary key(id))";
            try {
                execute(conn, new String[] {dropSql, createSql});
                TableName tn = TableName.valueOf(tableName);
                TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);

                // 1. 空partition参数,返回空字符串
                Assert.assertEquals(CopyUtil.buildPartitionClause(schema, null, null), "");
                Assert.assertEquals(
                        CopyUtil.buildPartitionClause(schema, new String[] {}, new String[][] {}),
                        "");

                // 2. 单分区列(text类型),单个分区值,值需要加引号
                Assert.assertEquals(
                        CopyUtil.buildPartitionClause(
                                schema, new String[] {"ds"}, new String[][] {{"20250101"}}),
                        "PARTITION (ds = '20250101')");

                // 3. 单分区列(int类型),单个分区值,值不需要加引号
                Assert.assertEquals(
                        CopyUtil.buildPartitionClause(
                                schema, new String[] {"kind"}, new String[][] {{"100"}}),
                        "PARTITION (kind = 100)");

                // 4. 多分区列(text+int),多个分区值组合
                Assert.assertEquals(
                        CopyUtil.buildPartitionClause(
                                schema,
                                new String[] {"ds", "kind"},
                                new String[][] {{"20250101", "100"}, {"20250102", "200"}}),
                        "PARTITION (ds = '20250101', kind = 100) PARTITION (ds = '20250102', kind = 200)");

                // 5. 分区值包含单引号,需要转义
                Assert.assertEquals(
                        CopyUtil.buildPartitionClause(
                                schema, new String[] {"ds"}, new String[][] {{"it's"}}),
                        "PARTITION (ds = 'it''s')");

                // 6. buildInsertTableSelectFromStageSql with partition parameters (insert into)
                List<String> columnNames = Arrays.asList("id", "name");
                String sql =
                        buildInsertTableSelectFromStageSql(
                                schema,
                                columnNames,
                                Collections.singletonList("my_stage"),
                                OnConflictAction.INSERT_OR_UPDATE,
                                new String[] {"ds"},
                                new String[][] {{"20250101"}});
                LOG.info("insert with partition sql: {}", sql);
                Assert.assertTrue(
                        sql.contains("PARTITION (ds = '20250101')"),
                        "SQL should contain PARTITION clause");
                Assert.assertTrue(
                        sql.startsWith("insert into"), "SQL should start with insert into");
                Assert.assertTrue(
                        sql.contains("on conflict"), "SQL should contain on conflict clause");

                // 7. buildInsertOverwriteTableSelectFromStageSql with partition parameters
                String overwriteSql =
                        CopyUtil.buildInsertOverwriteTableSelectFromStageSql(
                                schema,
                                columnNames,
                                Collections.singletonList("my_stage"),
                                new String[] {"ds", "kind"},
                                new String[][] {{"20250101", "100"}});
                LOG.info("insert overwrite with partition sql: {}", overwriteSql);
                Assert.assertTrue(
                        overwriteSql.contains("PARTITION (ds = '20250101', kind = 100)"),
                        "SQL should contain PARTITION clause");
                Assert.assertTrue(
                        overwriteSql.startsWith("insert overwrite"),
                        "SQL should start with insert overwrite");

                // 8. 不传partition参数时,SQL与原有行为一致(无PARTITION关键字)
                String noPartitionSql =
                        buildInsertTableSelectFromStageSql(
                                schema,
                                columnNames,
                                Collections.singletonList("my_stage"),
                                OnConflictAction.INSERT_OR_UPDATE);
                Assert.assertFalse(
                        noPartitionSql.contains("PARTITION"),
                        "SQL should not contain PARTITION when no partition params");
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }

    /**
     * copy stage: test insert into logical partition table with two partition columns via stage.
     */
    @Test
    public void testCopyStageLogicalPartition() throws Exception {
        if (properties == null) {
            return;
        }
        if (holoVersion.compareTo(new HoloVersion("4.1.0")) < 0) {
            return;
        }
        Properties info = new Properties();
        info.setProperty(PGProperty.PREFER_QUERY_MODE.getName(), "simple");
        try (Connection conn = buildConnection(info);
                HoloClient client = new HoloClient(buildConfig())) {
            String stageName = "test_stage_lp_" + System.currentTimeMillis();
            String createStageSql =
                    "call hologres.hg_create_internal_stage('"
                            + stageName
                            + "', 'test_group', 7200);";
            String dropStageSql = "call hologres.hg_drop_internal_stage('" + stageName + "');";
            String tableName = "test_stage_logical_partition_001";
            String dropSql = "drop table if exists " + tableName;
            // 两个分区列: ds(text需加引号) 和 kind(int不需要引号)
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null, name text, ds text, kind int not null,"
                            + " primary key(id, ds, kind))"
                            + " logical partition by list(ds, kind)";

            tryExecute(conn, new String[] {dropStageSql});
            execute(conn, new String[] {dropSql, createSql, createStageSql});

            try {
                TableSchema schema = client.getTableSchema(tableName);
                // 逻辑分区表: 写入列需包含所有PK列, 否则on conflict会报错
                List<String> columns = Arrays.asList("id", "name", "ds", "kind");

                // 写入数据到stage
                try (RecordArrowWriter arrowWriter = new RecordArrowWriter(schema, columns, 8192);
                        CopyInStageWrapper<Record> copyIn =
                                new CopyInStageWrapper<>(
                                        buildConfig(), stageName, "test_file", arrowWriter)) {
                    // 写入15行: 分区1(20250101,1) 5行, 分区2(20250102,2) 5行, 分区3(20250103,3) 5行
                    for (int i = 0; i < 15; ++i) {
                        Put put = new Put(schema);
                        put.setObject("id", i);
                        put.setObject("name", "name_" + i);
                        if (i % 3 == 0) {
                            put.setObject("ds", "20250101");
                            put.setObject("kind", 1);
                        } else if (i % 3 == 1) {
                            put.setObject("ds", "20250102");
                            put.setObject("kind", 2);
                        } else {
                            put.setObject("ds", "20250103");
                            put.setObject("kind", 3);
                        }
                        copyIn.putRecord(put.getRecord());
                    }
                }

                // 使用带partition参数的方法生成SQL并执行
                // ds是text(值加引号), kind是int(值不加引号)
                String insertSql =
                        buildInsertTableSelectFromStageSql(
                                schema,
                                columns,
                                Collections.singletonList(stageName),
                                OnConflictAction.INSERT_OR_UPDATE,
                                new String[] {"ds", "kind"},
                                new String[][] {{"20250101", "1"}, {"20250102", "2"}});
                LOG.info("insert with logical partition sql: {}", insertSql);
                Assert.assertTrue(
                        insertSql.contains("PARTITION"),
                        "insert SQL should contain PARTITION clause");
                // 验证text类型ds加引号, int类型kind不加引号
                Assert.assertTrue(
                        insertSql.contains("ds = '20250101'"),
                        "text partition value should be quoted");
                Assert.assertTrue(
                        insertSql.contains("kind = 1"), "int partition value should not be quoted");

                try (Statement stat = conn.createStatement()) {
                    stat.execute(insertSql);
                }

                // 验证: 只有分区1和分区2的数据被写入(各5行), 分区3的数据被过滤
                int count = 0;
                try (Statement stat = conn.createStatement()) {
                    try (ResultSet rs =
                            stat.executeQuery("select * from " + tableName + " order by id")) {
                        while (rs.next()) {
                            int id = rs.getInt("id");
                            // 只有 i%3==0 (分区1) 和 i%3==1 (分区2) 的行被写入
                            Assert.assertTrue(
                                    id % 3 == 0 || id % 3 == 1,
                                    "only partition 1 and 2 data should exist, but found id=" + id);
                            ++count;
                        }
                    }
                }
                Assert.assertEquals(10, count, "should have 10 rows (5 per partition)");

                // 验证: stage中分区3(20250103,3)的5行数据未被写入表
                try (Statement stat = conn.createStatement()) {
                    try (ResultSet rs =
                            stat.executeQuery(
                                    "select count(*) from "
                                            + tableName
                                            + " where ds = '20250103' and kind = 3")) {
                        Assert.assertTrue(rs.next());
                        Assert.assertEquals(
                                0,
                                rs.getInt(1),
                                "partition (ds='20250103', kind=3) should have no data");
                    }
                }
            } finally {
                execute(conn, new String[] {dropSql});
                tryExecute(conn, new String[] {dropStageSql});
            }
        }
    }

    /**
     * 覆盖所有支持的逻辑分区键类型: int, text, varchar, date, timestamp, timestamptz.
     * 每种类型使用刁钻的测试数据(单引号、反斜杠、边界值等).
     */
    @Test
    public void testCopyStageLogicalPartitionAllTypes() throws Exception {
        if (properties == null) {
            return;
        }
        if (holoVersion == null || holoVersion.compareTo(new HoloVersion("4.1.0")) < 0) {
            return;
        }
        // 每一项: {类型def, value1, value2, value3(不在PARTITION子句中)}
        // 注意: int/date类型必须传对应的Java对象, text/timestamp/timestamptz可传String
        Object[][] cases = {
            {"int not null", 0, -2147483648, 2147483647},
            {"text not null", "O'Brien", "a\\b\"c", "中文;,'"},
            {"varchar(64) not null", "it's ok", "x\\y", "foo'bar'baz"},
            {
                "date not null",
                java.sql.Date.valueOf("2025-01-01"),
                java.sql.Date.valueOf("2025-12-31"),
                java.sql.Date.valueOf("1970-01-01")
            },
            {
                "timestamp not null",
                java.sql.Timestamp.valueOf("2025-01-01 00:00:00"),
                java.sql.Timestamp.valueOf("2025-06-15 12:34:56.123456"),
                java.sql.Timestamp.valueOf("2099-12-31 23:59:59")
            },
            {
                "timestamptz not null",
                java.sql.Timestamp.valueOf("2025-01-01 00:00:00"),
                java.sql.Timestamp.valueOf("2025-06-15 12:34:56.123"),
                java.sql.Timestamp.valueOf("2099-12-31 23:59:59")
            },
        };

        Properties info = new Properties();
        info.setProperty(PGProperty.PREFER_QUERY_MODE.getName(), "simple");
        for (Object[] aCase : cases) {
            String colType = String.valueOf(aCase[0]);
            String v1 = String.valueOf(aCase[1]);
            String v2 = String.valueOf(aCase[2]);
            String v3 = String.valueOf(aCase[3]);
            String typeShort = colType.split(" ")[0].replaceAll("[^a-zA-Z]", "");
            LOG.info("==== test logical partition type: {} ====", colType);

            try (Connection conn = buildConnection(info);
                    HoloClient client = new HoloClient(buildConfig())) {
                String stageName =
                        "test_stage_lp_t_" + typeShort + "_" + System.currentTimeMillis();
                String createStageSql =
                        "call hologres.hg_create_internal_stage('"
                                + stageName
                                + "', 'test_group', 7200);";
                String dropStageSql = "call hologres.hg_drop_internal_stage('" + stageName + "');";
                String tableName = "test_stage_lp_alltypes_" + typeShort;
                String dropSql = "drop table if exists " + tableName;
                String createSql =
                        "create table "
                                + tableName
                                + "(id int not null, name text, p "
                                + colType
                                + ", primary key(id, p)) logical partition by list(p)";

                tryExecute(conn, new String[] {dropStageSql});
                execute(conn, new String[] {dropSql, createSql, createStageSql});

                try {
                    TableSchema schema = client.getTableSchema(tableName);
                    List<String> columns = Arrays.asList("id", "name", "p");
                    Object[] threeValues = {aCase[1], aCase[2], aCase[3]};

                    try (RecordArrowWriter arrowWriter =
                                    new RecordArrowWriter(schema, columns, 8192);
                            CopyInStageWrapper<Record> copyIn =
                                    new CopyInStageWrapper<>(
                                            buildConfig(), stageName, "test_file", arrowWriter)) {
                        for (int i = 0; i < 12; ++i) {
                            Put put = new Put(schema);
                            put.setObject("id", i);
                            put.setObject("name", "name_" + i);
                            put.setObject("p", threeValues[i % 3]); // 直接传字符串, Arrow Writer内部自动解析
                            copyIn.putRecord(put.getRecord());
                        }
                    }

                    // 先测试不带 PARTITION 子句的 INSERT, 看 Stage 中 timestamptz 值是否能正常写入
                    String insertSqlNoPartition =
                            buildInsertTableSelectFromStageSql(
                                    schema,
                                    columns,
                                    Collections.singletonList(stageName),
                                    OnConflictAction.INSERT_OR_UPDATE);
                    LOG.info(
                            "type {} insert sql (no partition): {}", colType, insertSqlNoPartition);

                    try (Statement stat = conn.createStatement()) {
                        stat.execute(insertSqlNoPartition);
                    }

                    // 查询表中所有数据, 用于诊断问题
                    try (Statement stat = conn.createStatement()) {
                        try (ResultSet rs =
                                stat.executeQuery(
                                        "select id, p, p::text as p_text from "
                                                + tableName
                                                + " order by id")) {
                            StringBuilder sb = new StringBuilder();
                            int actualCount = 0;
                            while (rs.next()) {
                                int id = rs.getInt(1);
                                String pVal = rs.getString(2);
                                String pText = rs.getString(3);
                                sb.append(
                                        String.format(
                                                "[id=%d, p=%s, p::text=%s] ", id, pVal, pText));
                                actualCount++;
                            }
                            LOG.info(
                                    "type {} actual data in table without PARTITION (count={}): {}",
                                    colType,
                                    actualCount,
                                    sb);
                        }
                    }

                    int count = 0;
                    try (Statement stat = conn.createStatement()) {
                        try (ResultSet rs =
                                stat.executeQuery("select count(*) from " + tableName)) {
                            Assert.assertTrue(rs.next());
                            count = rs.getInt(1);
                        }
                    }

                    // 验证不带PARTITION的写入结果
                    Assert.assertEquals(
                            12,
                            count,
                            "type=" + colType + " should have 12 rows without PARTITION");

                    String insertSql =
                            buildInsertTableSelectFromStageSql(
                                    schema,
                                    columns,
                                    Collections.singletonList(stageName),
                                    OnConflictAction.INSERT_OR_UPDATE,
                                    new String[] {"p"},
                                    new String[][] {{v1}, {v2}});
                    LOG.info("type {} insert sql: {}", colType, insertSql);
                    Assert.assertTrue(
                            insertSql.contains("PARTITION"),
                            "insert SQL should contain PARTITION clause");

                    // 先drop表重建, 重新测试带PARTITION的
                    execute(conn, new String[] {dropSql, createSql});

                    // 重新写入 stage
                    try (RecordArrowWriter arrowWriter =
                                    new RecordArrowWriter(schema, columns, 8192);
                            CopyInStageWrapper<Record> copyIn =
                                    new CopyInStageWrapper<>(
                                            buildConfig(), stageName, "test_file", arrowWriter)) {
                        for (int i = 0; i < 12; ++i) {
                            Put put = new Put(schema);
                            put.setObject("id", i);
                            put.setObject("name", "name_" + i);
                            put.setObject("p", threeValues[i % 3]); // 直接传字符串, Arrow Writer内部自动解析
                            copyIn.putRecord(put.getRecord());
                        }
                    }

                    try (Statement stat = conn.createStatement()) {
                        stat.execute(insertSql);
                    }

                    count = 0;
                    try (Statement stat = conn.createStatement()) {
                        try (ResultSet rs =
                                stat.executeQuery("select count(*) from " + tableName)) {
                            Assert.assertTrue(rs.next());
                            count = rs.getInt(1);
                        }
                    }
                    Assert.assertEquals(
                            8, count, "type=" + colType + " should have 8 rows after insert");

                    try (Statement stat = conn.createStatement()) {
                        String quotedV3 = v3.replace("'", "''");
                        try (ResultSet rs =
                                stat.executeQuery(
                                        "select count(*) from "
                                                + tableName
                                                + " where p = '"
                                                + quotedV3
                                                + "'")) {
                            Assert.assertTrue(rs.next());
                            Assert.assertEquals(
                                    0,
                                    rs.getInt(1),
                                    "type=" + colType + " partition v3 should have no data");
                        }
                    }
                } finally {
                    execute(conn, new String[] {dropSql});
                    tryExecute(conn, new String[] {dropStageSql});
                }
            }
        }
    }

    /**
     * Verify that various string values (including "N", "\N", empty string, null, etc.) are
     * correctly handled when written via COPY CSV format. Ensures that changing the NULL marker to
     * \N prevents "N" from being misinterpreted as NULL.
     */
    @Test
    public void testCopyNullStringMarker() throws Exception {
        if (properties == null) {
            return;
        }
        try (Connection conn = buildConnection()) {
            String tableName = "holo_client_copy_null_string_marker";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table " + tableName + "(id int not null, val text, primary key(id))";
            try {
                execute(conn, new String[] {dropSql, createSql});

                TableName tn = TableName.valueOf(tableName);
                TableSchema schema = ConnectionUtil.getTableSchema(conn, tn);

                // Test data: id -> expected value
                Map<Integer, String> testData = new HashMap<>();
                testData.put(0, "N"); // single char N, old version would wrongly treat as null
                testData.put(1, "hello"); // normal string
                testData.put(2, ""); // empty string
                testData.put(3, null); // actual null
                testData.put(4, "NULL"); // string "NULL"
                testData.put(5, "n"); // lowercase n
                testData.put(6, ","); // delimiter
                testData.put(7, "\""); // quote char
                testData.put(8, "\\"); // backslash
                testData.put(9, "ab"); // contains newline
                testData.put(10, "\\N"); // null marker

                // Write via COPY CSV
                try (Connection copyConn = buildConnection();
                        CopyInWrapper copyIn =
                                new CopyInWrapper(
                                        copyConn,
                                        schema,
                                        CopyFormat.CSV,
                                        CopyMode.BULK_LOAD,
                                        OnConflictAction.INSERT_OR_REPLACE,
                                        1024 * 1024 * 10)) {
                    for (Map.Entry<Integer, String> entry : testData.entrySet()) {
                        Record record = Record.build(schema);
                        record.setObject(0, entry.getKey());
                        record.setObject(1, entry.getValue());
                        copyIn.putRecord(record);
                    }
                }

                // Read back and verify
                try (Statement stat = conn.createStatement()) {
                    try (ResultSet rs =
                            stat.executeQuery(
                                    "select id, val from " + tableName + " order by id")) {
                        int count = 0;
                        while (rs.next()) {
                            int id = rs.getInt(1);
                            String expected = testData.get(id);
                            String actual = rs.getString(2);
                            if (expected == null) {
                                Assert.assertNull(
                                        actual, "id=" + id + " expected null but got: " + actual);
                                Assert.assertTrue(
                                        rs.wasNull(), "id=" + id + " expected wasNull()=true");
                            } else {
                                Assert.assertNotNull(
                                        actual,
                                        "id="
                                                + id
                                                + " expected non-null '"
                                                + expected
                                                + "' but got null");
                                Assert.assertEquals(
                                        actual, expected, "id=" + id + " value mismatch");
                            }
                            count++;
                        }
                        Assert.assertEquals(count, testData.size());
                    }
                }
            } finally {
                // execute(conn, new String[] {dropSql});
            }
        }
    }
}
