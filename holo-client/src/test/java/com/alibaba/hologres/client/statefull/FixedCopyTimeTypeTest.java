package com.alibaba.hologres.client.statefull;

import com.alibaba.hologres.client.HoloClientTestBase;
import com.alibaba.hologres.client.copy.CopyFormat;
import com.alibaba.hologres.client.copy.CopyUtil;
import com.alibaba.hologres.client.copy.in.CopyInOutputStream;
import com.alibaba.hologres.client.copy.in.RecordBinaryOutputStream;
import com.alibaba.hologres.client.copy.in.RecordOutputStream;
import com.alibaba.hologres.client.copy.in.RecordTextOutputStream;
import com.alibaba.hologres.client.copy.in.binaryrow.RecordBinaryRowOutputStream;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.PgConnection;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Callable;

public class FixedCopyTimeTypeTest extends HoloClientTestBase {
    /** time timetz timestamp timestamptz test. */
    @Test(groups = "nonConcurrentGroup")
    public void testFixedCopyTimeType() throws Exception {
        if (properties == null) {
            return;
        }
        boolean[] useFixedFeList = new boolean[] {true, false};
        boolean[] setTimeZoneList = new boolean[] {true, false};
        CopyFormat[] formatList =
                new CopyFormat[] {CopyFormat.BINARY, CopyFormat.BINARYROW, CopyFormat.CSV};
        SimpleDateFormat timetzSDF = new SimpleDateFormat("HH:mm:ssZ");
        SimpleDateFormat timestamptzSDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");

        // Check version once
        HoloVersion version;
        try (Connection conn = buildConnection()) {
            version = ConnectionUtil.getHoloVersion(conn);
        }

        for (boolean setTimeZone : setTimeZoneList) {
            TimeZone lastTimeZone = TimeZone.getDefault();
            try {
                if (setTimeZone) {
                    TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.ofHours(-3)));
                }
                Map<String, Object[]> typeCaseDataMap = new LinkedHashMap<>();
                typeCaseDataMap.put(
                        "time",
                        new Object[] {
                            java.sql.Time.valueOf("00:00:00"),
                            new Time(timetzSDF.parse("10:00:00+0700").getTime())
                        });
                typeCaseDataMap.put(
                        "timetz",
                        new Object[] {
                            java.sql.Time.valueOf("00:00:00"),
                            new Time(timetzSDF.parse("10:00:00+0700").getTime())
                        });
                typeCaseDataMap.put(
                        "timestamp",
                        new Object[] {
                            java.sql.Timestamp.valueOf("1901-01-03 00:00:00.0"),
                            new Timestamp(
                                    timestamptzSDF.parse("1901-01-03 00:00:00+0700").getTime())
                        });
                typeCaseDataMap.put(
                        "timestamptz",
                        new Object[] {
                            java.sql.Timestamp.valueOf("1901-01-03 00:00:00.0"),
                            new Timestamp(
                                    timestamptzSDF.parse("1901-01-03 00:00:00+0700").getTime())
                        });

                // Build parallel tasks: each task handles its own DDL + COPY IN + verify
                List<Callable<Void>> tasks = new ArrayList<>();
                for (CopyFormat format : formatList) {
                    if (format == CopyFormat.BINARYROW
                            && version.compareTo(new HoloVersion("3.2.0")) < 0) {
                        continue;
                    }
                    for (boolean useFixedFe : useFixedFeList) {
                        if (useFixedFe && version.compareTo(new HoloVersion("3.1.0")) < 0) {
                            continue;
                        }
                        for (Map.Entry<String, Object[]> entry : typeCaseDataMap.entrySet()) {
                            String typeName = entry.getKey();
                            Object[] values = entry.getValue();
                            CopyFormat taskFormat = format;
                            boolean taskFixedFe = useFixedFe;
                            String tableName =
                                    "\"holo_client_copy_sql_005_"
                                            + format
                                            + "_"
                                            + useFixedFe
                                            + "_"
                                            + setTimeZone
                                            + "_"
                                            + typeName
                                            + "\"";
                            String createSql =
                                    "create table "
                                            + tableName
                                            + "(id int primary key, col "
                                            + typeName
                                            + ")";
                            tasks.add(
                                    tableTask(
                                            tableName,
                                            createSql,
                                            taskConn -> {
                                                TableName tn = TableName.valueOf(tableName);
                                                ConnectionUtil.checkMeta(
                                                        taskConn, version, tn.getFullName(), 120);
                                                TableSchema schema =
                                                        ConnectionUtil.getTableSchema(taskConn, tn);

                                                try (Connection pgConn =
                                                        buildConnection(taskFixedFe)
                                                                .unwrap(PgConnection.class)) {
                                                    CopyManager copyManager =
                                                            new CopyManager(
                                                                    pgConn.unwrap(
                                                                            PgConnection.class));

                                                    // COPY IN
                                                    RecordOutputStream ros = null;
                                                    for (int i = 0; i < values.length; ++i) {
                                                        Record record = new Record(schema);
                                                        record.setObject(0, i);
                                                        record.setObject(1, values[i]);
                                                        if (ros == null) {
                                                            String copySql =
                                                                    CopyUtil.buildCopyInSql(
                                                                            record,
                                                                            taskFormat,
                                                                            OnConflictAction
                                                                                    .INSERT_OR_UPDATE);
                                                            LOG.info("copySql : {}", copySql);
                                                            OutputStream os =
                                                                    new CopyInOutputStream(
                                                                            copyManager.copyIn(
                                                                                    copySql));
                                                            ros =
                                                                    taskFormat == CopyFormat.BINARY
                                                                            ? new RecordBinaryOutputStream(
                                                                                    os,
                                                                                    schema,
                                                                                    pgConn.unwrap(
                                                                                            BaseConnection
                                                                                                    .class),
                                                                                    1024 * 1024
                                                                                            * 10)
                                                                            : taskFormat
                                                                                            == CopyFormat
                                                                                                    .BINARYROW
                                                                                    ? new RecordBinaryRowOutputStream(
                                                                                            os,
                                                                                            schema,
                                                                                            pgConn
                                                                                                    .unwrap(
                                                                                                            BaseConnection
                                                                                                                    .class),
                                                                                            1024
                                                                                                    * 1024
                                                                                                    * 10)
                                                                                    : new RecordTextOutputStream(
                                                                                            os,
                                                                                            schema,
                                                                                            pgConn
                                                                                                    .unwrap(
                                                                                                            BaseConnection
                                                                                                                    .class),
                                                                                            1024
                                                                                                    * 1024
                                                                                                    * 10);
                                                        }
                                                        ros.putRecord(record);
                                                    }
                                                    ros.close();
                                                }

                                                // Verify
                                                int count = 0;
                                                try (Statement stat = taskConn.createStatement()) {
                                                    try (ResultSet rs =
                                                            stat.executeQuery(
                                                                    "select * from "
                                                                            + tableName
                                                                            + " order by id")) {
                                                        while (rs.next()) {
                                                            Assert.assertEquals(
                                                                    count, rs.getInt(1));
                                                            Assert.assertEquals(
                                                                    values[count], rs.getObject(2));
                                                            ++count;
                                                        }
                                                        Assert.assertEquals(values.length, count);
                                                    }
                                                }
                                            }));
                        }
                    }
                }

                // Execute all tasks in parallel
                runParallelTasks(tasks);
            } finally {
                TimeZone.setDefault(lastTimeZone);
            }
        }
    }
}
