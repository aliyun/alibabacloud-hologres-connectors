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
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

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
        for (boolean setTimeZone : setTimeZoneList) {
            TimeZone lastTimeZone = TimeZone.getDefault();
            try {
                if (setTimeZone) {
                    TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.ofHours(-3)));
                }
                Map<String, Object[]> typeCaseDataMap = new HashMap<>();
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
                for (CopyFormat format : formatList) {
                    for (boolean useFixedFe : useFixedFeList) {
                        for (Map.Entry<String, Object[]> entry : typeCaseDataMap.entrySet()) {
                            try (Connection conn = buildConnection()) {
                                HoloVersion version = ConnectionUtil.getHoloVersion(conn);
                                if (useFixedFe && version.compareTo(new HoloVersion("3.1.0")) < 0) {
                                    continue;
                                } else if (format == CopyFormat.BINARYROW
                                        && version.compareTo(new HoloVersion("3.2.0")) < 0) {
                                    continue;
                                }
                                String tableName =
                                        "\"holo_client_copy_sql_005_"
                                                + format
                                                + "_"
                                                + useFixedFe
                                                + "_"
                                                + setTimeZone
                                                + "_"
                                                + entry.getKey()
                                                + "\"";
                                String forceReplaySql =
                                        "set hg_experimental_force_sync_replay = on";
                                String dropSql = "drop table if exists " + tableName;
                                String createSql =
                                        "create table "
                                                + tableName
                                                + "(id int primary key,"
                                                + "col "
                                                + entry.getKey()
                                                + ")";
                                try {
                                    execute(conn, new String[] {forceReplaySql});
                                    execute(conn, new String[] {dropSql});
                                    execute(conn, new String[] {createSql});

                                    try (Connection pgConn =
                                            buildConnection(useFixedFe)
                                                    .unwrap(PgConnection.class)) {
                                        TableName tn = TableName.valueOf(tableName);
                                        ConnectionUtil.checkMeta(
                                                conn, version, tn.getFullName(), 120);

                                        TableSchema schema =
                                                ConnectionUtil.getTableSchema(conn, tn);
                                        CopyManager copyManager =
                                                new CopyManager(pgConn.unwrap(PgConnection.class));
                                        String copySql = null;
                                        OutputStream os = null;
                                        RecordOutputStream ros = null;
                                        for (int i = 0; i < entry.getValue().length; ++i) {
                                            Record record = new Record(schema);
                                            record.setObject(0, i);
                                            record.setObject(1, entry.getValue()[i]);
                                            if (ros == null) {
                                                copySql =
                                                        CopyUtil.buildCopyInSql(
                                                                record,
                                                                format,
                                                                OnConflictAction.INSERT_OR_UPDATE);
                                                LOG.info("copySql : {}", copySql);
                                                os =
                                                        new CopyInOutputStream(
                                                                copyManager.copyIn(copySql));
                                                ros =
                                                        format == CopyFormat.BINARY
                                                                ? new RecordBinaryOutputStream(
                                                                        os,
                                                                        schema,
                                                                        pgConn.unwrap(
                                                                                BaseConnection
                                                                                        .class),
                                                                        1024 * 1024 * 10)
                                                                : format == CopyFormat.BINARYROW
                                                                        ? new RecordBinaryRowOutputStream(
                                                                                os,
                                                                                schema,
                                                                                pgConn.unwrap(
                                                                                        BaseConnection
                                                                                                .class),
                                                                                1024 * 1024 * 10)
                                                                        : new RecordTextOutputStream(
                                                                                os,
                                                                                schema,
                                                                                pgConn.unwrap(
                                                                                        BaseConnection
                                                                                                .class),
                                                                                1024 * 1024 * 10);
                                            }
                                            // this record does not contain the third field address
                                            ros.putRecord(record);
                                        }
                                        ros.close();
                                    }

                                    int count = 0;
                                    try (Statement stat = conn.createStatement()) {
                                        try (ResultSet rs =
                                                stat.executeQuery(
                                                        "select * from "
                                                                + tableName
                                                                + " order by id")) {
                                            while (rs.next()) {
                                                Assert.assertEquals(count, rs.getInt(1));
                                                Assert.assertEquals(
                                                        entry.getValue()[count], rs.getObject(2));
                                                ++count;
                                            }
                                            Assert.assertEquals(entry.getValue().length, count);
                                        }
                                    }
                                } finally {
                                    execute(conn, new String[] {dropSql});
                                }
                            }
                        }
                    }
                }
            } finally {
                TimeZone.setDefault(lastTimeZone);
            }
        }
    }
}
