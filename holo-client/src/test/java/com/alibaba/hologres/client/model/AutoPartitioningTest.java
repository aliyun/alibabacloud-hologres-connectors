package com.alibaba.hologres.client.model;

import com.alibaba.hologres.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

/** AutoPartitioning Tester. */
public class AutoPartitioningTest extends HoloClientTestBase {

    private static final Map<String, String> supportTimeZones = new LinkedHashMap<>();

    static {
        supportTimeZones.put("Asia/Shanghai", "Asia/Shanghai");
        supportTimeZones.put("America/Los_Angeles", "America/Los_Angeles");
        supportTimeZones.put("PRC", "PRC");

        supportTimeZones.put("UTC", "UTC");
        supportTimeZones.put("UTC+08:00", "UTC-08:00");
        supportTimeZones.put("UTC-07:00", "UTC+07:00");
        supportTimeZones.put("UTC+8", "UTC-08:00");
        supportTimeZones.put("UTC-7", "UTC+07:00");

        supportTimeZones.put("GMT", "GMT");
        supportTimeZones.put("GMT+8", "UTC-08:00");
        supportTimeZones.put("GMT-7", "UTC+07:00");
        supportTimeZones.put("GMT+08:00", "UTC-08:00");
        supportTimeZones.put("GMT-07:00", "UTC+07:00");

        supportTimeZones.put("+8", "UTC+08:00");
        supportTimeZones.put("-7", "UTC-07:00");
        supportTimeZones.put("+08:00", "UTC-08:00");
        supportTimeZones.put("-07:00", "UTC+07:00");

        supportTimeZones.put("EAST+08", "UTC-08:00");
        supportTimeZones.put("WAT+02abcdefg", "UTC-01:00");
    }

    @Test
    public void testSetTimeZone() throws Throwable {
        AutoPartitioning autoPartitioning = new AutoPartitioning();
        try (Connection conn = buildConnection()) {
            for (Map.Entry<String, String> entry : supportTimeZones.entrySet()) {
                if (!autoPartitioning.setTimeZone(entry.getKey())) {
                    autoPartitioning.setTimeZoneByOffset(getPgTimeZoneOffset(conn, entry.getKey()));
                }
                ZoneId zoneId = autoPartitioning.getTimeZoneId();

                Assert.assertEquals(zoneId.getId(), entry.getValue());

                // 验证java和postgres的时区设置是否一致, 当前时间所在的整点时间比较好处理, 如果测试运行时间接近0点, sleep一小下
                LocalTime now = LocalTime.now();
                int second = now.getSecond();
                int minute = now.getMinute();
                if (minute == 59 && second >= 40) {
                    Thread.sleep(20000);
                }
                DateTimeFormatter formatter =
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX'['VV']'");
                String javaResult =
                        ZonedDateTime.now(zoneId)
                                .truncatedTo(java.time.temporal.ChronoUnit.HOURS)
                                .format(formatter)
                                // java UTC时区,返回的结果类似"2023-03-01 08:00:00Z[UTC]", 这里替换下好和pg查询的结果比较
                                .replaceFirst("Z\\[UTC\\]", "+00")
                                .replaceFirst("Z\\[GMT\\]", "+00");

                String backendResult;
                try (Statement statement = conn.createStatement()) {
                    statement.execute("set timezone = '" + entry.getKey() + "';");
                    ResultSet rs =
                            statement.executeQuery(
                                    "select date_trunc('hour', now())::timestamptz::text;");
                    if (rs.next()) {
                        backendResult = rs.getString(1);
                    } else {
                        throw new RuntimeException("no result");
                    }
                }
                System.out.println(
                        String.format(
                                "timezone name: %s, timezoneId: %s, javaResult: %s, backendResult: %s",
                                entry.getKey(), zoneId.getId(), javaResult, backendResult));
                Assert.assertTrue(javaResult.contains(backendResult));
            }
        }
    }

    private int getPgTimeZoneOffset(Connection conn, String pgTimeZoneName) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);
            stmt.execute("BEGIN;");
            stmt.execute(String.format("set local timezone = '%s'", pgTimeZoneName));
            try (ResultSet rs =
                    stmt.executeQuery(
                            "SELECT EXTRACT(TIMEZONE FROM NOW()) AS timezone_offset_seconds;")) {
                if (rs.next()) {
                    return rs.getInt("timezone_offset_seconds");
                }
            }
        } finally {
            try (Statement statement = conn.createStatement()) {
                statement.execute("END;");
            } catch (SQLException ignored) {
            }
        }
        return 0;
    }

    @Test
    public void testAutoPartitionTableSchemaToString() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setWriteMode(WriteMode.INSERT_OR_REPLACE);

        for (Map.Entry<String, String> entry : supportTimeZones.entrySet()) {
            String pgTimeZoneName = entry.getKey();
            String javaTimeZoneId = entry.getValue();
            try (Connection conn = buildConnection();
                    HoloClient client = new HoloClient(config)) {
                String tableName = "\"holo_client_table_schema_to_string_" + pgTimeZoneName + "\"";

                String dropSql = "drop table if exists " + tableName;
                String createSql =
                        "create table "
                                + tableName
                                + "(id int not null, amount decimal(12,2), t text not null, ts timestamptz not null, ba bytea, t_a text[],i_a int[], primary key(id, t))\n "
                                + "PARTITION BY LIST (t)\n"
                                + "WITH (\n"
                                + "   clustering_key = 'ts',\n"
                                + "   orientation = 'row,column',\n"
                                + "   binlog_level = 'replica',\n"
                                + "   auto_partitioning_enable = 'true',\n"
                                + "   auto_partitioning_time_unit = 'DAY',\n"
                                + "   auto_partitioning_time_zone = '"
                                + pgTimeZoneName
                                + "'"
                                + ");";
                execute(conn, new String[] {dropSql, createSql});

                try {
                    TableSchema schema = client.getTableSchema(tableName, true);
                    Assert.assertEquals(
                            schema.toString(),
                            String.format(
                                    "TableSchema{\n"
                                            + "tableId='%s', \n"
                                            + "schemaVersion='%S', \n"
                                            + "tableName=\"public\".%s, \n"
                                            + "distributionKeys=[id, t], \n"
                                            + "clusteringKey=[ts:asc], \n"
                                            + "segmentKey=[ts], \n"
                                            + "partitionInfo='t', \n"
                                            + "orientation='row,column', \n"
                                            + "binlogLevel=REPLICA, \n"
                                            + "columns=[\n"
                                            + "Column{name='id', typeName='int4', not null, primary key}, \n"
                                            + "Column{name='amount', typeName='numeric'}, \n"
                                            + "Column{name='t', typeName='text', not null, primary key}, \n"
                                            + "Column{name='ts', typeName='timestamptz', not null}, \n"
                                            + "Column{name='ba', typeName='bytea'}, \n"
                                            + "Column{name='t_a', typeName='_text'}, \n"
                                            + "Column{name='i_a', typeName='_int4'}], \n"
                                            + "autoPartitioning=AutoPartitioning{enable=true, timeUnit=DAY, timeZoneId=%s, preCreateNum=4%s}}",
                                    schema.getTableId(),
                                    schema.getSchemaVersion(),
                                    tableName,
                                    javaTimeZoneId,
                                    holoVersion.compareTo(new HoloVersion(3, 0, 12)) >= 0
                                            ? ", timeFormat=YYYYMMDD"
                                            : ""));
                } finally {
                    execute(conn, new String[] {dropSql});
                }
            }
        }
    }
}
