package com.alibaba.hologres.spark.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Stage模式全自动分区检测 demo - 双分区列(day, hour), 仿 ga_dwd.fish_3d_user_stats.
 *
 * <p>不配置任何 write.target_partition_columns / write.target_partition_values 参数:
 * <ul>
 *   <li>分区列(day, hour)从目标表的元数据(LOGICAL PARTITION BY LIST(day, hour))自动解析;</li>
 *   <li>分区值从实际写入的数据中自动采集, 数据落到哪些 (day, hour) 分区就写入哪些分区;</li>
 *   <li>SaveMode.Overwrite 时为分区级覆盖: 只覆盖数据中出现的分区, 其他分区不受影响.</li>
 * </ul>
 *
 * <p>流程:
 * <ol>
 *   <li>Step 1: 写入两天共 48 个小时分区的数据(2026-02-17/18 各 24 小时, 每分区 100 行);</li>
 *   <li>Step 2: 用 Overwrite 只写 2026-02-18 的 00~05 共 6 个小时分区, 验证仅这 6 个分区被覆盖,
 *       其余 42 个分区不受影响.</li>
 * </ol>
 *
 * <p>使用方式: 在 setting.properties 中配置 USERNAME, PASSWORD, JDBCURL 后直接运行main方法。
 * 依赖 connector 1.6.3-SNAPSHOT (feat/spark-logical-partition-write 分支构建)。
 */
public class SparkStageAutoPartitionExample {

    private static final String TABLE_NAME = System.getProperty("demo.table", "ga_dwd.fish_3d_user_stats_demo");
    // 为 false 时建普通主键表(非逻辑分区), 用于对照 stage 基础路径
    private static final boolean LOGICAL_PARTITION = Boolean.parseBoolean(System.getProperty("demo.logicalPartition", "true"));
    // 每个 (day, hour) 分区写入的行数
    private static final int ROWS_PER_PARTITION = 100;

    private static final StructType SCHEMA = new StructType(new StructField[]{
            DataTypes.createStructField("player_id", DataTypes.LongType, false),
            DataTypes.createStructField("event_time", DataTypes.StringType, true),
            DataTypes.createStructField("ingest_time", DataTypes.StringType, true),
            DataTypes.createStructField("log", DataTypes.StringType, true),
            DataTypes.createStructField("day", DataTypes.StringType, false),
            DataTypes.createStructField("hour", DataTypes.StringType, false)
    });

    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        InputStream inputStream = SparkStageAutoPartitionExample.class.getClassLoader().getResourceAsStream("setting.properties");
        prop.load(inputStream);
        String username = prop.getProperty("USERNAME");
        String password = prop.getProperty("PASSWORD");
        String url = prop.getProperty("JDBCURL");

        recreateTable(url, username, password);

        SparkSession sparkSession =
                SparkSession.builder()
                        .appName("StageAutoPartitionDayHourExample")
                        .master("local[*]")
                        .getOrCreate();

        try {
            // ==================== Step 1: 全自动 INSERT, 48 个小时分区 ====================
            System.out.println("========== Step 1: stage 全自动模式 INSERT (两天 x 24 小时 = 48 个分区) ==========");
            Dataset<Row> initData = createData(sparkSession, new String[]{"2026-02-17", "2026-02-18"}, 24, ROWS_PER_PARTITION, 0);
            writeStage(initData, username, password, url, SaveMode.Append);
            System.out.println("写入完成, 期望: 48 个分区各 " + ROWS_PER_PARTITION + " 行, 总计 " + 48 * ROWS_PER_PARTITION + " 行");
            printPartitionCounts(url, username, password);

            // ==================== Step 2: 全自动分区级 OVERWRITE, 仅覆盖 6 个小时分区 ====================
            System.out.println("========== Step 2: stage 全自动模式 OVERWRITE (仅 2026-02-18 的 hour 00~05) ==========");
            Dataset<Row> overwriteData = createData(sparkSession, new String[]{"2026-02-18"}, 6, 50, 1_000_000);
            writeStage(overwriteData, username, password, url, SaveMode.Overwrite);
            System.out.println("覆盖完成, 期望: 2026-02-18 hour 00~05 各 50 行(新数据), 其余 42 个分区各 " + ROWS_PER_PARTITION + " 行(不变)");
            printPartitionCounts(url, username, password);

            System.out.println("========== Demo 结束 ==========");
        } finally {
            sparkSession.stop();
            dropTable(url, username, password);
        }
    }

    /** 全自动模式写入: 注意这里没有任何 target_partition_columns / values 参数. */
    private static void writeStage(Dataset<Row> df, String username, String password, String url, SaveMode mode) {
        df.write()
                .format("hologres")
                .option("username", username)
                .option("password", password)
                .option("jdbcurl", url)
                .option("table", TABLE_NAME)
                .option("write.mode", "stage")
                .option("write.on_conflict_action", "insertOrReplace")
                .mode(mode)
                .save();
    }

    /** 生成 days 天 x hours 个小时分区的数据, 每个分区 rowsPerPartition 行. */
    private static Dataset<Row> createData(SparkSession spark, String[] days, int hours, int rowsPerPartition, long pkOffset) {
        List<Row> rows = new ArrayList<>(days.length * hours * rowsPerPartition);
        long pk = pkOffset;
        for (String day : days) {
            for (int h = 0; h < hours; h++) {
                String hour = String.format("%02d", h);
                for (int i = 0; i < rowsPerPartition; i++) {
                    pk++;
                    rows.add(RowFactory.create(
                            pk,
                            day + " " + hour + ":00:00",
                            day + " " + hour + ":30:00",
                            "{\"player_id\":" + pk + ",\"action\":\"login\",\"level\":" + (pk % 100) + "}",
                            day,
                            hour
                    ));
                }
            }
        }
        return spark.createDataFrame(rows, SCHEMA);
    }

    /** 通过 JDBC 查询各 (day, hour) 分区行数, 观察自动分区的效果. */
    private static void printPartitionCounts(String url, String username, String password) throws Exception {
        try (Connection conn = DriverManager.getConnection(toJdbcUrl(url), username, password);
             Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT count(DISTINCT (day, hour)) AS partitions, count(*) AS total FROM " + TABLE_NAME)) {
                if (rs.next()) {
                    System.out.println("  >>> 共 " + rs.getLong(1) + " 个分区, 总计 " + rs.getLong(2) + " 行");
                }
            }
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT day, hour, count(*) AS cnt FROM " + TABLE_NAME + " GROUP BY day, hour ORDER BY day, hour")) {
                while (rs.next()) {
                    System.out.println("  分区 (day=" + rs.getString(1) + ", hour=" + rs.getString(2) + ") -> " + rs.getLong(3) + " 行");
                }
            }
        }
    }

    private static void recreateTable(String url, String username, String password) throws Exception {
        Class.forName("com.alibaba.hologres.org.postgresql.Driver");
        try (Connection conn = DriverManager.getConnection(toJdbcUrl(url), username, password);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS ga_dwd");
            stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME + " CASCADE");
            String suffix = LOGICAL_PARTITION ? ") LOGICAL PARTITION BY LIST(day, hour)" : ")";
            stmt.execute("CREATE TABLE " + TABLE_NAME + " (\n" +
                    "  player_id bigint NOT NULL,\n" +
                    "  event_time text,\n" +
                    "  ingest_time text,\n" +
                    "  log jsonb,\n" +
                    "  day text NOT NULL,\n" +
                    "  hour text NOT NULL,\n" +
                    "  PRIMARY KEY(player_id, day, hour)\n" + suffix);
            System.out.println("表 " + TABLE_NAME + " 已创建 (LOGICAL PARTITION=" + LOGICAL_PARTITION + ")");
        }
    }

    private static void dropTable(String url, String username, String password) {
        try (Connection conn = DriverManager.getConnection(toJdbcUrl(url), username, password);
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME + " CASCADE");
            System.out.println("表 " + TABLE_NAME + " 已清理");
        } catch (Exception e) {
            System.err.println("清理表失败: " + e.getMessage());
        }
    }

    private static String toJdbcUrl(String url) {
        return url.startsWith("jdbc:") ? url : "jdbc:postgresql://" + url;
    }
}
