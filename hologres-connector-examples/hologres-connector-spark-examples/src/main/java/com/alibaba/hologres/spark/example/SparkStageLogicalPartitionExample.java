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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stage导入逻辑分区表子表 - 并发写入测试.
 *
 * <p>验证两个线程同时通过stage模式写入同一逻辑分区表的不同子表(分区)时，是否存在表锁互斥。
 *
 * <p>前置DDL（会由程序自动执行）:
 * <pre>
 * CREATE TABLE IF NOT EXISTS lp_concurrent_test (
 *   pk bigint NOT NULL,
 *   id bigint,
 *   name text,
 *   ds text NOT NULL,
 *   PRIMARY KEY(pk, ds)
 * ) LOGICAL PARTITION BY LIST(ds)
 * WITH (
 *   orientation = 'column',
 *   distribution_key = 'pk'
 * );
 * </pre>
 *
 * <p>使用方式: 在 setting.properties 中配置 USERNAME, PASSWORD, JDBCURL 后直接运行main方法。
 * 程序会无限循环写入，直到手动 Ctrl+C 终止。
 */
public class SparkStageLogicalPartitionExample {

    private static final String TABLE_NAME = "lp_concurrent_test";
    // 每批写入行数，数据量大才能让commit阶段耗时更长，更容易观察到锁冲突
    private static final int BATCH_SIZE = 100000;

    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        InputStream inputStream = SparkStageLogicalPartitionExample.class.getClassLoader().getResourceAsStream("setting.properties");
        prop.load(inputStream);
        String username = prop.getProperty("USERNAME");
        String password = prop.getProperty("PASSWORD");
        String url = prop.getProperty("JDBCURL");

        // 自动建表
        createTableIfNotExists(url, username, password);

        SparkSession sparkSession =
                SparkSession.builder()
                        .appName("LogicalPartitionConcurrentWriteTest")
                        .master("local[*]")
                        .config("spark.default.parallelism", 4)
                        .getOrCreate();

        System.out.println("========== 开始并发写入逻辑分区表测试 ==========");
        System.out.println("线程1写入分区 ds='20250101'，线程2写入分区 ds='20250102'");
        System.out.println("每批 " + BATCH_SIZE + " 行，数据量大以延长commit阶段，更容易观察锁冲突");
        System.out.println("如果没有表锁，两个线程可以同时写入而不会互相阻塞。");
        System.out.println("=============================================");

        AtomicLong counter1 = new AtomicLong(0);
        AtomicLong counter2 = new AtomicLong(0);

        // 线程1：写入分区 20250101
        Thread writer1 = new Thread(() -> {
            try {
                long batchId = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    long startPk = batchId * BATCH_SIZE;
                    Dataset<Row> df = createBatchData(sparkSession, startPk, BATCH_SIZE, "20250101");
                    df.write()
                            .format("hologres")
                            .option("username", username)
                            .option("password", password)
                            .option("jdbcurl", url)
                            .option("table", TABLE_NAME)
                            .option("write.mode", "stage")
                            .option("write.on_conflict_action", "insertOrUpdate")
                            .option("write.target_partition_columns", "\"ds\"")
                            .option("write.target_partition_values", "\"20250101\"")
                            .mode(SaveMode.Append)
                            .save();
                    batchId++;
                    counter1.set(batchId);
                    System.out.println("[Writer-1 ds=20250101] 完成第 " + batchId + " 批次写入 (累计 " + (batchId * BATCH_SIZE) + " 行)");
                }
            } catch (Exception e) {
                System.err.println("[Writer-1] 异常: " + e.getMessage());
                e.printStackTrace();
            }
        }, "Writer-1-20250101");

        // 线程2：写入分区 20250102
        Thread writer2 = new Thread(() -> {
            try {
                long batchId = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    long startPk = batchId * BATCH_SIZE;
                    Dataset<Row> df = createBatchData(sparkSession, startPk, BATCH_SIZE, "20250102");
                    df.write()
                            .format("hologres")
                            .option("username", username)
                            .option("password", password)
                            .option("jdbcurl", url)
                            .option("table", TABLE_NAME)
                            .option("write.mode", "stage")
                            .option("write.on_conflict_action", "insertOrUpdate")
                            .option("write.target_partition_columns", "\"ds\"")
                            .option("write.target_partition_values", "\"20250102\"")
                            .mode(SaveMode.Append)
                            .save();
                    batchId++;
                    counter2.set(batchId);
                    System.out.println("[Writer-2 ds=20250102] 完成第 " + batchId + " 批次写入 (累计 " + (batchId * BATCH_SIZE) + " 行)");
                }
            } catch (Exception e) {
                System.err.println("[Writer-2] 异常: " + e.getMessage());
                e.printStackTrace();
            }
        }, "Writer-2-20250102");

        writer1.start();
        writer2.start();

        // 锁检查线程：在commit期间查询hg_locks
        Thread lockChecker = new Thread(() -> {
            try {
                // 等待第一批stage上传完成，commit开始
                Thread.sleep(15000);
                while (!Thread.currentThread().isInterrupted()) {
                    System.out.println("\n" + "=".repeat(60));
                    System.out.println(">>> [LockChecker] 正在查询 hg_locks 表锁信息...");
                    System.out.println("=".repeat(60));
                    queryLocks(url, username, password);
                    Thread.sleep(5000);
                }
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                System.err.println("[LockChecker] 查询锁异常: " + e.getMessage());
                e.printStackTrace();
            }
        }, "LockChecker");
        lockChecker.setDaemon(true);
        lockChecker.start();

        // 监控线程，定期打印进度
        Thread monitor = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Thread.sleep(10000);
                    System.out.println("\n>>> [Monitor] Writer-1 已完成 " + counter1.get() + " 批次, Writer-2 已完成 " + counter2.get() + " 批次 <<<\n");
                }
            } catch (InterruptedException ignored) {
            }
        }, "Monitor");
        monitor.setDaemon(true);
        monitor.start();

        // 永远等待，直到用户手动中断
        writer1.join();
        writer2.join();

        sparkSession.stop();
    }

    private static Dataset<Row> createBatchData(SparkSession spark, long startPk, int count, String partitionValue) {
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("pk", DataTypes.LongType, false),
                DataTypes.createStructField("id", DataTypes.LongType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("ds", DataTypes.StringType, false)
        });

        List<Row> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            rows.add(RowFactory.create(
                    startPk + i,
                    (long) (Math.random() * 100000),
                    "data_" + partitionValue + "_" + (startPk + i),
                    partitionValue
            ));
        }
        return spark.createDataFrame(rows, schema);
    }

    private static void queryLocks(String url, String username, String password) {
        String jdbcUrl = url;
        if (!jdbcUrl.startsWith("jdbc:")) {
            jdbcUrl = "jdbc:postgresql://" + jdbcUrl;
        }
        String sql = "WITH target_table AS (" +
                "  SELECT property_value::BIGINT AS table_id" +
                "  FROM hologres.hg_table_properties" +
                "  WHERE table_name = '" + TABLE_NAME + "'" +
                "    AND property_key = 'table_id'" +
                "), target_txn AS (" +
                "  SELECT DISTINCT transaction_id" +
                "  FROM hologres.hg_locks l" +
                "  JOIN target_table t ON l.lock_id = t.table_id" +
                "  WHERE l.lock_type = 'Table'" +
                ") SELECT lock_type, lock_id, lock_desc, lock_mode" +
                "  FROM hologres.hg_locks" +
                "  WHERE transaction_id IN (SELECT transaction_id FROM target_txn)";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            boolean hasLock = false;
            while (rs.next()) {
                if (!hasLock) {
                    System.out.println(String.format("%-12s %-15s %-40s %-20s", "lock_type", "lock_id", "lock_desc", "lock_mode"));
                    System.out.println("-".repeat(90));
                    hasLock = true;
                }
                System.out.println(String.format("%-12s %-15s %-40s %-20s",
                        rs.getString("lock_type"),
                        rs.getString("lock_id"),
                        rs.getString("lock_desc"),
                        rs.getString("lock_mode")));
            }
            if (!hasLock) {
                System.out.println(">>> [LockChecker] 未发现 " + TABLE_NAME + " 相关的表锁");
            }
            System.out.println();
        } catch (Exception e) {
            System.err.println("[LockChecker] SQL执行失败: " + e.getMessage());
        }
    }

    private static void createTableIfNotExists(String url, String username, String password) throws Exception {
        Class.forName("org.postgresql.Driver");
        String jdbcUrl = url;
        if (!jdbcUrl.startsWith("jdbc:")) {
            jdbcUrl = "jdbc:postgresql://" + jdbcUrl;
        }
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (\n" +
                    "  pk bigint NOT NULL,\n" +
                    "  id bigint,\n" +
                    "  name text,\n" +
                    "  ds text NOT NULL,\n" +
                    "  PRIMARY KEY(pk, ds)\n" +
                    ") LOGICAL PARTITION BY LIST(ds)\n" +
                    "WITH (\n" +
                    "  orientation = 'column',\n" +
                    "  distribution_key = 'pk'\n" +
                    ")");
            System.out.println("表 " + TABLE_NAME + " 已就绪");
        }
    }
}
