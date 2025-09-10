package com.alibaba.hologres.connector.flink.example;

import com.alibaba.hologres.client.Command;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.connector.flink.api.HologresTableSchema;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.jdbc.HologresJDBCClientProvider;
import com.alibaba.hologres.org.postgresql.jdbc.TimestampUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.api.common.functions.Partitioner;
import com.alibaba.hologres.client.impl.util.ShardUtil;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Flink data stream demo sink data to hologres and do custom partition.
 */
public class FlinkToHoloRePartitionExample {
    private static final transient Logger LOG = LoggerFactory.getLogger(FlinkToHoloRePartitionExample.class);

    public static void main(String[] args) throws Exception {
        String sourceDDL;
        String sourceDql;
        String sinkDDL;

        Options options = new Options();
        options.addOption("s", "sourceDDL", true, "sourceDDL");
        options.addOption("q", "sourceDql", true, "sourceDql");
        options.addOption("h", "sinkDDL", true, "sinkDDL");
        options.addOption("p", "sqlFilePath", true, "SqlFilePath");

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);
        String sqlFilePath = commandLine.getOptionValue("sqlFilePath");
        if (sqlFilePath != null) {
            List<String> statements = loadSql(sqlFilePath);
            if (statements.size() != 3) {
                throw new IllegalArgumentException("sql file should contain 3(sourceDDL, sourceDQL, sinkDDL) statements.");
            }
            sourceDDL = statements.get(0);
            sourceDql = statements.get(1);
            sinkDDL = statements.get(2);
        } else {
            sourceDDL = commandLine.getOptionValue("sourceDDL");
            sourceDql = commandLine.getOptionValue("sourceDql");
            sinkDDL = commandLine.getOptionValue("sinkDDL");
            if (sourceDDL == null || sourceDql == null || sinkDDL == null) {
                throw new IllegalArgumentException("params sourceDDL, sourceDQL, sinkDDL should not be null.");
            }
        }

        EnvironmentSettings.Builder streamBuilder = EnvironmentSettings.newInstance().inStreamingMode();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, streamBuilder.build());

        // 通过DDL创建源表和结果表, 返回结果表的schema和with参数
        Tuple3<String, TableSchema, Configuration> sinkTableInfo = createTable(tableEnv, sourceDDL, sinkDDL);
        LOG.info("hologres sink table name: {}", sinkTableInfo.f0);
        LOG.info("hologres sink table schema: {}", sinkTableInfo.f1);
        LOG.info("hologres sink table options: {}", sinkTableInfo.f2);

        Table resultTable = tableEnv.sqlQuery(sourceDql);
        DataStream<Row> sourceStream = tableEnv.toDataStream(resultTable);
        TableSchema sourceSchema = resultTable.getSchema();
        LOG.info("derived source table from DQL schema: {}", sourceSchema);
        // schema check：检查字段数量是否相等
        if (sinkTableInfo.f1.getFieldCount() != sourceSchema.getFieldCount()) {
            throw new IllegalArgumentException("source schema field count not equal to sink schema field count");
        }

        DataStream<Row> rePartitionStream = repartitonByHoloDistributionKey(sourceStream, sinkTableInfo.f1, sinkTableInfo.f2);

        // 计算结果转换为表
        Table rePartitionTable = tableEnv.fromDataStream(rePartitionStream);
        // 写入计算结果
        tableEnv.executeSql(String.format("insert into %s select * from %s", sinkTableInfo.f0, rePartitionTable));
    }

    public static DataStream<Row> repartitonByHoloDistributionKey(DataStream<Row> sourceStream, TableSchema sinkSchema, Configuration configuration) throws HoloClientException {
        Map<String, Integer> columnNameToIndexmap = new HashMap<>();
        for (int i = 0; i < sinkSchema.getFieldCount(); ++i) {
            columnNameToIndexmap.put(sinkSchema.getFieldNames()[i], i);
        }

        // 获取holo物理表schema
        HologresConnectionParam connectionParam = new HologresConnectionParam(configuration);
        HologresTableSchema holoSchema = HologresTableSchema.get(connectionParam.getJdbcOptions());
        HologresJDBCClientProvider clientProvider = new HologresJDBCClientProvider(connectionParam);
        int shardCount = Command.getShardCount(clientProvider.getClient(), holoSchema.get());

        // sinkSchema可能只包含部分holo物理表的字段，或者与holo物理表的字段顺序不一致，这里拿到distribution key字段在flink侧sinkSchema中的index
        int[] keyIndexInHolo = holoSchema.get().getDistributionKeyIndex();
        int[] keyIndexInRow = new int[keyIndexInHolo.length];
        for (int i = 0; i < keyIndexInHolo.length; i++) {
            String columnName = holoSchema.get().getColumn(keyIndexInHolo[i]).getName();
            if (!columnNameToIndexmap.containsKey(columnName)) {
                throw new IllegalArgumentException(String.format("repartition sink need write all distribution keys %s.", Arrays.toString(holoSchema.get().getDistributionKeys())));
            }
            keyIndexInRow[i] = columnNameToIndexmap.get(columnName);
        }

        return sourceStream
                // 根据distribution key字段信息,计算Flink Row写入holo时所处的shard,并按照作业并发进行re partition
                .partitionCustom(new HoloShardPartitioner(), new HoloKeySelector(shardCount, keyIndexInRow));
    }

    public static class HoloKeySelector implements KeySelector<Row, Integer> {
        private final ConcurrentSkipListMap<Integer, Integer> splitRange = new ConcurrentSkipListMap<>();
        private final int[] keyIndex;

        public HoloKeySelector(int shardCount, int[] keyIndex) {
            int[][] range = RowShardUtil.split(shardCount);
            for (int i = 0; i < shardCount; ++i) {
                splitRange.put(range[i][0], i);
            }
            this.keyIndex = keyIndex;
        }

        /**
         * 根据Row中的distribution key字段的信息，计算写入holo时所处的shard
         */
        @Override
        public Integer getKey(Row value) throws Exception {
            int raw = RowShardUtil.hash(value, keyIndex);
            int hash = Integer.remainderUnsigned(raw, RowShardUtil.RANGE_END);
            return splitRange.floorEntry(hash).getValue();
        }
    }

    /**
     * 根据HoloKeySelector计算得到的数据 shard id 以及当前作业并发 numPartitions, 将数据分布在不同的task上,
     * 保证一个task仅写某一个或某几个shard的数据.
     */
    public static class HoloShardPartitioner implements Partitioner<Integer> {
        @Override
        public int partition(Integer key, int numPartitions) {
            return key % numPartitions;
        }
    }


    private static Tuple3<String, TableSchema, Configuration> createTable(TableEnvironment tableEnv, String sourceDDL, String sinkDDL) {

        // 创建hologres sink 表, 并返回参数
        tableEnv.executeSql(sinkDDL);
        String sinkTableName;
        TableSchema sinkSchema;
        Configuration configuration = new Configuration();

        try {
            String currentCatalogName = tableEnv.getCurrentCatalog();
            String currentDatabaseName = tableEnv.getCurrentDatabase();

            // 从当前 Catalog 和数据库中获取 CatalogTable 对象, 目前应该仅有结果表一张表
            Optional<Catalog> currentCatalog = tableEnv.getCatalog(currentCatalogName);
            if (currentCatalog.isPresent()) {
                Catalog catalog = currentCatalog.get();

                List<String> tables = catalog.listTables(currentDatabaseName);
                if (tables.isEmpty()) {
                    throw new RuntimeException("sink table create failed." + currentCatalogName);
                }
                if (tables.size() > 1) {
                    throw new RuntimeException(String.format("don't know which is the special sink table: %s.", tables));
                }

                // 获取 sink CatalogTable
                sinkTableName = tables.get(0);
                ObjectPath tablePath = new ObjectPath(currentDatabaseName, sinkTableName);
                CatalogBaseTable catalogTable = catalog.getTable(tablePath);

                // 结果表的schema
                sinkSchema = catalogTable.getSchema();
                // 结果表的WITH参数
                Map<String, String> options = catalogTable.getOptions();
                for (Map.Entry<String, String> entry : options.entrySet()) {
                    configuration.setString(entry.getKey().toLowerCase(), entry.getValue());
                }
            } else {
                throw new RuntimeException("default catalog not found: " + currentCatalogName);
            }
        } catch (TableNotExistException | DatabaseNotExistException e) {
            throw new RuntimeException(e);
        }

        // 创建源表
        tableEnv.executeSql(sourceDDL);

        return new Tuple3<>(sinkTableName, sinkSchema, configuration);
    }

    public static class RowShardUtil extends ShardUtil {
        // hologres仅支持int等整数类型,字符串类型,Date类型做distribution key,大多数类型toString的字面值与holo是一致的,因此不存在计算错误的情况
        // 但date类型在holo中存储为int,如果遇到LocalDate类型（我们使用的Row是tableEnv.toDataStream转过来的，所以类型比较确定）,则转换为int计算
        // 类似的,目前timestamptz类型也可以当作distribution key(timestamp类型不可以),因此计算shard使用其对应的long值.
        public static int hash(Row row, int[] indexes) {
            int hash = 0;
            boolean first = true;
            if (indexes == null || indexes.length == 0) {
                ThreadLocalRandom rand = ThreadLocalRandom.current();
                hash = rand.nextInt();
            } else {
                for (int i : indexes) {
                    Object o = row.getField(i);
                    if (o instanceof LocalDate) {
                        o = ((LocalDate) o).toEpochDay();
                    } else if (o instanceof LocalDateTime) {
                        o = TimestampUtil.timestampToMillisecond(o, "timestamptz");
                    }
                    if (first) {
                        hash = ShardUtil.hash(o);
                    } else {
                        hash ^= ShardUtil.hash(o);
                    }
                    first = false;
                }
            }
            return hash;
        }
    }

    public static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    protected static List<String> loadSql(String sqlFilePath)
            throws Exception {
        final List<String> statements =
                Arrays.stream(
                                Files.readAllLines(Paths.get(sqlFilePath)).stream()
                                        .map(String::trim)
                                        .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                        .map(
                                                x -> {
                                                    final Matcher m = COMMENT_PATTERN.matcher(x);
                                                    return m.matches() ? m.group(1) : x;
                                                })
                                        .collect(Collectors.joining("\n"))
                                        .split(";"))
                        .collect(Collectors.toList());
        return statements;
    }
}