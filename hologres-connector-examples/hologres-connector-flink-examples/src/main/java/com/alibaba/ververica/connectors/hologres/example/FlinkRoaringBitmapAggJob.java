package com.alibaba.ververica.connectors.hologres.example;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

/** UV deduplication statistics based on RoaringBitmap. */
public class FlinkRoaringBitmapAggJob {
    public static void main(String[] args) throws Exception {
        // 从执行命令中获取配置文件及数据源文件路径
        Options options = new Options();
        options.addOption("p", "propsFilePath", true, "Properties file path");
        options.addOption("s", "sourceFilePath", true, "DataSource file path");
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);
        String propsPath = commandLine.getOptionValue("propsFilePath");
        String filePath = commandLine.getOptionValue("sourceFilePath");

        // 从配置文件中读取参数
        Properties props = new Properties();
        props.load(new java.io.FileInputStream(propsPath));

        // 参数，包括维表dim与结果表dws的名称
        String endpoint = props.getProperty("endpoint");
        String username = props.getProperty("username");
        String password = props.getProperty("password");
        String database = props.getProperty("database");
        String dimTableName = props.getProperty("dimTableName");
        String dwsTableName = props.getProperty("dwsTableName");
        String connectionSize = props.getProperty("connectionSize");

        // flink env设置
        EnvironmentSettings.Builder settingsBuilder =
                EnvironmentSettings.newInstance().inStreamingMode();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv =
                StreamTableEnvironment.create(env, settingsBuilder.build());
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // fieldsMask 为感兴趣的fields序号,fields类型在如下TupleTypeInfo中设置，其名称如注释所示
        // 此处使用csv文件作为数据源，也可以是kafka等；csv文件参考src/main/resources/ods_app_example.csv
        int[] fieldsMask = {0, 1, 2, 3, 12};
        TupleTypeInfo typeInfo =
                new TupleTypeInfo<>(
                        Types.STRING, // uid
                        Types.STRING, // country
                        Types.STRING, // prov
                        Types.STRING, // city
                        Types.STRING // ymd
                        );
        TupleCsvInputFormat csvInput = csvInputFormatBuilder(filePath, fieldsMask, typeInfo);
        // createInput函数需要typeInfo设置输出格式
        DataStreamSource odsStream = env.createInput(csvInput, typeInfo);
        // 将DataStream转化为table,
        // 与维表join需要proctime，详见https://help.aliyun.com/document_detail/62506.html
        Table odsTable =
                tableEnv.fromDataStream(
                        odsStream,
                        $("uid"),
                        $("country"),
                        $("prov"),
                        $("city"),
                        $("ymd"),
                        $("proctime").proctime());
        // 注册到catalog环境
        tableEnv.createTemporaryView("odsTable", odsTable);

        // 创建hologres维表，其中insertifnotexists表示查询不到则自行插入
        String createUidMappingTable =
                String.format(
                        "create table uid_mapping_dim("
                                + "  uid string,"
                                + "  uid_int32 INT"
                                + ") with ("
                                + "  'connector'='hologres',"
                                + "  'dbname' = '%s',"
                                + "  'tablename' = '%s',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s',"
                                + "  'endpoint' = '%s',"
                                + "  'insertifnotexists'='true'"
                                + ")",
                        database, dimTableName, username, password, endpoint);
        tableEnv.executeSql(createUidMappingTable);

        // 源表与维表join
        String odsJoinDim =
                "SELECT ods.country, ods.prov, ods.city, ods.ymd, dim.uid_int32"
                        + "  FROM odsTable AS ods JOIN uid_mapping_dim FOR SYSTEM_TIME AS OF ods.proctime AS dim"
                        + "  ON ods.uid = dim.uid";
        Table joinRes = tableEnv.sqlQuery(odsJoinDim);

        // 将join的结果转换为DataStream进行计算
        DataStream<Tuple5<String, String, String, String, Integer>> source =
                tableEnv.toAppendStream(
                        joinRes,
                        new TupleTypeInfo<>(
                                Types.STRING, // country
                                Types.STRING, // prov
                                Types.STRING, // city
                                Types.STRING, // ymd
                                Types.INT // uid_int32
                                ));

        // 使用RoaringBitmap进行去重处理
        DataStream<Tuple6<String, String, String, String, Timestamp, byte[]>> processedSource =
                source
                        // 筛选需要统计的维度（country, prov, city, ymd）
                        .keyBy(0, 1, 2, 3)
                        // 滚动时间窗口；此处由于使用读取csv模拟输入流，采用ProcessingTime，实际使用中可使用EventTime
                        .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
                        // 触发器，可以在窗口未结束时获取聚合结果
                        .trigger(ContinuousProcessingTimeTrigger.of(Time.minutes(1)))
                        // 窗口聚合函数，根据keyBy筛选的维度，进行聚合
                        .aggregate(
                                new AggregateFunction<
                                        Tuple5<String, String, String, String, Integer>,
                                        RoaringBitmap,
                                        RoaringBitmap>() {

                                    @Override
                                    public RoaringBitmap createAccumulator() {
                                        return new RoaringBitmap();
                                    }

                                    @Override
                                    public RoaringBitmap add(
                                            Tuple5<String, String, String, String, Integer> in,
                                            RoaringBitmap acc) {
                                        // 将32位的uid添加到RoaringBitmap进行去重
                                        acc.add(in.f4);
                                        return acc;
                                    }

                                    @Override
                                    public RoaringBitmap getResult(RoaringBitmap acc) {
                                        return acc;
                                    }

                                    @Override
                                    public RoaringBitmap merge(
                                            RoaringBitmap acc1, RoaringBitmap acc2) {
                                        return RoaringBitmap.or(acc1, acc2);
                                    }
                                },
                                new WindowFunction<
                                        RoaringBitmap,
                                        Tuple6<String, String, String, String, Timestamp, byte[]>,
                                        Tuple,
                                        TimeWindow>() {
                                    @Override
                                    public void apply(
                                            Tuple keys,
                                            TimeWindow timeWindow,
                                            Iterable<RoaringBitmap> iterable,
                                            Collector<
                                                            Tuple6<
                                                                    String,
                                                                    String,
                                                                    String,
                                                                    String,
                                                                    Timestamp,
                                                                    byte[]>>
                                                    out)
                                            throws Exception {

                                        RoaringBitmap result = iterable.iterator().next();

                                        // 优化RoaringBitmap
                                        result.runOptimize();
                                        // 将RoaringBitmap转化为字节数组以存入Holo中
                                        byte[] byteArray = new byte[result.serializedSizeInBytes()];
                                        result.serialize(ByteBuffer.wrap(byteArray));

                                        // 其中 Tuple6.f4(Timestamp) 字段表示以窗口长度为周期进行统计，以秒为单位
                                        out.collect(
                                                new Tuple6<>(
                                                        keys.getField(0),
                                                        keys.getField(1),
                                                        keys.getField(2),
                                                        keys.getField(3),
                                                        new Timestamp(
                                                                timeWindow.getEnd() / 1000 * 1000),
                                                        byteArray));
                                    }
                                });

        // 计算结果转换为表
        Table resTable =
                tableEnv.fromDataStream(
                        processedSource,
                        $("country"),
                        $("prov"),
                        $("city"),
                        $("ymd"),
                        $("timest"),
                        $("uid32_bitmap"));

        // 创建holo结果表, 其中holo的roaringbitmap类型通过byte数组存入
        String createHologresTable =
                String.format(
                        "create table sink("
                                + "  country string,"
                                + "  prov string,"
                                + "  city string,"
                                + "  ymd string,"
                                + "  timetz timestamp,"
                                + "  uid32_bitmap BYTES"
                                + ") with ("
                                + "  'connector'='hologres',"
                                + "  'dbname' = '%s',"
                                + "  'tablename' = '%s',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s',"
                                + "  'endpoint' = '%s',"
                                + "  'connectionSize' = '%s',"
                                + "  'mutatetype' = 'insertOrReplace'"
                                + ")",
                        database, dwsTableName, username, password, endpoint, connectionSize);
        tableEnv.executeSql(createHologresTable);

        // 写入计算结果到dws表
        tableEnv.executeSql("insert into sink select * from " + resTable);
    }

    public static TupleCsvInputFormat csvInputFormatBuilder(
            String filePath, int[] fieldsMask, TupleTypeInfo typeInfo) {
        TupleCsvInputFormat csvInput =
                new TupleCsvInputFormat<>(new Path(filePath), "\n", ",", typeInfo, fieldsMask);

        // 跳过第一行 表头
        csvInput.setSkipFirstLineAsHeader(true);
        return csvInput;
    }
}
