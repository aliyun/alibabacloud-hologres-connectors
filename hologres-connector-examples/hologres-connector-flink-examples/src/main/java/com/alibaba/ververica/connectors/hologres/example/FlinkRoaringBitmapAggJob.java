package com.alibaba.ververica.connectors.hologres.example;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Properties;

import static org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.TIMESTAMP;
import static org.apache.flink.table.api.Expressions.$;

/**
 * UV deduplication statistics based on RoaringBitmap.
 */
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
        int windowSize = Integer.parseInt(props.getProperty("windowSize"));

        // flink env设置
        EnvironmentSettings.Builder settingsBuilder =
                EnvironmentSettings.newInstance().inStreamingMode();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv =
                StreamTableEnvironment.create(env, settingsBuilder.build());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // fieldsMask 为感兴趣的fields序号,fields类型在如下TupleTypeInfo中设置，其名称如注释所示
        // 此处使用csv文件作为数据源，也可以是kafka等；csv文件参考src/main/resources/ods_app_example.csv
        int[] fieldsMask = {0, 1, 2, 3, 8, 12};
        TupleTypeInfo typeInfo =
                new TupleTypeInfo<>(
                        Types.STRING, // uid
                        Types.STRING, // country
                        Types.STRING, // prov
                        Types.STRING, // city
                        TIMESTAMP,    // click_time
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
                        $("click_time"),
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
                "SELECT ods.country, ods.prov, ods.city, ods.click_time, ods.ymd, dim.uid_int32"
                        + "  FROM odsTable AS ods JOIN uid_mapping_dim FOR SYSTEM_TIME AS OF ods.proctime AS dim"
                        + "  ON ods.uid = dim.uid";
        Table joinRes = tableEnv.sqlQuery(odsJoinDim);

        // 将join的结果转换为DataStream进行计算
        DataStream<Row> source =
                tableEnv.toDataStream(joinRes);

        // 使用RoaringBitmap进行去重处理
        SingleOutputStreamOperator<Row> processedSource = source
                // 数据源中的click_time字段当做EventTime,注册watermarks.
                // 实际使用中,event_time期望是基本有序的,比如上游数据源是kafka或者hologres binlog表,否则建议使用ProcessingTimeWindow
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Row element) {
                        // 将LocalDateTime转换为毫秒时间戳
                        LocalDateTime localDateTime = LocalDateTime.parse(element.getField(3).toString());
                        return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                    }
                })
                // 筛选需要统计的维度（country, prov, city, ymd）
                .keyBy(new KeySelector<Row, Row>() {
                    @Override
                    public Row getKey(Row row) throws Exception {
                        return Row.of(row.getField(0), row.getField(1), row.getField(2), row.getField(4));
                    }
                })
                // 滚动事件时间窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                // 触发器，可以在窗口未结束时获取聚合结果
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(Math.max(1, windowSize / 20))))
                // 允许延迟半个窗口的时间
                .allowedLateness(Time.seconds(Math.max(1, windowSize / 2)))
                // 窗口聚合函数，根据keyBy筛选的维度，进行聚合
                .aggregate(
                        new AggregateFunction<
                                Row,
                                RoaringBitmap,
                                RoaringBitmap>() {

                            @Override
                            public RoaringBitmap createAccumulator() {
                                return new RoaringBitmap();
                            }

                            @Override
                            public RoaringBitmap add(
                                    Row value,
                                    RoaringBitmap acc) {
                                // 将32位的uid添加到RoaringBitmap进行去重
                                acc.add((Integer) value.getField(5));
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
                                Row,
                                Row,
                                TimeWindow>() {

                            @Override
                            public void apply(
                                    Row keys,
                                    TimeWindow timeWindow,
                                    Iterable<RoaringBitmap> iterable,
                                    Collector<Row> out)
                                    throws Exception {

                                RoaringBitmap result = iterable.iterator().next();

                                // 优化RoaringBitmap
                                result.runOptimize();
                                // 将RoaringBitmap转化为字节数组以存入Holo中
                                byte[] byteArray = new byte[result.serializedSizeInBytes()];
                                result.serialize(ByteBuffer.wrap(byteArray));

                                // 其中 Timestamp字段表示以窗口长度为周期进行统计
                                out.collect(
                                        Row.of(
                                                keys.getField(0),
                                                keys.getField(1),
                                                keys.getField(2),
                                                keys.getField(3),
                                                new Timestamp(
                                                        timeWindow.getEnd() / 1000 * 1000),
                                                byteArray));
                            }
                        }).returns(new RowTypeInfo(Types.STRING, Types.STRING, Types.STRING, Types.STRING, TIMESTAMP, Types.PRIMITIVE_ARRAY(Types.BYTE)));


        // 计算结果转换为表
        Table resTable =
                tableEnv.fromDataStream(
                        processedSource,
                        $("country"),
                        $("prov"),
                        $("city"),
                        $("ymd"),
                        $("event_window_time"),
                        $("uid32_bitmap"));

        // 创建holo结果表, 其中holo的roaringbitmap类型通过byte数组存入
        String createHologresTable =
                String.format(
                        "create table sink("
                                + "  country string,"
                                + "  prov string,"
                                + "  city string,"
                                + "  ymd string,"
                                + "  event_window_time timestamp,"
                                + "  uid32_bitmap BYTES"
                                + ") with ("
                                + "  'connector'='hologres',"
                                + "  'dbname' = '%s',"
                                + "  'tablename' = '%s',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s',"
                                + "  'endpoint' = '%s',"
                                + "  'mutatetype' = 'insertOrReplace'"
                                + ")",
                        database, dwsTableName, username, password, endpoint);
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
