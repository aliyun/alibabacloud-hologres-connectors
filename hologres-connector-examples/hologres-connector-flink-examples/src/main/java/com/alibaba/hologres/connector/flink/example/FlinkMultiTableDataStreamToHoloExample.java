package com.alibaba.hologres.connector.flink.example;

import com.alibaba.hologres.connector.flink.config.HologresConfigs;
import com.alibaba.hologres.connector.flink.config.WriteMode;
import com.alibaba.hologres.connector.flink.sink.v1.multi.HoloRecordConverter;
import com.alibaba.hologres.connector.flink.sink.v1.multi.HologresMultiTableSinkFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * A Flink data stream example sink multi table to Hologres.
 */
public class FlinkMultiTableDataStreamToHoloExample {

    public static void main(String[] args) throws Exception {
        SettingHelper settingHelper = new SettingHelper(args, "setting.properties");
        String endpoint = settingHelper.getEndpoint();
        String username = settingHelper.getUsername();
        String password = settingHelper.getPassword();
        String database = settingHelper.getDatabase();

        Configuration envConf = new Configuration();
        envConf.setString(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // hologres中已经存在以下表:
        // create table users(id bigint primary key, name text);
        // create table items(id bigint primary key, name text, price decimal(38,2));

        // Json数据
        DataStreamSource<String> source = env.fromData(
                "{\"tablename\":\"users\",\"id\":1,\"name\":\"user1\"},",
                "{\"tablename\":\"items\",\"id\":1,\"name\":\"item1\",\"price\":10.0},",
                "{\"tablename\":\"users\",\"id\":2,\"name\":\"user2\"},",
                "{\"tablename\":\"items\",\"id\":2,\"name\":\"item2\",\"price\":100.99},",
                "{\"tablename\":\"items\",\"id\":3,\"name\":\"item2\",\"extra_column\":100.99},"
        );

        // 设置hologres连接配置
        Configuration configuration = new Configuration();
        configuration.set(HologresConfigs.ENDPOINT, endpoint);
        configuration.set(HologresConfigs.DATABASE, database);
        configuration.set(HologresConfigs.USERNAME, username);
        configuration.set(HologresConfigs.PASSWORD, password);
        // 使用INSERT写入
        configuration.set(HologresConfigs.WRITE_MODE, WriteMode.INSERT);
        // 设置共享连接池的名称以及大小, 所有表共享连接池
        configuration.set(HologresConfigs.CONNECTION_POOL_SIZE, 10);
        configuration.set(HologresConfigs.WRITE_BATCH_SIZE, 2048);
        configuration.set(HologresConfigs.WRITE_BATCH_BYTE_SIZE, 20971520L);
        configuration.set(HologresConfigs.CONNECTION_POOL_NAME, "common-pool");
        // 当json中存在holo表不存在的字段时,配置此参数忽略,否则抛出异常
        configuration.set(HologresConfigs.ENABLE_MULTI_TABLE_EXTRA_COLUMN_TOLERANT, true);

        // 创建sink 仅需要config以及RecordConverter实现
        source.addSink(new HologresMultiTableSinkFunction<>(configuration, new RecordConverter()));

        env.execute("Insert");
    }

    /**
     * RecordConverter
     * 将用户数据转换为Map<String, Object>对象
     * 每个Map必须包含表名key: tablename， value为写入holo的表名；如有schema通过.隔开, 如"public.users"
     * 其他对象的key为字段名, value为字段值; value的类型需要与hologres表字段类型一致, 尽可能使用java的基本类型和java.sql类型
     */
    public static class RecordConverter implements HoloRecordConverter<String> {
        public Map<String, Object> convert(String record) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
            Map<String, Object> resultMap;
            try {
                resultMap = objectMapper.readValue(record, Map.class);
            } catch (JsonProcessingException e) {
                System.out.println("parse json error: " + record);
                throw new RuntimeException(e);
            }
            return resultMap;
        }
    }
}
