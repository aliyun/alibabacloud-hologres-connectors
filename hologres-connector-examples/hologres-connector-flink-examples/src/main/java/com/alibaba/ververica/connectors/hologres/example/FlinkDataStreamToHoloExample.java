package com.alibaba.ververica.connectors.hologres.example;

import com.alibaba.hologres.client.Put.MutationType;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.ververica.connectors.common.sink.OutputFormatSinkFunction;
import com.alibaba.ververica.connectors.hologres.api.HologresRecordConverter;
import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.config.HologresConfigs;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.example.SourceItem.EventType;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCWriter;
import com.alibaba.ververica.connectors.hologres.sink.HologresOutputFormat;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

import java.math.BigDecimal;
import java.sql.Timestamp;

/** A Flink data streak example sinking data to Hologres. */
public class FlinkDataStreamToHoloExample {
    /**
     * Hologres DDL. create table sink_table(user_id bigint, user_name text, price decimal(38,
     * 2),sale_timestamp timestamptz);
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("e", "endpoint", true, "Hologres endpoint");
        options.addOption("u", "username", true, "Username");
        options.addOption("p", "password", true, "Password");
        options.addOption("d", "database", true, "Database");
        options.addOption("t", "tablename", true, "Table name");

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);
        String endPoint = commandLine.getOptionValue("endpoint");
        String userName = commandLine.getOptionValue("username");
        String password = commandLine.getOptionValue("password");
        String database = commandLine.getOptionValue("database");
        String tableName = commandLine.getOptionValue("tablename");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<SourceItem> source =
                env.fromElements(
                        new SourceItem(
                                123L,
                                "Adam",
                                new BigDecimal("123.11"),
                                new Timestamp(System.currentTimeMillis())),
                        new SourceItem(
                                234,
                                "Bob",
                                new BigDecimal("000.11"),
                                new Timestamp(System.currentTimeMillis())));

        TableSchema schema =
                TableSchema.builder()
                        .field("user_id", DataTypes.BIGINT())
                        .field("user_name", DataTypes.STRING())
                        .field("price", DataTypes.DECIMAL(38, 2))
                        .field("sale_timestamp", Types.SQL_TIMESTAMP)
                        .build();

        Configuration configuration = new Configuration();
        configuration.set(HologresConfigs.ENDPOINT, endPoint);
        configuration.set(HologresConfigs.DATABASE, database);
        configuration.set(HologresConfigs.USERNAME, userName);
        configuration.set(HologresConfigs.PASSWORD, password);
        configuration.set(HologresConfigs.TABLE, tableName);
        HologresConnectionParam hologresConnectionParam =
                new HologresConnectionParam(configuration);

        source.addSink(
                new OutputFormatSinkFunction<SourceItem>(
                        new HologresOutputFormat<>(
                                hologresConnectionParam,
                                new HologresJDBCWriter<>(
                                        hologresConnectionParam,
                                        schema,
                                        new RecordConverter(hologresConnectionParam)))));

        env.execute("Insert");
    }

    /** 将用户POJO数据转换至Hologres Record的实现. */
    public static class RecordConverter implements HologresRecordConverter<SourceItem, Record> {
        private HologresConnectionParam hologresConnectionParam;
        private HologresTableSchema tableSchema;

        public RecordConverter(HologresConnectionParam hologresConnectionParam) {
            this.hologresConnectionParam = hologresConnectionParam;
        }

        @Override
        public Record convertFrom(SourceItem record) {
            if (tableSchema == null) {
                this.tableSchema =
                        HologresTableSchema.get(hologresConnectionParam.getJdbcOptions());
            }
            Record result = new Record(tableSchema.get());
            result.setObject(0, record.userId);
            result.setObject(1, record.userName);
            result.setObject(2, record.price);
            result.setObject(3, record.saleTimestamp);
            /* 在DataStream作业中，用户需要使用自定义的OutputFormatSinkFunction以及RecordConverter，如果要支持消息的回撤，需要在此处对convert结果设置MutationType。 需要hologres-connector 1.3.2及以上版本 */
             if (record.eventType == EventType.DELETE) {
                 result.setType(MutationType.DELETE);
             }
            return result;
        }

        @Override
        public SourceItem convertTo(Record record) {
            throw new UnsupportedOperationException("No need to implement");
        }

        @Override
        public Record convertToPrimaryKey(SourceItem record) {
            throw new UnsupportedOperationException("No need to implement");
        }
    }
}
