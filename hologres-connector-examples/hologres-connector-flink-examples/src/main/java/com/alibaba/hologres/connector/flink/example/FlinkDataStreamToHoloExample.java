package com.alibaba.hologres.connector.flink.example;

import com.alibaba.hologres.connector.flink.api.HologresTableSchema;
import com.alibaba.hologres.connector.flink.config.HologresConfigs;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.jdbc.HologresJDBCWriter;
import com.alibaba.hologres.connector.flink.sink.v1.HologresSinkFunction;
import com.alibaba.hologres.connector.flink.utils.SchemaUtil;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.*;

import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * A Flink data streak example sinking data to Hologres.
 */
public class FlinkDataStreamToHoloExample {
    /**
     * Hologres DDL. create table sink_table(user_id bigint, user_name text, price decimal(38,
     * 2),sale_timestamp timestamptz);
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        SettingHelper settingHelper = new SettingHelper(args, "setting.properties");
        String endpoint = settingHelper.getEndpoint();
        String username = settingHelper.getUsername();
        String password = settingHelper.getPassword();
        String database = settingHelper.getDatabase();
        String tableName = "sink_table";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<RowData> source =
                env.fromElements(
                        GenericRowData.of(
                                123L,
                                StringData.fromString("Adam"),
                                DecimalData.fromBigDecimal(new BigDecimal("123.11"), 38, 2),
                                TimestampData.fromTimestamp(new Timestamp(System.currentTimeMillis()))),
                        GenericRowData.of(
                                234L,
                                StringData.fromString("Bob"),
                                DecimalData.fromBigDecimal(new BigDecimal("000.11"), 38, 2),
                                TimestampData.fromTimestamp(new Timestamp(System.currentTimeMillis())))
                );

        TableSchema schema =
                TableSchema.builder()
                        .field("user_id", DataTypes.BIGINT())
                        .field("user_name", DataTypes.STRING())
                        .field("price", DataTypes.DECIMAL(38, 2))
                        .field("sale_timestamp", Types.SQL_TIMESTAMP)
                        .build();

        Configuration configuration = new Configuration();
        configuration.set(HologresConfigs.ENDPOINT, endpoint);
        configuration.set(HologresConfigs.DATABASE, database);
        configuration.set(HologresConfigs.USERNAME, username);
        configuration.set(HologresConfigs.PASSWORD, password);
        configuration.set(HologresConfigs.TABLE, tableName);
        HologresConnectionParam hologresConnectionParam =
                new HologresConnectionParam(configuration);

        source.addSink(new HologresSinkFunction(hologresConnectionParam,
                HologresJDBCWriter.createTableWriter(
                        hologresConnectionParam,
                        schema.getFieldNames(),
                        SchemaUtil.getLogicalTypes(schema),
                        HologresTableSchema.get(hologresConnectionParam.getJdbcOptions()))));

        env.execute("Insert");
    }
}
