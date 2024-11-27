package io.hologres.flink.ordergen;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

import java.util.Collections;
import java.util.Set;

/**
 * OrderTableFactory.
 */
public class OrderGenTableFactory implements DynamicTableSourceFactory {
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        TableSchema schema =
                TableSchema.builder()
                        .field("user_id", DataTypes.BIGINT())
                        .field("user_name", DataTypes.STRING())
                        .field("item_id", DataTypes.BIGINT())
                        .field("item_name", DataTypes.STRING())
                        .field("price", DataTypes.DECIMAL(38, 2))
                        .field("province", DataTypes.STRING())
                        .field("city", DataTypes.STRING())
                        .field("longitude", DataTypes.STRING())
                        .field("latitude", DataTypes.STRING())
                        .field("ip", DataTypes.STRING())
                        .field("sale_timestamp", DataTypes.TIMESTAMP())
                        .build();
        return new OrderGenTableSource(schema);
    }

    @Override
    public String factoryIdentifier() {
        return "ordergen";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
