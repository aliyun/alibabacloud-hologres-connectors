package com.alibaba.ververica.connectors.hologres.source.lookup;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import com.alibaba.ververica.connectors.common.dim.cache.CacheStrategy;
import com.alibaba.ververica.connectors.hologres.api.HologresReader;

import java.io.IOException;

/** An abstract hologres sync or async row fetcher class. */
public abstract class AbstractHologresLookupFunction<T> extends DimJoinFetcher<T>
        implements ResultTypeQueryable<RowData> {
    protected InternalTypeInfo rowTypeInfo;
    protected HologresReader<RowData> hologresReader;
    protected boolean hasPrimaryKey;

    protected AbstractHologresLookupFunction(
            String sqlTableName,
            TableSchema tableSchema,
            String[] index,
            CacheStrategy cacheStrategy,
            HologresReader<RowData> hologresReader,
            boolean hasPrimaryKey) {
        super(
                sqlTableName,
                (RowType) tableSchema.toPhysicalRowDataType().getLogicalType(),
                index,
                cacheStrategy);
        this.hologresReader = hologresReader;
        this.rowTypeInfo = InternalTypeInfo.of(tableSchema.toRowDataType().getLogicalType());
        this.hasPrimaryKey = hasPrimaryKey;
    }

    @Override
    public void openConnection(Configuration parameters) {
        try {
            hologresReader.open();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void closeConnection() {
        try {
            hologresReader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasPrimaryKey() {
        return this.hasPrimaryKey;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return this.rowTypeInfo;
    }
}
