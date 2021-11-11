package com.alibaba.ververica.connectors.hologres.source.lookup;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.ververica.connectors.common.dim.DimJoinFetcher;
import com.alibaba.ververica.connectors.common.dim.cache.CacheStrategy;
import com.alibaba.ververica.connectors.hologres.api.HologresReader;
import com.alibaba.ververica.connectors.hologres.utils.FlinkUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** An abstract hologres sync or async row fetcher class. */
public abstract class AbstractHologresLookupFunction extends DimJoinFetcher
        implements ResultTypeQueryable<RowData> {
    protected TypeInformation<RowData> rowDataTypeInformation;
    protected HologresReader<RowData> hologresReader;
    protected final List<Integer> sourceKeys;
    protected final LogicalType[] indexColumnTypes;
    protected final String[] fieldNames;
    protected final DataType[] fieldTypes;

    protected AbstractHologresLookupFunction(
            String sqlTableName,
            TableSchema tableSchema,
            String[] index,
            CacheStrategy cacheStrategy,
            HologresReader<RowData> hologresReader) {
        super(sqlTableName, index, cacheStrategy);
        this.hologresReader = hologresReader;
        this.fieldNames = tableSchema.getFieldNames();
        this.fieldTypes = tableSchema.getFieldDataTypes();
        this.rowDataTypeInformation = FlinkUtil.getRowTypeInfo(tableSchema);
        this.sourceKeys = new ArrayList<>();
        this.indexColumnTypes = new LogicalType[index.length];
        for (int i = 0; i < index.length; i++) {
            this.sourceKeys.add(i);
            int targetIdx = -1;
            for (int j = 0; j < fieldNames.length; j++) {
                if (fieldNames[j].equals(index[i])) {
                    targetIdx = j;
                    break;
                }
            }
            this.indexColumnTypes[i] = fieldTypes[targetIdx].getLogicalType();
        }
    }

    @Override
    public void openConnection(Configuration parameters) {
        try {
            hologresReader.open(getRuntimeContext());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void closeConnection() {
        try {
            hologresReader.close();
        } catch (IOException | HoloClientException e) {
            throw new RuntimeException(e);
        }
    }

    protected Object getSourceKey(org.apache.flink.table.data.RowData source) {
        return getKey(source, sourceKeys, indexColumnTypes);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return rowDataTypeInformation;
    }
}
