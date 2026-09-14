package com.alibaba.hologres.connector.flink.source.bulkread;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import com.alibaba.hologres.connector.flink.api.HologresTableSchema;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.config.JDBCOptions;
import com.alibaba.hologres.connector.flink.utils.JDBCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

/** Bulkread input format using COPY OUT with Arrow format. */
public class HologresCopyBulkreadInputFormat
        extends RichInputFormat<RowData, HologresShardInputSplit> implements Serializable {

    private static final Logger LOG =
            LoggerFactory.getLogger(HologresCopyBulkreadInputFormat.class);

    private final HologresConnectionParam connectionParam;
    private final JDBCOptions options;
    private final String[] fieldNames;
    private final DataType[] fieldTypes;
    private final String[] holoColumnTypes;
    private final String filterPredicate;
    private final long limit;

    private transient HologresCopyBulkReader copyReader;
    private transient RowData currentRecord;

    public HologresCopyBulkreadInputFormat(
            HologresConnectionParam connectionParam,
            JDBCOptions jdbcOptions,
            TableSchema tableSchema,
            String filterPredicate,
            long limit) {
        this.connectionParam = connectionParam;
        this.options = jdbcOptions;
        this.fieldNames = tableSchema.getFieldNames();
        this.fieldTypes = tableSchema.getFieldDataTypes();
        this.filterPredicate = filterPredicate;
        this.limit = limit;
        this.holoColumnTypes = new String[fieldNames.length];
        HologresTableSchema hologresTableSchema =
                HologresTableSchema.get(connectionParam.getJdbcOptions());
        for (int i = 0; i < fieldNames.length; i++) {
            Integer hologresColumnIndex = hologresTableSchema.get().getColumnIndex(fieldNames[i]);
            this.holoColumnTypes[i] =
                    hologresTableSchema.get().getColumn(hologresColumnIndex).getTypeName();
        }
    }

    @Override
    public HologresShardInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        LOG.info("Creating input splits for Holo shards (COPY mode)");
        int numShards = JDBCUtils.getShardCount(options);
        HologresShardInputSplit[] splits = new HologresShardInputSplit[numShards];
        for (int i = 0; i < numShards; i++) {
            splits[i] = new HologresShardInputSplit(i);
        }
        LOG.info("Created {} input splits for Holo shards (COPY mode)", splits.length);
        return splits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(
            HologresShardInputSplit[] hologresShardInputSplits) {
        return new DefaultInputSplitAssigner(hologresShardInputSplits);
    }

    @Override
    public void open(HologresShardInputSplit inputSplit) throws IOException {
        LOG.info("Opening HoloShardInputSplit {} (COPY mode)", inputSplit.getSplitNumber());
        copyReader =
                new HologresCopyBulkReader(
                        connectionParam,
                        options,
                        fieldNames,
                        fieldTypes,
                        holoColumnTypes,
                        new String[] {String.valueOf(inputSplit.getSplitNumber())},
                        filterPredicate,
                        limit);
        copyReader.open();
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return (currentRecord = copyReader.nextRecord()) == null;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        return currentRecord;
    }

    @Override
    public void close() throws IOException {
        if (copyReader != null) {
            copyReader.close();
        }
    }

    @Override
    public void configure(Configuration configuration) {}

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return null;
    }
}
