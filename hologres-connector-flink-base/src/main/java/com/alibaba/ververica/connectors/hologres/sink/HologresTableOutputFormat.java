package com.alibaba.ververica.connectors.hologres.sink;

import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.ververica.connectors.hologres.api.HologresWriter;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** Implementation for table api. */
public class HologresTableOutputFormat extends AbstractHologresOutputFormat<RowData> {
    private static final transient Logger LOG =
            LoggerFactory.getLogger(HologresTableOutputFormat.class);

    public HologresTableOutputFormat(
            HologresConnectionParam param, HologresWriter<RowData> hologresIOClient) {
        super(param, hologresIOClient);
    }

    @Override
    public long writeData(RowData rowData) throws HoloClientException, IOException {
        final RowKind kind = rowData.getRowKind();
        long writtenBytes = 0;

        if (kind.equals(RowKind.INSERT) || kind.equals(RowKind.UPDATE_AFTER)) {
            writtenBytes = hologresIOClient.writeAddRecord(rowData);
        } else if ((kind.equals(RowKind.DELETE) || kind.equals(RowKind.UPDATE_BEFORE))
                && !ignoreDelete) {
            writtenBytes = hologresIOClient.writeDeleteRecord(rowData);
        } else {
            LOG.debug("Ignore rowdata {}.", rowData);
        }
        return writtenBytes;
    }
}
