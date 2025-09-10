/*
 *  Copyright (c) 2021, Alibaba Group;
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.hologres.connector.flink.sink.v1;

import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.connector.flink.api.HologresWriter;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
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
