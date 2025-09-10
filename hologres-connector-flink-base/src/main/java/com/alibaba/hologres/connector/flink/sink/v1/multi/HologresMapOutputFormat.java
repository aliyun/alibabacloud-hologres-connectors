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

package com.alibaba.hologres.connector.flink.sink.v1.multi;

import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.connector.flink.api.HologresWriter;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.sink.v1.AbstractHologresOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/** Implementation for table api. */
public class HologresMapOutputFormat extends AbstractHologresOutputFormat<Map<String, Object>> {
    private static final transient Logger LOG =
            LoggerFactory.getLogger(HologresMapOutputFormat.class);

    public HologresMapOutputFormat(
            HologresConnectionParam param, HologresWriter<Map<String, Object>> hologresIOClient) {
        super(param, hologresIOClient);
    }

    @Override
    public long writeData(Map<String, Object> record) throws HoloClientException, IOException {
        return hologresIOClient.writeAddRecord(record);
    }
}
