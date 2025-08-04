/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.connectors.hologres.utils;

import org.apache.flink.configuration.ReadableConfig;

import com.alibaba.hologres.client.model.checkandput.CheckAndPutCondition;
import com.alibaba.hologres.client.model.checkandput.CheckCompareOp;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCConfigs;

import java.util.Objects;

/** HologresUtils. */
public class HologresUtils {

    public static CheckAndPutCondition getCheckAndPutCondition(ReadableConfig properties) {
        String columnName = properties.get(HologresJDBCConfigs.OPTIONAL_CHECK_AND_PUT_COLUMN);
        if (Objects.isNull(columnName)) {
            return null;
        }

        String operator = properties.get(HologresJDBCConfigs.OPTIONAL_CHECK_AND_PUT_OPERATOR);
        String nullAs = properties.get(HologresJDBCConfigs.OPTIONAL_CHECK_AND_PUT_NULL_AS);
        return new CheckAndPutCondition(columnName, CheckCompareOp.valueOf(operator), null, nullAs);
    }

    public static String removeU0000(final String in) {
        if (in != null && in.contains("\u0000")) {
            return in.replaceAll("\u0000", "");
        } else {
            return in;
        }
    }
}
