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

package com.alibaba.ververica.connectors.hologres.sink.repartition;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;

import com.alibaba.hologres.client.impl.util.ShardUtil;
import com.alibaba.hologres.org.postgresql.jdbc.TimestampUtil;

import java.util.concurrent.ThreadLocalRandom;

/**
 * RowDataShardUtil.
 *
 * <p>Based on a row of data and the key index (Distribution Key) to be hashed, and return the hash
 * result, which is consistent with the distribution hash algorithm of hologres.
 */
public class RowDataShardUtil extends ShardUtil {
    /*
     * hologres currently only supports the following types as distribution keys.
     *  1. Integer types: smallint, int, bigint
     *  2. String types: text, varchar, char
     *  3. Time types: date, timestamptz (timestamp is not supported)
     *      Among them, date type is stored as int in holo, and timestamptz is stored as long in holo
     *  4. Boolean type: boolean
     */
    public static int hash(RowData row, Integer[] indexes, LogicalType[] fieldTypes) {
        int hash = 0;
        boolean first = true;
        if (indexes == null || indexes.length == 0) {
            ThreadLocalRandom rand = ThreadLocalRandom.current();
            hash = rand.nextInt();
        } else {
            for (int i : indexes) {
                Object o;
                LogicalType type = fieldTypes[i];
                switch (type.getTypeRoot()) {
                    case BINARY:
                    case VARBINARY:
                        o = row.getBinary(i);
                        break;
                    case CHAR:
                    case VARCHAR:
                        o = row.getString(i).toString();
                        break;
                    case TINYINT:
                        o = row.getByte(i);
                        break;
                    case SMALLINT:
                        o = row.getShort(i);
                        break;
                    case INTEGER:
                    case DATE:
                        o = row.getInt(i);
                        break;
                    case BIGINT:
                        o = row.getLong(i);
                        break;
                    case BOOLEAN:
                        o = row.getBoolean(i);
                        break;
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                    case TIME_WITHOUT_TIME_ZONE:
                        o =
                                row.getTimestamp(i, ((TimestampType) type).getPrecision())
                                        .toLocalDateTime();
                        o = TimestampUtil.timestampToMillisecond(o, "timestamptz");
                        break;
                    default:
                        throw new IllegalArgumentException(
                                String.format(
                                        "Hologres not support %s type be distribution key",
                                        type.getTypeRoot()));
                }
                if (first) {
                    hash = ShardUtil.hash(o);
                } else {
                    hash ^= ShardUtil.hash(o);
                }
                first = false;
            }
        }
        return hash;
    }
}
