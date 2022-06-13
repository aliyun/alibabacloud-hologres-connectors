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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/** A utility class for blink store. */
public class HologresUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HologresUtils.class);

    public static Object convertStringToInternalObject(String v, DataType dataType) {
        if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.BINARY)) {
            return parseBytes(v);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.VARBINARY)) {
            return parseBytes(v);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.CHAR)) {
            return StringData.fromString(v);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.VARCHAR)) {
            return StringData.fromString(v);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.TINYINT)) {
            return Byte.valueOf(v);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.SMALLINT)) {
            return Short.valueOf(v);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.INTEGER)) {
            return Integer.valueOf(v);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.BIGINT)) {
            return Long.valueOf(v);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.FLOAT)) {
            return Float.valueOf(v);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.DOUBLE)) {
            return Double.valueOf(v);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.BOOLEAN)) {
            v = v.equals("t") ? "true" : "false";
            return Boolean.valueOf(v);
        } else if (dataType.getLogicalType()
                .getTypeRoot()
                .equals(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
            // Hologres supports TIMESTAMP_WITHOUT_TIME_ZONE and TIMESTAMP_WITH_TIME_ZONE
            // Flink doesn't support TIMESTAMP_WITH_TIME_ZONE
            // Hologres presents zone id in ZoneOffset, e.g. "+08", "+08:05:43" */
            int index = v.lastIndexOf("+") != -1 ? v.lastIndexOf("+") : v.lastIndexOf("-");
            String timezonePart = v.substring(index);
            Timestamp ts;
            if (isValidTimeZone(timezonePart)) {
                String datetimePart = v.substring(0, index).replace(" ", "T");
                ts =
                        Timestamp.valueOf(
                                ZonedDateTime.of(
                                                LocalDateTime.parse(datetimePart),
                                                ZoneId.of(timezonePart))
                                        .toLocalDateTime());
            } else {
                ts = Timestamp.valueOf(LocalDateTime.parse(v.replace(" ", "T")));
            }
            return TimestampData.fromTimestamp(ts);
        } else if (dataType.getLogicalType()
                .getTypeRoot()
                .equals(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE)) {
            return (int) (Time.valueOf(v).toLocalTime().toNanoOfDay() / 1_000_000L);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.DATE)) {
            return (int) Date.valueOf(v).toLocalDate().toEpochDay();
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.DECIMAL)) {
            DecimalType logicalType = (DecimalType) dataType.getLogicalType();
            return DecimalData.fromBigDecimal(
                    new BigDecimal(v), logicalType.getPrecision(), logicalType.getScale());
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.ARRAY)) {
            String[] arrayObject = v.split("\\{|}|,");
            if (dataType.getLogicalType()
                    .getChildren()
                    .get(0)
                    .getTypeRoot()
                    .equals(LogicalTypeRoot.BIGINT)) {
                long[] arrLongObject = new long[arrayObject.length - 1];
                for (int i = 1; i < arrayObject.length; ++i) {
                    arrLongObject[i - 1] = Long.valueOf(arrayObject[i]);
                }
                return new GenericArrayData(arrLongObject);
            } else if (dataType.getLogicalType()
                    .getChildren()
                    .get(0)
                    .getTypeRoot()
                    .equals(LogicalTypeRoot.INTEGER)) {
                int[] arrIntObject = new int[arrayObject.length - 1];
                for (int i = 1; i < arrayObject.length; ++i) {
                    arrIntObject[i - 1] = Integer.valueOf(arrayObject[i]);
                }
                return new GenericArrayData(arrIntObject);
            } else if (dataType.getLogicalType()
                    .getChildren()
                    .get(0)
                    .getTypeRoot()
                    .equals(LogicalTypeRoot.FLOAT)) {
                float[] arrFloatObject = new float[arrayObject.length - 1];
                for (int i = 1; i < arrayObject.length; ++i) {
                    arrFloatObject[i - 1] = Float.valueOf(arrayObject[i]);
                }
                return new GenericArrayData(arrFloatObject);
            } else if (dataType.getLogicalType()
                    .getChildren()
                    .get(0)
                    .getTypeRoot()
                    .equals(LogicalTypeRoot.DOUBLE)) {
                double[] arrDoubleObject = new double[arrayObject.length - 1];
                for (int i = 1; i < arrayObject.length; ++i) {
                    arrDoubleObject[i - 1] = Double.valueOf(arrayObject[i]);
                }
                return new GenericArrayData(arrDoubleObject);
            } else if (dataType.getLogicalType()
                    .getChildren()
                    .get(0)
                    .getTypeRoot()
                    .equals(LogicalTypeRoot.VARCHAR)) {
                StringData[] arrStringObject = new StringData[arrayObject.length - 1];
                for (int i = 1; i < arrayObject.length; ++i) {
                    arrStringObject[i - 1] = StringData.fromString(arrayObject[i]);
                }
                return new GenericArrayData(arrStringObject);
            } else if (dataType.getLogicalType()
                    .getChildren()
                    .get(0)
                    .getTypeRoot()
                    .equals(LogicalTypeRoot.BOOLEAN)) {
                boolean[] arrBoolObject = new boolean[arrayObject.length - 1];
                for (int i = 1; i < arrayObject.length; ++i) {
                    arrayObject[i] = arrayObject[i].equals("t") ? "true" : "false";
                    arrBoolObject[i - 1] = Boolean.valueOf(arrayObject[i]);
                }
                return new GenericArrayData(arrBoolObject);
            } else {
                throw new IllegalArgumentException(
                        "Currently hologres does not support type: " + dataType);
            }
        }
        throw new IllegalArgumentException("Unknown hologres type: " + dataType);
    }

    /** Hologres presents zone id in ZoneOffset, e.g. "+08". */
    private static boolean isValidTimeZone(String zoneId) {
        try {
            ZoneOffset.of(zoneId);
        } catch (DateTimeException e) {
            return false;
        }
        return true;
    }

    private static byte[] parseBytes(String hex) {
        try {
            // Postgres uses the Hex format to store the bytes.
            // The input string hex is beginning with "\\x". Please refer to
            // https://www.postgresql.org/docs/11/datatype-binary.html?spm=a2c4g.11186623.2.9.3a907ad7ihlkEI
            return Hex.decodeHex(hex.substring(3));
        } catch (DecoderException e) {
            throw new RuntimeException(
                    String.format("Failed to parse the bytes from the '%s'.", hex), e);
        }
    }
}
