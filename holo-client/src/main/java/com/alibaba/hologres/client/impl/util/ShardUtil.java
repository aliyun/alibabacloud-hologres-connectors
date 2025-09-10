/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.util;

import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.Record;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.postgresql.jdbc.ArrayUtil;
import org.postgresql.jdbc.TimestampUtil;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.sql.Types;
import java.time.LocalTime;
import java.util.concurrent.ThreadLocalRandom;

/** shard相关的工具方法. */
public class ShardUtil {

    private static final Charset UTF8 = Charset.forName("UTF-8");
    public static final String NAME = "HASH";
    public static final HashFunction HASH = Hashing.murmur3_32(104729);

    public static final int RANGE_START = 0;
    public static final int RANGE_END = 65536;

    public static final int NULL_HASH_CODE = Integer.remainderUnsigned(hash(""), RANGE_END);

    public static int hash(Record record, int[] indexes) {
        int hash = 0;
        boolean first = true;
        if (indexes == null || indexes.length == 0) {
            ThreadLocalRandom rand = ThreadLocalRandom.current();
            hash = rand.nextInt();
        } else {
            for (int i : indexes) {
                if (record.getSchema().getColumn(i).isGeneratedColumn()) {
                    // 生成列做分布键,写入时无法确定相应的value,skip
                    continue;
                }
                Object obj = getStorageValue(record, i);
                if (first) {
                    hash = ShardUtil.hash(obj);
                } else {
                    hash ^= ShardUtil.hash(obj);
                }
                first = false;
            }
        }
        return hash;
    }

    public static int hash(Object obj) {
        if (obj == null) {
            return NULL_HASH_CODE;
        } else {
            if (obj instanceof byte[]) {
                return HASH.hashBytes((byte[]) obj).asInt();
            } else if (obj.getClass().isArray()) {
                int hash = 0;
                int length = Array.getLength(obj);
                for (int i = 0; i < length; ++i) {
                    Object child = Array.get(obj, i);
                    hash = hash * 31 + (child == null ? 0 : hash(child));
                }
                return hash;
            } else if (obj instanceof Boolean) {
                return hash(((Boolean) obj) ? 1 : 0);
            } else {
                return hash(String.valueOf(obj).getBytes(UTF8));
            }
        }
    }

    public static int[][] split(int n) {
        int base = RANGE_END / n;
        int remain = RANGE_END % n;
        int start = 0;
        int end = 0;
        int[][] ret = new int[n][];
        for (int i = 0; i < n; ++i) {
            end = start + base + ((remain > 0) ? 1 : 0);
            if (remain > 0) {
                --remain;
            }
            ret[i] = new int[] {start, end};
            start = end;
        }
        return ret;
    }

    /** 一些类型在holo中的实际存储类型并不是字面值,需要获取其存储的类型来计算shard信息. */
    private static Object getStorageValue(Record record, int index) {
        Object obj = record.getObject(index);
        Column column = record.getSchema().getColumn(index);
        switch (column.getType()) {
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                if ("timestamp".equals(column.getTypeName())) {
                    // hologres 3.0版本支持timestamp类型做分布键
                    return TimestampUtil.timestampToMicroSecond(obj, column.getTypeName(), false);
                } else {
                    return TimestampUtil.timestampToMillisecond(obj, column.getTypeName());
                }
            case Types.DATE:
                return TimestampUtil.formatDateObject(obj).toLocalDate().toEpochDay();
            case Types.NUMERIC:
            case Types.DECIMAL:
                // hologres 3.1版本支持decimal类型做分布键
                BigInteger rescaled =
                        ((BigDecimal) obj)
                                .setScale(column.getScale(), RoundingMode.HALF_UP)
                                .unscaledValue();
                byte[] tmp = rescaled.toByteArray();
                if (tmp.length > 16) {
                    throw new NumberFormatException(obj + " is Too Large to Store!");
                }
                ArrayUtil.reverse(tmp);
                byte[] result = new byte[16];
                System.arraycopy(tmp, 0, result, 0, tmp.length);
                return result;
            case Types.TIME:
                // hologres 3.1版本支持time类型做分布键, timetz不支持
                if ("time".equals(column.getTypeName())) {
                    if (obj instanceof java.sql.Time) {
                        java.sql.Time timeObj = (java.sql.Time) obj;
                        LocalTime localTime = timeObj.toLocalTime();
                        long nanos = (timeObj.getTime() % 1000) * 1000000L;
                        localTime = localTime.plusNanos(nanos);
                        return localTime.toNanoOfDay() / 1000;
                    } else if (obj instanceof java.time.LocalTime) {
                        java.time.LocalTime localTime = (java.time.LocalTime) obj;
                        return localTime.toNanoOfDay() / 1000;
                    }
                }
            default:
                return obj;
        }
    }
}
