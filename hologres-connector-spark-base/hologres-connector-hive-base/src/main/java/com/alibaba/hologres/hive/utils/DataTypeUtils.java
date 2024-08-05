package com.alibaba.hologres.hive.utils;

import com.google.common.primitives.Booleans;
import com.google.common.primitives.Floats;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** DataTypeUtils. */
public class DataTypeUtils {
    public static int[] castIntWritableArrayListToArray(Object obj) {
        List<Integer> result = new ArrayList<>();
        if (obj instanceof List<?>) {
            for (Object o : (List<?>) obj) {
                if (Objects.isNull(o)) {
                    continue;
                }
                result.add(Integer.valueOf(o.toString()));
            }
            return result.stream().mapToInt(Integer::intValue).toArray();
        }
        return null;
    }

    public static long[] castLongWritableArrayListToArray(Object obj) {
        List<Long> result = new ArrayList<>();
        if (obj instanceof List<?>) {
            for (Object o : (List<?>) obj) {
                if (Objects.isNull(o)) {
                    continue;
                }
                result.add(Long.valueOf(o.toString()));
            }
            return result.stream().mapToLong(Long::longValue).toArray();
        }
        return null;
    }

    public static float[] castFloatWritableArrayListToArray(Object obj) {
        List<Float> result = new ArrayList<>();
        if (obj instanceof List<?>) {
            for (Object o : (List<?>) obj) {
                if (Objects.isNull(o)) {
                    continue;
                }
                result.add(Float.valueOf(o.toString()));
            }
            return Floats.toArray(result);
        }
        return null;
    }

    public static double[] castDoubleWritableArrayListToArray(Object obj) {
        List<Double> result = new ArrayList<>();
        if (obj instanceof List<?>) {
            for (Object o : (List<?>) obj) {
                if (Objects.isNull(o)) {
                    continue;
                }
                result.add(Double.valueOf(o.toString()));
            }
            return result.stream().mapToDouble(Double::doubleValue).toArray();
        }
        return null;
    }

    public static boolean[] castBooleanWritableArrayListToArray(Object obj) {
        List<Boolean> result = new ArrayList<>();
        if (obj instanceof List<?>) {
            for (Object o : (List<?>) obj) {
                if (Objects.isNull(o)) {
                    continue;
                }
                result.add(Boolean.valueOf(o.toString()));
            }
            return Booleans.toArray(result);
        }
        return null;
    }

    public static String[] castHiveTextArrayListToArray(Object obj) {
        List<String> result = new ArrayList<>();
        if (obj instanceof List<?>) {
            for (Object o : (List<?>) obj) {
                if (Objects.isNull(o)) {
                    continue;
                }
                result.add(String.valueOf(o.toString()));
            }
            return result.toArray(new String[0]);
        }
        return null;
    }
}
