package com.alibaba.hologres.hive.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** DataTypeUtils. */
public class DataTypeUtils {
    public static Integer[] castIntWritableArrayListToArray(Object obj) {
        List<Integer> result = new ArrayList<>();
        if (obj instanceof List<?>) {
            for (Object o : (List<?>) obj) {
                if (Objects.isNull(o)) {
                    continue;
                }
                result.add(Integer.valueOf(o.toString()));
            }
            return result.toArray(new Integer[0]);
        }
        return null;
    }

    public static Long[] castLongWritableArrayListToArray(Object obj) {
        List<Long> result = new ArrayList<>();
        if (obj instanceof List<?>) {
            for (Object o : (List<?>) obj) {
                if (Objects.isNull(o)) {
                    continue;
                }
                result.add(Long.valueOf(o.toString()));
            }
            return result.toArray(new Long[0]);
        }
        return null;
    }

    public static Float[] castFloatWritableArrayListToArray(Object obj) {
        List<Float> result = new ArrayList<>();
        if (obj instanceof List<?>) {
            for (Object o : (List<?>) obj) {
                if (Objects.isNull(o)) {
                    continue;
                }
                result.add(Float.valueOf(o.toString()));
            }
            return result.toArray(new Float[0]);
        }
        return null;
    }

    public static Double[] castDoubleWritableArrayListToArray(Object obj) {
        List<Double> result = new ArrayList<>();
        if (obj instanceof List<?>) {
            for (Object o : (List<?>) obj) {
                if (Objects.isNull(o)) {
                    continue;
                }
                result.add(Double.valueOf(o.toString()));
            }
            return result.toArray(new Double[0]);
        }
        return null;
    }

    public static Boolean[] castBooleanWritableArrayListToArray(Object obj) {
        List<Boolean> result = new ArrayList<>();
        if (obj instanceof List<?>) {
            for (Object o : (List<?>) obj) {
                if (Objects.isNull(o)) {
                    continue;
                }
                result.add(Boolean.valueOf(o.toString()));
            }
            return result.toArray(new Boolean[0]);
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
