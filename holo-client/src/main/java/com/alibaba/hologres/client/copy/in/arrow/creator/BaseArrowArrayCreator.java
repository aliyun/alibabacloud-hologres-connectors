package com.alibaba.hologres.client.copy.in.arrow.creator;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/** Arrow column vector creator for array. */
public class BaseArrowArrayCreator extends AbstractArrowVectorCreator {

    private final ListVector listVector;
    protected final int elementType;
    protected UnionListWriter writer;

    public BaseArrowArrayCreator(ListVector vector, int elementType) {
        super(vector);
        this.listVector = vector;
        this.elementType = elementType;
        this.writer = listVector.getWriter();
    }

    public void setArray(int rowId, List<?> value) {
        writer.setPosition(rowId);
        writer.startList();

        // 直接处理特定类型的List，无需再进行elementType判断
        for (Object obj : value) {
            if (obj == null) {
                writer.writeNull();
            } else {
                // 根据elementType调用对应的write方法
                switch (elementType) {
                    case Types.INTEGER:
                        writer.writeInt((Integer) obj);
                        break;
                    case Types.BIGINT:
                        writer.writeBigInt((Long) obj);
                        break;
                    case Types.SMALLINT:
                        writer.writeSmallInt((Short) obj);
                        break;
                    case Types.FLOAT:
                    case Types.REAL:
                        writer.writeFloat4((Float) obj);
                        break;
                    case Types.DOUBLE:
                        writer.writeFloat8((Double) obj);
                        break;
                    case Types.BOOLEAN:
                    case Types.BIT:
                        writer.writeBit((Boolean) obj ? 1 : 0);
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                        // 使用底层 ArrowBuf API 代替 writeVarChar(String),
                        // 后者是 Arrow 16+ 才引入的便利方法,
                        // 而 Spark 自带的 Arrow 版本较老(无此方法),
                        // 使用 writeVarChar(int, int, ArrowBuf) 版本可保证多版本兼容.
                        {
                            byte[] bytes = obj.toString().getBytes(StandardCharsets.UTF_8);
                            try (ArrowBuf buf = listVector.getAllocator().buffer(bytes.length)) {
                                buf.setBytes(0, bytes);
                                writer.writeVarChar(0, bytes.length, buf);
                            }
                        }
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Unsupported array element type: " + elementType);
                }
            }
        }

        writer.endList();
    }

    @Override
    public void set(int rowId, Object value) {
        if (value != null) {
            // 根据elementType将数组转换为特定类型的List
            List<?> list = null;
            switch (elementType) {
                case Types.INTEGER:
                    if (value instanceof int[]) {
                        int[] array = (int[]) value;
                        List<Integer> intList = new ArrayList<>(array.length);
                        for (int obj : array) {
                            intList.add(obj);
                        }
                        list = intList;
                    } else if (value instanceof Object[]) {
                        Object[] array = (Object[]) value;
                        List<Integer> intList = new ArrayList<>(array.length);
                        for (Object obj : array) {
                            intList.add((Integer) obj);
                        }
                        list = intList;
                    } else if (value instanceof List) {
                        list = (List<?>) value;
                    }
                    break;
                case Types.BIGINT:
                    if (value instanceof long[]) {
                        long[] array = (long[]) value;
                        List<Long> longList = new ArrayList<>(array.length);
                        for (long obj : array) {
                            longList.add(obj);
                        }
                        list = longList;
                    } else if (value instanceof Object[]) {
                        Object[] array = (Object[]) value;
                        List<Long> longList = new ArrayList<>(array.length);
                        for (Object obj : array) {
                            longList.add((Long) obj);
                        }
                        list = longList;
                    } else if (value instanceof List) {
                        list = (List<?>) value;
                    }
                    break;
                case Types.SMALLINT:
                    if (value instanceof short[]) {
                        short[] array = (short[]) value;
                        List<Short> shortList = new ArrayList<>(array.length);
                        for (short obj : array) {
                            shortList.add(obj);
                        }
                        list = shortList;
                    } else if (value instanceof Object[]) {
                        Object[] array = (Object[]) value;
                        List<Short> shortList = new ArrayList<>(array.length);
                        for (Object obj : array) {
                            shortList.add((Short) obj);
                        }
                        list = shortList;
                    } else if (value instanceof List) {
                        list = (List<?>) value;
                    }
                    break;
                case Types.FLOAT:
                case Types.REAL:
                    if (value instanceof float[]) {
                        float[] array = (float[]) value;
                        List<Float> floatList = new ArrayList<>(array.length);
                        for (float obj : array) {
                            floatList.add(obj);
                        }
                        list = floatList;
                    } else if (value instanceof Object[]) {
                        Object[] array = (Object[]) value;
                        List<Float> floatList = new ArrayList<>(array.length);
                        for (Object obj : array) {
                            floatList.add((Float) obj);
                        }
                        list = floatList;
                    } else if (value instanceof List) {
                        list = (List<?>) value;
                    }
                    break;
                case Types.DOUBLE:
                    if (value instanceof double[]) {
                        double[] array = (double[]) value;
                        List<Double> doubleList = new ArrayList<>(array.length);
                        for (double obj : array) {
                            doubleList.add(obj);
                        }
                        list = doubleList;
                    } else if (value instanceof Object[]) {
                        Object[] array = (Object[]) value;
                        List<Double> doubleList = new ArrayList<>(array.length);
                        for (Object obj : array) {
                            doubleList.add((Double) obj);
                        }
                        list = doubleList;
                    } else if (value instanceof List) {
                        list = (List<?>) value;
                    }
                    break;
                case Types.BOOLEAN:
                case Types.BIT:
                    if (value instanceof boolean[]) {
                        boolean[] array = (boolean[]) value;
                        List<Boolean> booleanList = new ArrayList<>(array.length);
                        for (boolean obj : array) {
                            booleanList.add(obj);
                        }
                        list = booleanList;
                    } else if (value instanceof Object[]) {
                        Object[] array = (Object[]) value;
                        List<Boolean> booleanList = new ArrayList<>(array.length);
                        for (Object obj : array) {
                            booleanList.add((Boolean) obj);
                        }
                        list = booleanList;
                    } else if (value instanceof List) {
                        list = (List<?>) value;
                    }
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                    if (value instanceof Object[]) {
                        Object[] array = (Object[]) value;
                        List<String> stringList = new ArrayList<>(array.length);
                        for (Object obj : array) {
                            stringList.add(obj != null ? obj.toString() : null);
                        }
                        list = stringList;
                    } else if (value instanceof List) {
                        list = (List<?>) value;
                    }
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported array element type: " + elementType);
            }

            if (list == null) {
                throw new IllegalArgumentException("Unsupported array type: " + value.getClass());
            }

            setArray(rowId, list);
        }
    }
}
