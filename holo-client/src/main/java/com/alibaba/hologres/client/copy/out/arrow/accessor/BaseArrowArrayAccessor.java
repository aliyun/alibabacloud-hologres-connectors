package com.alibaba.hologres.client.copy.out.arrow.accessor;

import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.reader.FieldReader;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/** Arrow column vector accessor for array. get BaseArrowArrayAccessor */
public class BaseArrowArrayAccessor extends AbstractArrowVectorAccessor {

    private final ListVector listVector;
    protected final int elementType;
    private final UnionListReader listReader;

    public BaseArrowArrayAccessor(ListVector vector, int elementType) {
        super(vector);
        this.listVector = vector;
        this.elementType = elementType;
        this.listReader = vector.getReader();
    }

    public List<Object> getArray(int rowId) {
        listReader.setPosition(rowId);
        if (!listReader.isSet()) {
            return null;
        }

        List<Object> array = new ArrayList<>();
        // 对当前行的 list 迭代元素
        while (listReader.next()) {
            Object value = readElement(listReader);
            array.add(value);
        }
        return array;
    }

    /**
     * 根据 elementType，从当前 listReader 的元素 reader 中读出一个元素。 当前元素对应的 reader 可以通过 listReader.reader() 获取。
     */
    private Object readElement(UnionListReader listReader) {
        // 当前元素的子 reader
        FieldReader elementReader = listReader.reader();

        switch (elementType) {
            case Types.CHAR:
            case Types.VARCHAR:
                return elementReader.readText().toString();
            case Types.INTEGER:
                return elementReader.readInteger();
            case Types.BIGINT:
                return elementReader.readLong();
            case Types.REAL:
                return elementReader.readFloat();
            case Types.DOUBLE:
                return elementReader.readDouble();
            case Types.BIT:
                return elementReader.readByte() != 0;
            default:
                return elementReader.readObject();
        }
    }

    @Override
    public Object get(int rowId) {
        if (isNullAt(rowId)) {
            return null;
        }
        List<Object> list = getArray(rowId);
        if (list == null) {
            return null;
        }
        switch (elementType) {
            case Types.CHAR:
            case Types.VARCHAR:
                return list.toArray(new String[0]);
            case Types.SMALLINT:
                return list.toArray(new Short[0]);
            case Types.INTEGER:
                return list.toArray(new Integer[0]);
            case Types.BIGINT:
                return list.toArray(new Long[0]);
            case Types.FLOAT:
            case Types.REAL:
                return list.toArray(new Float[0]);
            case Types.DOUBLE:
                return list.toArray(new Double[0]);
            case Types.BOOLEAN:
            case Types.BIT:
                return list.toArray(new Boolean[0]);
            default:
                return list;
        }
    }
}
