package com.alibaba.hologres.client.copy.out.arrow.accessor;

import org.apache.arrow.vector.Float8Vector;

/** Arrow column vector accessor for float8. */
public class BaseArrowFloat8Accessor extends AbstractArrowVectorAccessor {

    protected final Float8Vector doubleVector;

    public BaseArrowFloat8Accessor(Float8Vector doubleVector) {
        super(doubleVector);
        this.doubleVector = doubleVector;
    }

    public double getDouble(int rowId) {
        return doubleVector.get(rowId);
    }

    @Override
    public Object get(int rowId) {
        if (isNullAt(rowId)) {
            return null;
        }
        return getDouble(rowId);
    }
}
