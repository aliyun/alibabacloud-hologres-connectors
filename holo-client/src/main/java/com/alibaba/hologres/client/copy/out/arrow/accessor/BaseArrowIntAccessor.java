package com.alibaba.hologres.client.copy.out.arrow.accessor;

import org.apache.arrow.vector.IntVector;

/** Arrow column vector accessor for int. */
public class BaseArrowIntAccessor extends AbstractArrowVectorAccessor {
    private final IntVector intVector;

    public BaseArrowIntAccessor(IntVector intVector) {
        super(intVector);
        this.intVector = intVector;
    }

    public int getInt(int rowId) {
        return intVector.get(rowId);
    }

    public Object get(int rowId) {
        if (isNullAt(rowId)) {
            return null;
        }
        return getInt(rowId);
    }
}
