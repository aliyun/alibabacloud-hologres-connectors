package com.alibaba.hologres.client.copy.in.arrow.creator;

import org.apache.arrow.vector.IntVector;

/** Arrow column vector creator for int. */
public class BaseArrowIntCreator extends AbstractArrowVectorCreator {
    private final IntVector intVector;

    public BaseArrowIntCreator(IntVector intVector) {
        super(intVector);
        this.intVector = intVector;
    }

    public void setInt(int rowId, int value) {
        intVector.setSafe(rowId, value);
    }

    public void set(int rowId, Object value) {
        if (value == null) {
            intVector.setNull(rowId);
        } else {
            setInt(rowId, (int) value);
        }
    }
}
