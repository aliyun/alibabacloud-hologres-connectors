package com.alibaba.hologres.client.copy.in.arrow.creator;

import org.apache.arrow.vector.SmallIntVector;

/** Arrow column vector creator for smallint. */
public class BaseArrowSmallIntCreator extends AbstractArrowVectorCreator {

    protected final SmallIntVector smallIntVector;

    public BaseArrowSmallIntCreator(SmallIntVector smallIntVector) {
        super(smallIntVector);
        this.smallIntVector = smallIntVector;
    }

    public void setShort(int rowId, short value) {
        smallIntVector.setSafe(rowId, value);
    }

    @Override
    public void set(int rowId, Object value) {
        if (value == null) {
            smallIntVector.setNull(rowId);
        } else {
            setShort(rowId, (short) value);
        }
    }
}
