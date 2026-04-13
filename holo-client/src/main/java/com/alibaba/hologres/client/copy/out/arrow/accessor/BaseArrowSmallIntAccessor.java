package com.alibaba.hologres.client.copy.out.arrow.accessor;

import org.apache.arrow.vector.SmallIntVector;

/** Arrow column vector accessor for smallint. */
public class BaseArrowSmallIntAccessor extends AbstractArrowVectorAccessor {

    protected final SmallIntVector smallIntVector;

    public BaseArrowSmallIntAccessor(SmallIntVector smallIntVector) {
        super(smallIntVector);
        this.smallIntVector = smallIntVector;
    }

    public short getShort(int rowId) {
        return smallIntVector.get(rowId);
    }

    @Override
    public Object get(int rowId) {
        if (isNullAt(rowId)) {
            return null;
        }
        return getShort(rowId);
    }
}
