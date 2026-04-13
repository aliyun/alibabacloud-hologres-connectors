package com.alibaba.hologres.client.copy.in.arrow.creator;

import org.apache.arrow.vector.BigIntVector;

/** Arrow column vector creator for bigint. */
public class BaseArrowBigIntCreator extends AbstractArrowVectorCreator {

    protected final BigIntVector bigIntVector;

    public BaseArrowBigIntCreator(BigIntVector bigIntVector) {
        super(bigIntVector);
        this.bigIntVector = bigIntVector;
    }

    public void setLong(int rowId, long value) {
        bigIntVector.setSafe(rowId, value);
    }

    @Override
    public void set(int rowId, Object value) {
        if (value == null) {
            bigIntVector.setNull(rowId);
        } else {
            setLong(rowId, (long) value);
        }
    }
}
