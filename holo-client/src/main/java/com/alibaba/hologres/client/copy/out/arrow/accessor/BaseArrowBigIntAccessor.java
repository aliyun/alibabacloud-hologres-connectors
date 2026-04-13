package com.alibaba.hologres.client.copy.out.arrow.accessor;

import org.apache.arrow.vector.BigIntVector;

/** Arrow column vector accessor for bigint. */
public class BaseArrowBigIntAccessor extends AbstractArrowVectorAccessor {

    protected final BigIntVector bigIntVector;

    public BaseArrowBigIntAccessor(BigIntVector bigIntVector) {
        super(bigIntVector);
        this.bigIntVector = bigIntVector;
    }

    public long getLong(int rowId) {
        return bigIntVector.get(rowId);
    }

    @Override
    public Object get(int rowId) {
        if (isNullAt(rowId)) {
            return null;
        }
        return getLong(rowId);
    }
}
