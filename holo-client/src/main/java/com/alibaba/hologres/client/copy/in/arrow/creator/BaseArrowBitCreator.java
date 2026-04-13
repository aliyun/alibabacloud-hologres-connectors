package com.alibaba.hologres.client.copy.in.arrow.creator;

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.holders.BitHolder;

/** Arrow column vector creator for int. */
public class BaseArrowBitCreator extends AbstractArrowVectorCreator {
    private final BitVector bitVector;

    public BaseArrowBitCreator(BitVector bitVector) {
        super(bitVector);
        this.bitVector = bitVector;
    }

    public void setBit(int rowId, BitHolder value) {
        bitVector.setSafe(rowId, value);
    }

    @Override
    public void set(int rowId, Object value) {
        if (value == null) {
            bitVector.setNull(rowId);
        } else {
            BitHolder bitHolder = new BitHolder();
            bitHolder.value = (boolean) value ? 1 : 0;
            setBit(rowId, bitHolder);
        }
    }
}
