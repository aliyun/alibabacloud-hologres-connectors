package com.alibaba.hologres.client.copy.out.arrow.accessor;

import org.apache.arrow.vector.UInt1Vector;

/** Arrow column vector accessor for int. */
public class BaseArrowUInt1Accessor extends AbstractArrowVectorAccessor {
    private final UInt1Vector uInt1Vector;

    public BaseArrowUInt1Accessor(UInt1Vector uInt1Vector) {
        super(uInt1Vector);
        this.uInt1Vector = uInt1Vector;
    }

    public byte getUInt1(int rowId) {
        return uInt1Vector.get(rowId);
    }

    public Object get(int rowId) {
        if (isNullAt(rowId)) {
            return null;
        }
        return getUInt1(rowId) != 0;
    }
}
