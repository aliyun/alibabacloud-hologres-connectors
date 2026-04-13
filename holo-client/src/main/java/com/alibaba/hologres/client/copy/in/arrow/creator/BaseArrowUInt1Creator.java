package com.alibaba.hologres.client.copy.in.arrow.creator;

import org.apache.arrow.vector.UInt1Vector;

/** Arrow column vector creator for int. */
public class BaseArrowUInt1Creator extends AbstractArrowVectorCreator {
    private final UInt1Vector uInt1Vector;

    public BaseArrowUInt1Creator(UInt1Vector uInt1Vector) {
        super(uInt1Vector);
        this.uInt1Vector = uInt1Vector;
    }

    public void setUInt1(int rowId, byte value) {
        uInt1Vector.setSafe(rowId, value);
    }

    @Override
    public void set(int rowId, Object value) {
        if (value == null) {
            uInt1Vector.setNull(rowId);
        } else {
            boolean boolValue = (boolean) value;
            setUInt1(rowId, (byte) (boolValue ? 1 : 0));
        }
    }
}
