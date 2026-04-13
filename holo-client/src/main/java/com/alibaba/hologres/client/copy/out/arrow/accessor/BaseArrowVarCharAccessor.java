package com.alibaba.hologres.client.copy.out.arrow.accessor;

import org.apache.arrow.vector.VarCharVector;

/** Arrow column vector accessor for var char. */
public class BaseArrowVarCharAccessor extends AbstractArrowVectorAccessor {

    protected final VarCharVector varCharVector;
    protected final int precision;

    public BaseArrowVarCharAccessor(VarCharVector varCharVector) {
        this(varCharVector, -1);
    }

    public BaseArrowVarCharAccessor(VarCharVector varCharVector, int precision) {
        super(varCharVector);
        this.varCharVector = varCharVector;
        this.precision = precision;
    }

    public byte[] getBytes(int rowId) {
        return varCharVector.get(rowId);
    }

    @Override
    public Object get(int rowId) {
        if (isNullAt(rowId)) {
            return null;
        }
        String s = new String(getBytes(rowId));
        if (precision > 0) {
            s = String.format("%-" + precision + "s", s);
        }
        return s;
    }
}
