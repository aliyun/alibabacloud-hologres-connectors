package com.alibaba.hologres.client.copy.out.arrow.accessor;

import org.apache.arrow.vector.VarBinaryVector;

/** Arrow column vector accessor for var binary. */
public class BaseArrowVarBinaryAccessor extends AbstractArrowVectorAccessor {

    protected final VarBinaryVector varBinaryVector;

    public BaseArrowVarBinaryAccessor(VarBinaryVector varBinaryVector) {
        super(varBinaryVector);
        this.varBinaryVector = varBinaryVector;
    }

    public byte[] getBinary(int rowId) {
        return varBinaryVector.get(rowId);
    }

    @Override
    public Object get(int rowId) {
        if (isNullAt(rowId)) {
            return null;
        }
        return getBinary(rowId);
    }
}
