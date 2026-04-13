package com.alibaba.hologres.client.copy.in.arrow.creator;

import org.apache.arrow.vector.VarBinaryVector;

/** Arrow column vector creator for var binary. */
public class BaseArrowVarBinaryCreator extends AbstractArrowVectorCreator {

    protected final VarBinaryVector varBinaryVector;

    public BaseArrowVarBinaryCreator(VarBinaryVector varBinaryVector) {
        super(varBinaryVector);
        this.varBinaryVector = varBinaryVector;
    }

    public void setBinary(int rowId, byte[] value) {
        varBinaryVector.setSafe(rowId, value);
    }

    @Override
    public void set(int rowId, Object value) {
        if (value == null) {
            varBinaryVector.setNull(rowId);
        } else {
            setBinary(rowId, (byte[]) value);
        }
    }
}
