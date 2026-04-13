package com.alibaba.hologres.client.copy.in.arrow.creator;

import org.apache.arrow.vector.Float4Vector;

/** Arrow column vector creator for float4. */
public class BaseArrowFloat4Creator extends AbstractArrowVectorCreator {

    protected final Float4Vector floatVector;

    public BaseArrowFloat4Creator(Float4Vector floatVector) {
        super(floatVector);
        this.floatVector = floatVector;
    }

    public void setFloat(int rowId, float value) {
        floatVector.setSafe(rowId, value);
    }

    @Override
    public void set(int rowId, Object value) {
        if (value == null) {
            floatVector.setNull(rowId);
        } else {
            setFloat(rowId, (float) value);
        }
    }
}
