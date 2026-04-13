package com.alibaba.hologres.client.copy.out.arrow.accessor;

import org.apache.arrow.vector.Float4Vector;

/** Arrow column vector accessor for float4. */
public class BaseArrowFloat4Accessor extends AbstractArrowVectorAccessor {

    protected final Float4Vector floatVector;

    public BaseArrowFloat4Accessor(Float4Vector floatVector) {
        super(floatVector);
        this.floatVector = floatVector;
    }

    public float getFloat(int rowId) {
        return floatVector.get(rowId);
    }

    @Override
    public Object get(int rowId) {
        if (isNullAt(rowId)) {
            return null;
        }
        return getFloat(rowId);
    }
}
