package com.alibaba.hologres.client.copy.in.arrow.creator;

import org.apache.arrow.vector.Float8Vector;

/** Arrow column vector creator for float8. */
public class BaseArrowFloat8Creator extends AbstractArrowVectorCreator {

    protected final Float8Vector doubleVector;

    public BaseArrowFloat8Creator(Float8Vector doubleVector) {
        super(doubleVector);
        this.doubleVector = doubleVector;
    }

    public void setDouble(int rowId, double value) {
        doubleVector.setSafe(rowId, value);
    }

    @Override
    public void set(int rowId, Object value) {
        if (value == null) {
            doubleVector.setNull(rowId);
        } else {
            setDouble(rowId, (double) value);
        }
    }
}
