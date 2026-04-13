package com.alibaba.hologres.client.copy.out.arrow.accessor;

import org.apache.arrow.vector.DateMilliVector;

import java.sql.Timestamp;

/** Arrow column vector accessor for date milli. */
public class BaseArrowDateMilliAccessor extends AbstractArrowVectorAccessor {

    protected final DateMilliVector dateMilliVector;

    public BaseArrowDateMilliAccessor(DateMilliVector dateMilliVector) {
        super(dateMilliVector);
        this.dateMilliVector = dateMilliVector;
    }

    public long getMilliSeconds(int rowId) {
        return this.dateMilliVector.get(rowId);
    }

    @Override
    public Object get(int rowId) {
        if (isNullAt(rowId)) {
            return null;
        }
        return new Timestamp(getMilliSeconds(rowId));
    }
}
