package com.alibaba.hologres.client.copy.out.arrow.accessor;

import org.apache.arrow.vector.TimeMicroVector;

import java.sql.Time;
import java.util.TimeZone;

/** Arrow column vector accessor for time micro. */
public class BaseArrowTimeMicroAccessor extends AbstractArrowVectorAccessor {
    public static final long TIMEZONE_OFFSET = TimeZone.getDefault().getRawOffset();

    protected final TimeMicroVector timeMicroVector;

    public BaseArrowTimeMicroAccessor(TimeMicroVector timeMicroVector) {
        super(timeMicroVector);
        this.timeMicroVector = timeMicroVector;
    }

    public long getMicroSeconds(int rowId) {
        return this.timeMicroVector.get(rowId);
    }

    @Override
    public Object get(int rowId) {
        if (isNullAt(rowId)) {
            return null;
        }
        return new Time(getMicroSeconds(rowId) / 1000 - TIMEZONE_OFFSET);
    }
}
