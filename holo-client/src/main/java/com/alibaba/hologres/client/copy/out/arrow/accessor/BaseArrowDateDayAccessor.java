package com.alibaba.hologres.client.copy.out.arrow.accessor;

import org.apache.arrow.vector.DateDayVector;

import java.sql.Date;
import java.time.LocalDate;

/** Arrow column vector accessor for date day. */
public class BaseArrowDateDayAccessor extends AbstractArrowVectorAccessor {

    protected final DateDayVector dateDayVector;

    public BaseArrowDateDayAccessor(DateDayVector dateDayVector) {
        super(dateDayVector);
        this.dateDayVector = dateDayVector;
    }

    public int getEpochDay(int rowId) {
        return this.dateDayVector.get(rowId);
    }

    @Override
    public Object get(int rowId) {
        if (isNullAt(rowId)) {
            return null;
        }
        return Date.valueOf(LocalDate.ofEpochDay(getEpochDay(rowId)));
    }
}
