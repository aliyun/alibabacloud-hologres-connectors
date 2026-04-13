package com.alibaba.hologres.client.copy.in.arrow.creator;

import org.apache.arrow.vector.DateDayVector;

import java.sql.Date;
import java.time.LocalDate;

/** Arrow column vector creator for date day. */
public class BaseArrowDateDayCreator extends AbstractArrowVectorCreator {

    protected final DateDayVector dateDayVector;

    public BaseArrowDateDayCreator(DateDayVector dateDayVector) {
        super(dateDayVector);
        this.dateDayVector = dateDayVector;
    }

    public void setEpochDay(int rowId, int value) {
        dateDayVector.setSafe(rowId, value);
    }

    @Override
    public void set(int rowId, Object value) {
        if (value == null) {
            dateDayVector.setNull(rowId);
        } else {
            Date date = (Date) value;
            LocalDate localDate = date.toLocalDate();
            int epochDay = (int) localDate.toEpochDay();
            setEpochDay(rowId, epochDay);
        }
    }
}
