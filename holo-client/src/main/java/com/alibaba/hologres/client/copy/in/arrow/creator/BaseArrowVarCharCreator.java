package com.alibaba.hologres.client.copy.in.arrow.creator;

import org.apache.arrow.vector.VarCharVector;

import java.nio.charset.StandardCharsets;

/** Arrow column vector creator for var char. */
public class BaseArrowVarCharCreator extends AbstractArrowVectorCreator {

    protected final VarCharVector varCharVector;
    protected final int precision;

    public BaseArrowVarCharCreator(VarCharVector varCharVector) {
        this(varCharVector, -1);
    }

    public BaseArrowVarCharCreator(VarCharVector varCharVector, int precision) {
        super(varCharVector);
        this.varCharVector = varCharVector;
        this.precision = precision;
    }

    public void setBytes(int rowId, byte[] value) {
        varCharVector.setSafe(rowId, value);
    }

    public void set(int rowId, Object value) {
        if (value == null) {
            varCharVector.setNull(rowId);
        } else {
            String s = value.toString();
            if (precision > 0 && s.length() > precision) {
                s = s.substring(0, precision);
            }
            setBytes(rowId, s.getBytes(StandardCharsets.UTF_8));
        }
    }
}
