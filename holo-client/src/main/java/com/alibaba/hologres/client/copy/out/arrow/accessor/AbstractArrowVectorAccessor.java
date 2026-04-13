package com.alibaba.hologres.client.copy.out.arrow.accessor;

import org.apache.arrow.vector.ValueVector;

/** Access arrow column vector through specific subclasses. */
public abstract class AbstractArrowVectorAccessor {

    private final ValueVector vector;

    public AbstractArrowVectorAccessor(ValueVector vector) {
        if (vector == null) {
            throw new IllegalArgumentException("the ValueVector is null!");
        }
        this.vector = vector;
    }

    public boolean isNullAt(int rowId) {
        return vector.isNull(rowId);
    }

    /**
     * Get the value of the underlying column vector at position {@code rowId}.
     *
     * @param rowId the position of the element.
     * @return the value of the element, we implement a set of BaseAccessor that returns java or
     *     java.sql Object.
     */
    public abstract Object get(int rowId);

    public final void close() {
        vector.close();
    }
}
