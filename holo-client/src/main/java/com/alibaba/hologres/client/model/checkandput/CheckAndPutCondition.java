package com.alibaba.hologres.client.model.checkandput;

import com.alibaba.hologres.client.model.Column;

import java.io.Serializable;
import java.util.Objects;

/**
 * CheckAndPutCondition.
 */
public class CheckAndPutCondition implements Serializable {

	private final String checkColumnName;
	private Column checkColumn;
	private Object checkValue;
	private CheckCompareOp checkOp;
	/**
	 * postgres中，null值和任何值比较返回都是false，因此如果我们希望更新原有的null，需要进行 coalesce(old.column1 nullValue).
	 */
	private Object nullValue;

	public CheckAndPutCondition(String checkColumnName, CheckCompareOp checkOp, Object checkValue, Object nullValue) {
		this.checkColumnName = checkColumnName;
		this.checkOp = checkOp;
		this.checkValue = checkValue;
		this.nullValue = nullValue;
	}

	public CheckAndPutCondition(Column checkColumn, CheckCompareOp checkOp, Object checkValue, Object nullValue) {
		this.checkColumn = checkColumn;
		this.checkColumnName = checkColumn.getName();
		this.checkOp = checkOp;
		this.checkValue = checkValue;
		this.nullValue = nullValue;
	}

	public Object getCheckValue() {
		return checkValue;
	}

	public Object getNullValue() {
		return nullValue;
	}

	public void setNullValue(Object nullValue) {
		this.nullValue = nullValue;
	}

	public Column getCheckColumn() {
		return checkColumn;
	}

	public String getCheckColumnName() {
		return checkColumnName;
	}

	public CheckCompareOp getCheckOp() {
		return checkOp;
	}

	public void setCheckOp(CheckCompareOp checkOp) {
		this.checkOp = checkOp;
	}

	public void setCheckValue(Object checkValue) {
		this.checkValue = checkValue;
	}

	public void setCheckColumn(Column checkColumn) {
		this.checkColumn = checkColumn;
	}

	/**
	 * @return boolean
	 *  true: new value check with old value.
	 *  false: old value is null or is not null or no provide checkValue.
	 */
	public boolean isNewValueCheckWithOldValue() {
		return checkOp != CheckCompareOp.IS_NOT_NULL && checkOp != CheckCompareOp.IS_NULL
				&& Objects.isNull(checkValue);
	}

	public boolean isOldValueCheckWithNull() {
		return checkOp == CheckCompareOp.IS_NOT_NULL || checkOp == CheckCompareOp.IS_NULL;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
        if (o == null || getClass() != o.getClass()) {
			return false;
		}
		CheckAndPutCondition that = (CheckAndPutCondition) o;
		return checkColumn.equals(that.checkColumn)
				&& checkOp.equals(that.checkOp)
				&& Objects.equals(checkValue, that.checkValue)
				&& Objects.equals(nullValue, that.nullValue);
	}

	@Override
	public String toString() {
        return "CheckAndPutCondition{" +
                "checkColumn=" + checkColumn +
                ", checkOp=" + checkOp +
                ", checkValue=" + checkValue +
                ", nullValue=" + nullValue +
                '}';
    }
}
