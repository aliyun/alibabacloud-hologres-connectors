package com.alibaba.hologres.client.model.checkandput;

/** 用于checkAndPut. */
public enum CheckCompareOp {
    /** less than. */
    LESS("<"),
    /** less than or equal to. */
    LESS_OR_EQUAL("<="),
    /** equals. */
    EQUAL("="),
    /** not equal. */
    NOT_EQUAL("<>"),
    /** is null. */
    IS_NULL("is null"),
    /** is not null. */
    IS_NOT_NULL("is not null"),
    /** greater than or equal to. */
    GREATER_OR_EQUAL(">="),
    /** greater than. */
    GREATER(">");

    private final String operatorString;

    CheckCompareOp(String operatorString) {
        this.operatorString = operatorString;
    }

    public String getOperatorString() {
        return operatorString;
    }
}
