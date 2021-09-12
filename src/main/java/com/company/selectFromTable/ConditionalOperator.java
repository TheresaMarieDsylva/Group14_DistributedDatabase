package com.company.selectFromTable;

public class ConditionalOperator {
    private final String EQUAL = "=";
    private final String NOT_EQUAL = "<>";
    private final String GREATER_THAN = ">";
    private final String GREATER_THAN_EQUAL = ">=";
    private final String LESS_THAN = "<";
    private final String LESS_THAN_EQUAL = "<=";


    public boolean doOperation(String operator, String eachRow, String operandValue) {
        if (operator.equalsIgnoreCase(EQUAL)) {
            return eachRow.equals(operandValue);
        }
        if (operator.equalsIgnoreCase(NOT_EQUAL)) {
            return !eachRow.equals(operandValue);
        }
        if (operator.equalsIgnoreCase(GREATER_THAN)) {
            return Integer.parseInt(eachRow) > Integer.parseInt(operandValue);
        }
        if (operator.equalsIgnoreCase(GREATER_THAN_EQUAL)) {
            return Integer.parseInt(eachRow) >= Integer.parseInt(operandValue);
        }
        if (operator.equalsIgnoreCase(LESS_THAN)) {
            return Integer.parseInt(eachRow) < Integer.parseInt(operandValue);
        }
        if (operator.equalsIgnoreCase(LESS_THAN_EQUAL)) {
            return Integer.parseInt(eachRow) <= Integer.parseInt(operandValue);
        }
        return false;
    }
}
