package com.company.selectFromTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SelectColumnQuery implements ISelectColumn {
    private ConditionalOperator conditionalOperator = new ConditionalOperator();

    public void selectColumn(Map<String, List<String>> tableValue, String column, String whereCondition) {

        int sIndex = column.indexOf("select");
        String columnSubStr = column.substring(sIndex + 7);
        String[] columns = columnSubStr.split(",");

        int row = -1;
        for (Map.Entry<String, List<String>> eachCol : tableValue.entrySet()) {
            row = eachCol.getValue().size();
        }
        String[][] selectTableMatrix = new String[row][columns.length];
        int c = 1;

        if (whereCondition.isEmpty()) {
            for (String columnName : columns) {
                for (Map.Entry<String, List<String>> eachCol : tableValue.entrySet()) {
                    if (eachCol.getKey().equalsIgnoreCase(columnName.trim())) {
                        int r = 1;
                        for (String eachRow : eachCol.getValue()) {
                            selectTableMatrix[r - 1][c - 1] = eachRow;
                            r++;
                        }
                    }
                }
                c++;
            }
        } else {
            String[] expression = whereCondition.split(" ");
            List<Integer> rowIndexes = new ArrayList<>();
            for (Map.Entry<String, List<String>> eachCol : tableValue.entrySet()) {
                if (expression[0].equalsIgnoreCase(eachCol.getKey())) {
                    int index = 1;
                    for (String eachRow : eachCol.getValue()) {
                        if (conditionalOperator.doOperation(expression[1], eachRow, expression[2])) {
                            rowIndexes.add(index - 1);
                        }
                        index++;
                    }
                }
            }
            selectTableMatrix = new String[rowIndexes.size()][columns.length];
            for (String columnName : columns) {
                for (Map.Entry<String, List<String>> eachCol : tableValue.entrySet()) {
                    int r = 1;
                    int rIndex = 1;
                    for (String eachRow : eachCol.getValue()) {
                        if (eachCol.getKey().equalsIgnoreCase(columnName.trim())) {
                            if (rowIndexes.contains(r - 1)) {
                                selectTableMatrix[rIndex - 1][c - 1] = eachRow;
                                rIndex++;
                            }
                            r++;
                        }
                    }
                }
                c++;
            }

        }
        for (String[] tableMatrix : selectTableMatrix) {
            for (int cr = 0; cr < tableMatrix.length; cr++) {
                System.out.printf("%5s", tableMatrix[cr]);
            }
            System.out.println();
        }

    }
}
