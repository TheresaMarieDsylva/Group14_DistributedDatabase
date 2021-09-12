package com.company.selectFromTable;

import java.util.*;

public class SelectStarQuery implements ISelectStar {
    private ConditionalOperator conditionalOperator = new ConditionalOperator();

    @Override
    public void select(Map<String, List<String>> tableValue, String column, String whereCondition) {
        int col = tableValue.size();
        int row = -1;
        for (Map.Entry<String, List<String>> eachCol : tableValue.entrySet()) {
            row = eachCol.getValue().size();
        }
        String[][] selectTableMatrix = new String[row][col];
        int c = 1;

        if (column.isEmpty() && whereCondition.isEmpty())
        {
            for (Map.Entry<String, List<String>> eachCol : tableValue.entrySet()) {
                int r = 1;
                for (String eachRow : eachCol.getValue()) {
                    selectTableMatrix[r - 1][c - 1] = eachRow;
                    r++;
                }
                c++;
            }
        }
        else {
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
            selectTableMatrix = new String[rowIndexes.size()][col];

            for (Map.Entry<String, List<String>> eachCol : tableValue.entrySet()) {
                int r = 1; int rIndex = 1;
                for (String eachRow : eachCol.getValue()) {
                    if (rowIndexes.contains(r - 1)) {
                        selectTableMatrix[rIndex - 1][c - 1] = eachRow;
                        rIndex++;
                    }
                    r++;
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
