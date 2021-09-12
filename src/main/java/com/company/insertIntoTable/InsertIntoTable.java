package com.company.insertIntoTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import com.company.user.GddFile;
import com.company.user.GlobalVariablesAndContsants;

public class InsertIntoTable extends GlobalVariablesAndContsants implements IInsertIntoTable {
    String[] gddDataArr;
    public void insert(String insertQuery) {
        Log.log(Level.INFO, "Started inserting into table...");
        try {
            GddFile gddFile = new GddFile();
            String query = insertQuery.toLowerCase();
            String[] qArr = query.split("values");
            String iQueryTest = qArr[0].trim();
            boolean isQueryWithColumns = iQueryTest.indexOf("(") > 0;
            int size = -1;
            List<String> rows;
            String tableName = "";
            Map<String, List<String>> columnMap = new HashMap<>();
            Map<String, Map<String, List<String>>> table = new HashMap<>();
            List<String> columns = new ArrayList<>();
            String[] values = qArr[1].split(openBracketRegExp);
            if (isQueryWithColumns) {
                String[] columnExtract = qArr[0].split(openBracketRegExp);
                String[] columnArr = columnExtract[1].split(closeBracketRegExp);
                columns = Arrays.asList(columnArr[0].split(commaRegExp));

                String[] tableArr = columnExtract[0].split(" into ");
                tableName = tableArr[1].trim();

                String gddData = gddFile.getGddData(tableName);
                gddDataArr = gddData.split(",");

               tableNameData = fetchTableData(tableName, gddDataArr);

            } else {
                String[] tableArr = qArr[0].split(" into ");
                tableName = tableArr[1].trim();

                Map<String, List<String>> tableMap = new HashMap<>();

                String gddData = gddFile.getGddData(tableName);
                gddDataArr = gddData.split(",");

                tableNameData = fetchTableData(tableName, gddDataArr);

                for (Map.Entry<String, Map<String, Map<String, List<String>>>> db : tableNameData.entrySet()) {
                    for (Map.Entry<String, Map<String, List<String>>> tables : db.getValue().entrySet()) {
                        if (tables.getKey().equalsIgnoreCase(tableName)) {
                            tableMap = tables.getValue();
                        }
                    }
                }
                for (Map.Entry<String, List<String>> eachRecord : tableMap.entrySet()) {
                    columns.add(eachRecord.getKey());
                }
            }

            String pkColumn = "";
            if (primaryData.size() > 0) {
                Map<String, String> primaryKeyTable = primaryData.get(currentDatabase);
                for (Map.Entry<String, String> eachTable : primaryKeyTable.entrySet()) {
                    if (eachTable.getKey().equalsIgnoreCase(tableName)) {
                        pkColumn = eachTable.getValue().replace(")", "").replace(";", "");
                    }
                }
            }

            List<Integer> errorIds = new ArrayList<>();
            for (int j = 0; j < columns.size(); j++) {
                rows = new ArrayList<>();
                String columnName = columns.get(j).trim();
                for (int i = 1; i < values.length; i++) {
                    int r = i;
                    String[] eachVal = values[r].split(commaRegExp);
                    eachVal[j] = eachVal[j].replace(")", "");
                    eachVal[j] = eachVal[j].replace(";", "");
                    eachVal[j] = eachVal[j].replace("'", "");
                    String eachValue = eachVal[j].trim();
                    if (columnName.equalsIgnoreCase(pkColumn) && rows.contains(eachValue)) {
                        errorIds.add(i);
                        System.out.println("Duplicate key constraint on column " + columnName + ". Cannot insert value " + eachValue + " into table " + tableName);
                    } else if (!errorIds.contains(i)) {
                        rows.add(eachValue);
                    }
                    columnMap.put(columnName, rows);
                }
            }
            table.put(tableName, columnMap);
            tableNameData.put(currentDatabase, table);
            System.out.println(tableNameData.size() + " rows inserted successfully");
            try {
                if (!IS_TRANSACTION_ENABLED) {
                    writeToFile();
                    if (gddDataArr[1].equalsIgnoreCase(REMOTE_INSTANCE)) {
                        gcpRemoteDBReaderWriter.writeFile(tableName + ".csv");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        Log.log(Level.INFO, "Finished inserting into table...");
    }
}
