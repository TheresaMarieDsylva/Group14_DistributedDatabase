package com.company.alterTable;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.company.user.GddFile;
import com.company.user.GlobalVariablesAndContsants;

public class AlterTable extends GlobalVariablesAndContsants implements IAlterTable {

    public void alter(String alterQuery) throws Exception {
        sqlDumpData.add(alterQuery);
        String aQuery = alterQuery.toLowerCase();
        boolean isQueryWithAdd = false;
        boolean isQueryWithDrop = false;
        String[] splitter = new String[0];
        if (aQuery.contains(" add ")) {
            splitter = aQuery.split(" add ");
            isQueryWithAdd = true;
        } else if (aQuery.contains(" drop ")) {
            splitter = aQuery.split(" drop ");
            isQueryWithDrop = true;
        }

        String[] alterQrr = splitter[0].split(" ");
        String queryTableName = alterQrr[2].trim();
        String[] columnArr = splitter[1].split(" ");
        String columnName = columnArr[0];
        GddFile gddFile = new GddFile();
        String gddData = gddFile.getGddData(queryTableName);
        String[] gddDataArr = gddData.split(",");
        Map<String, List<String>> columns = new LinkedHashMap<>();

        try {
            if (gddDataArr[1].equalsIgnoreCase(LOCAL_INSTANCE)) {
                tableNameData = readFromFile(queryTableName);
            }
            if (gddDataArr[1].equalsIgnoreCase(REMOTE_INSTANCE)) {
                List<String> fileLines = gcpRemoteDBReaderWriter.readFile(queryTableName + ".csv");
                tableNameData = readFromFile(fileLines, queryTableName, REMOTE_INSTANCE);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        if (isQueryWithAdd) {
            for (Map.Entry<String, Map<String, Map<String, List<String>>>> database : tableNameData.entrySet()) {
                for (Map.Entry<String, Map<String, List<String>>> table : database.getValue().entrySet()) {
                    if (table.getKey().equalsIgnoreCase(queryTableName)) {
                        columns = table.getValue();
                        columns.remove(columnName);
                        columns.put(columnName, new ArrayList<>());
                    }
                }
            }
        } else if (isQueryWithDrop) {
            for (Map.Entry<String, Map<String, Map<String, List<String>>>> database : tableNameData.entrySet()) {
                for (Map.Entry<String, Map<String, List<String>>> table : database.getValue().entrySet()) {
                    if (table.getKey().equalsIgnoreCase(queryTableName)) {
                        columns = table.getValue();
                        if (columns.containsKey(columnName)) {
                            columns.remove(columnName);
                        }
                    }
                }
            }
        }
        try {
            if (gddDataArr[1].equalsIgnoreCase(LOCAL_INSTANCE)) {
                writeToFile();
            }
            if (gddDataArr[1].equalsIgnoreCase(REMOTE_INSTANCE)) {
                String fileContent = writeRemoteFile();
                gcpRemoteDBReaderWriter.writeFile(queryTableName + ".csv", fileContent);
                Files.deleteIfExists(Paths.get("local\\" + queryTableName + ".csv"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(queryTableName + " table is altered!");
    }

}
