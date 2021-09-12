package com.company.createTable;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import com.company.user.GddFile;
import com.company.user.GlobalVariablesAndContsants;

public class CreateTable extends GlobalVariablesAndContsants implements ICreateTable {

    public void createTable(String createTableQuery, String dbInstance, String userName) {
        Log.log(Level.INFO, "Creating table started...");
        String gddTableData = createTableQuery.split(" ")[2];
        GddFile gddFile = new GddFile();
        String gddData = gddFile.getGddData(gddTableData);

        if (null == gddData) {
            sqlDumpData.add(createTableQuery);
            createTableQuery = removeNotNull(createTableQuery);
            boolean primaryKeyExist = false;
            Map<String, String> tempPrimaryMap = new HashMap<>();
            if (createTableQuery.toLowerCase().contains("primary key")) {
                tempPrimaryMap = fetchPrimaryKey(createTableQuery);
                primaryKeyExist = true;
                createTableQuery = removePrimaryKey(createTableQuery);
            }

            String[] qArr = createTableQuery.split(" ");

            Map<String, Map<String, List<String>>> tempTableNameData = null;

            if (null != tableNameData.get(currentDatabase)) {
                tempTableNameData = tableNameData.get(currentDatabase);
            } else {
                tempTableNameData = new HashMap<>();
            }
            Map<String, List<String>> tempColumnData = new HashMap<>();

            Map<String, Map<String, String>> tempTableWithTypeData = new HashMap<>();
            Map<String, String> tempTypeData = new HashMap<>();
            List<String> newTempArr = new ArrayList<String>();
            for (int i = 3; i < qArr.length; i++) {
                newTempArr.add(qArr[i]);
            }

            if (primaryKeyExist) {
                primaryData.put(currentDatabase, tempPrimaryMap);
            }

            boolean flag = true;
            boolean terminate = true;
            String typeSave;
            for (int j = 0; j < newTempArr.size(); j++) {
                if (flag && terminate && !newTempArr.get(j).trim().equals(",") && !newTempArr.get(j).trim().equals(")")
                        && !newTempArr.get(j).trim().equals("(")) {
                    String tempCol = extractDataFromString(newTempArr.get(j));

                    if (!tempCol.equalsIgnoreCase("CONSTRAINT")) {
                        // dataTypeStorage
                        typeSave = newTempArr.get(j + 1).replace(",", "");
                        tempTypeData.put(tempCol, typeSave);
                        tempColumnData.put(tempCol, new ArrayList<>());
                    } else {
                        Map<String, List<String>> tempTablesData = foreignKeyData.get(currentDatabase);
                        Log.log(Level.INFO, "Setting all foreign keys...");
                        if (null != tempTablesData) {
                            List<String> allForeignKeyData = new ArrayList<String>();
                            Map<String, List<String>> tempForeignKey = tempTablesData;
                            allForeignKeyData.add(extractDataFromString(newTempArr.get(j + 4)));
                            allForeignKeyData.add(extractDataFromString(newTempArr.get(j + 6)));
                            allForeignKeyData.add(extractDataFromString(newTempArr.get(j + 7)));
                            tempForeignKey.put(qArr[2], allForeignKeyData);

                            foreignKeyData.replace(currentDatabase, tempForeignKey);
                            terminate = false;
                        } else {
                            List<String> allForeignKeyData = new ArrayList<String>();
                            Map<String, List<String>> tempForeignKey = new HashMap<>();
                            allForeignKeyData.add(extractDataFromString(newTempArr.get(j + 4)));
                            allForeignKeyData.add(extractDataFromString(newTempArr.get(j + 6)));
                            allForeignKeyData.add(extractDataFromString(newTempArr.get(j + 7)));
                            tempForeignKey.put(qArr[2], allForeignKeyData);
                            foreignKeyData.put(currentDatabase, tempForeignKey);
                            terminate = false;
                        }
                    }
                    flag = false;
                } else {
                    flag = true;
                }
            }
            tempTableNameData.put(qArr[2], tempColumnData);
            tempTableWithTypeData.put(qArr[2], tempTypeData);
            tableNameData.put(currentDatabase, tempTableNameData);
            dataTypeStorage.put(currentDatabase, tempTableWithTypeData);
            try {
                writeToFile();
                if (dbInstance.equalsIgnoreCase(REMOTE_INSTANCE)) {
                    gcpRemoteDBReaderWriter.writeFile(qArr[2] + ".csv");
                    Files.deleteIfExists(Paths.get("local\\" + qArr[2] + ".csv"));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            gddFile.gddStore(userName, dbInstance, qArr[2]);
            Log.log(Level.INFO, "Table is created..." + qArr[2]);
            System.out.println("Created.");
        } else {
            Log.log(Level.SEVERE, "Table already exist...");
            System.out.println("Table already exist...Please check...");
        }
    }
}
