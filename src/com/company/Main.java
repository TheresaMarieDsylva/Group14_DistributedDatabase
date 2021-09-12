package com.company;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.regex.Pattern;

import com.company.dataStorage.FileData;
import com.company.dataStorage.IFileData;
import com.company.erd.GenerateERD;
import com.company.erd.IGenerateERD;
import com.company.select.ISelectColumn;
import com.company.select.ISelectStar;
import com.company.select.SelectColumnQuery;
import com.company.select.SelectStarQuery;
import com.company.sqldump.ISQLDump;
import com.company.sqldump.SQLDump;

public class Main {
    public static final String openBracketRegExp = "\\(";
    public static final String closeBracketRegExp = "\\)";
    public static final String commaRegExp = "\\,";
    public static Map<String, Map<String, Map<String, List<String>>>> tableNameData = new HashMap<>();
    public static Map<String, Map<String, String>> primaryData = new HashMap<>();
    public static Map<String, Map<String, List<String>>> foreignKeyData = new HashMap<>();
    public static Map<String, Map<String, Map<String, String>>> dataTypeStorage = new HashMap<>();
    public static List<String> sqlDumpData = new ArrayList<>();
    public static IFileData fileData = new FileData();
    public static String currentDatabase = "";
    public static ISQLDump sqlDump = new SQLDump();
    public static IGenerateERD generateERD = new GenerateERD();
    public static GCPRemoteDBReaderWriter gcpRemoteDBReaderWriter = new GCPRemoteDBReaderWriter();
    public static Logging Log = new Logging();

    public static void main(String[] args) throws Exception {
        System.out.println("Group 14 console app");
        Scanner sc = new Scanner(System.in);

        int choiceLogin = 0;

        System.out.println("===============================================================");
        System.out.println("Choose any of the below");
        System.out.println("1 => New User");
        System.out.println("2 => Login");
        System.out.println("===============================================================");

        choiceLogin = Integer.parseInt(sc.nextLine());

        System.out.print("Database instance => ");
        String dbInstance = sc.nextLine();
        System.out.print("Username => ");
        String userName = sc.nextLine();
        System.out.print("Password => ");
        String password = sc.nextLine();
        switch (choiceLogin) {
            case 1:
                Authorization.userRegistration(dbInstance, userName, password);
                break;
            case 2: {
                currentDatabase = Authorization.login(dbInstance, userName, password);
                primaryData = readPrimaryKeyMeta(currentDatabase);
                break;
            }
            default:
                break;
        }

        foreignKeyData = fileData.readForeignKeysFromFile(currentDatabase);
        sqlDumpData = fileData.readSQLDumpFromFile(currentDatabase);
        boolean flag = true;
        System.out.println("===========Perform any sql operations===================");
        while (flag) {
            String input = "";
            Scanner scanner = new Scanner(System.in);
            try {
                while (scanner.hasNext()) {
                    input = scanner.nextLine();

                    if (input.equalsIgnoreCase("exit")) {
                        flag = false;
                        break;
                    }

                    String queryType = input.split(" ")[0].toLowerCase();

                    if (queryType.equalsIgnoreCase("create")) {
                        createTable(input);
                    } else if (queryType.equalsIgnoreCase("insert")) {
                        insert(input);
                    } else if (queryType.equalsIgnoreCase("alter")) {
                        alter(input);
                    } else if (queryType.equalsIgnoreCase("select")) {
                        select(input);
                    } else if (queryType.equalsIgnoreCase("drop")) {
                        drop(input);
                    } else if (queryType.equalsIgnoreCase("update")) {
                        updateRecord(input);
                    } else if (queryType.equalsIgnoreCase("delete")) {
                        deleteRecord(input);
                    } else if (queryType.equalsIgnoreCase("erd")) {
                        generateERD.generateERD(tableNameData, primaryData, foreignKeyData, dataTypeStorage);
                    } else if (queryType.equalsIgnoreCase("sqldump")) {
                        sqlDump.createSQLDump(sqlDumpData);
                    }

                    //input = scanner.nextLine();
                }
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
            finally{
            	scanner.close();
            	sc.close();
            }
        }
    }

    private static void insert(String insertQuery) {
        try {
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

                tableNameData = readFromFile(tableName);

            } else {
                String[] tableArr = qArr[0].split(" into ");
                tableName = tableArr[1].trim();

                Map<String, List<String>> tableMap = new HashMap<>();

                tableNameData = readFromFile(tableName);

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
            writeToFile();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    private static void alter(String alterQuery) throws Exception {
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

        tableNameData = readFromFile(queryTableName);

        if (isQueryWithAdd) {
            for (Map.Entry<String, Map<String, Map<String, List<String>>>> database : tableNameData.entrySet()) {
                for (Map.Entry<String, Map<String, List<String>>> table : database.getValue().entrySet()) {
                    if (table.getKey().equalsIgnoreCase(queryTableName)) {
                        Map<String, List<String>> columns = table.getValue();
                        columns.put(columnName, new ArrayList<>());
                    }
                }
            }
        } else if (isQueryWithDrop) {
            for (Map.Entry<String, Map<String, Map<String, List<String>>>> database : tableNameData.entrySet()) {
                for (Map.Entry<String, Map<String, List<String>>> table : database.getValue().entrySet()) {
                    if (table.getKey().equalsIgnoreCase(queryTableName)) {
                        Map<String, List<String>> columns = table.getValue();
                        if (columns.containsKey(columnName)) {
                            columns.remove(columnName);
                        }
                    }
                }
            }
        }

        System.out.println(queryTableName+ " table is altered!");
    }

    private static void select(String selectQuery) throws Exception {
        String sQuery = selectQuery.toLowerCase().trim();

        String[] fromSeparator = sQuery.split("from");
        String queryTest = fromSeparator[0].trim();
        String whereTest = fromSeparator[1].trim();
        boolean isSelectQueryWithStar = queryTest.indexOf("*") > 0;
        boolean isSelectQueryWithColumn = queryTest.indexOf(",") > 0;
        boolean isSelectQueryWithWhere = whereTest.indexOf("where") > 0;

        String queryTableName = isSelectQueryWithWhere ?
                Pattern.compile("where").split(fromSeparator[1])[0].trim() :
                Pattern.compile(";").split(fromSeparator[1])[0].trim();

        String queryColumnName = isSelectQueryWithColumn ?
                Pattern.compile("from").split(fromSeparator[0])[0].trim() : "";

        String queryWhereCondition = isSelectQueryWithWhere ?
                Pattern.compile("where").split(fromSeparator[1])[1].replace(";", "").trim() : "";

        ISelectStar selectStarObj;
        ISelectColumn selectColumnObj;

        tableNameData = readFromFile(queryTableName);

        for (Map.Entry<String, Map<String, Map<String, List<String>>>> database : tableNameData.entrySet()) {
            for (Map.Entry<String, Map<String, List<String>>> table : database.getValue().entrySet()) {
                if (table.getKey().equalsIgnoreCase(queryTableName)) {
                    Map<String, List<String>> tableValue = table.getValue();
                    if (isSelectQueryWithStar && !isSelectQueryWithWhere) {
                        selectStarObj = new SelectStarQuery();
                        selectStarObj.select(tableValue, "", "");
                    } else if (isSelectQueryWithStar) {
                        selectStarObj = new SelectStarQuery();
                        selectStarObj.select(tableValue, queryColumnName, queryWhereCondition);
                    } else if (isSelectQueryWithColumn && !isSelectQueryWithWhere) {
                        selectColumnObj = new SelectColumnQuery();
                        selectColumnObj.selectColumn(tableValue, queryColumnName, "");
                    } else if (isSelectQueryWithColumn) {
                        selectColumnObj = new SelectColumnQuery();
                        selectColumnObj.selectColumn(tableValue, queryColumnName, queryWhereCondition);
                    }
                }
            }
        }
    }

    private static void createTable(String createTableQuery) throws Exception {
    	Log.log(Level.INFO, "Creating table started...");
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
            if (flag && terminate && !newTempArr.get(j).trim().equals(",") && !newTempArr.get(j).trim().equals(")") && !newTempArr.get(j).trim().equals("(")) {
                String tempCol = extractDataFromString(newTempArr.get(j));

                if (!tempCol.equalsIgnoreCase("CONSTRAINT")) {
                    //dataTypeStorage
                    typeSave = newTempArr.get(j + 1).replace(",", "");
                    tempTypeData.put(tempCol, typeSave);
                    tempColumnData.put(tempCol, new ArrayList<>());
                } else {
                	Map<String, List<String>> tempTablesData = foreignKeyData.get(currentDatabase);
                	Log.log(Level.INFO, "Setting all foreign keys...");
                	if(null != tempTablesData) {
                		List<String> allForeignKeyData = new ArrayList<String>();
                        Map<String, List<String>> tempForeignKey = tempTablesData;
                        allForeignKeyData.add(extractDataFromString(newTempArr.get(j + 4)));
                        allForeignKeyData.add(extractDataFromString(newTempArr.get(j + 6)));
                        allForeignKeyData.add(extractDataFromString(newTempArr.get(j + 7)));
                        tempForeignKey.put(qArr[2], allForeignKeyData);
                        
                        foreignKeyData.replace(currentDatabase, tempForeignKey);
                        terminate = false;
                	}
                	else {
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
        writeToFile();
    	Log.log(Level.INFO, "Table is created..."+qArr[2]);
        System.out.println("Created.");
    }

    private static void drop(String dropQuery) {
        sqlDumpData.add(dropQuery);
        List<String> tableList = Arrays.asList(dropQuery.replace(";", "").split(" "));

        GddFile gdd = new GddFile();
        String isAvailable = gdd.getGddData(tableList.get(2));
        if (isAvailable != null) {
            String[] gddData = isAvailable.split(",");
            String filepath = gddData[1] + "/" + gddData[2] + ".csv";
            File table = new File(filepath);
            if (table.delete()) {
                gdd.removeTableFromGDD(tableList.get(2));
                Log.log(Level.INFO, "Deleted table " + tableList.get(2));
            } else {
                Log.log(Level.SEVERE, "Failed to deleted table " + tableList.get(2));
            }
        } else {
            Log.log(Level.INFO, "No table available with " + tableList.get(2));
        }
    }

    private static void updateRecord(String updateQuery) throws Exception {
    	Log.log(Level.INFO, "Updating table started...");
        String[] qArr = updateQuery.split(" ");
        String tableName = qArr[1];
        tableNameData = readFromFile(tableName);
        Map<String, Map<String, List<String>>> tempTablesData = tableNameData.get(currentDatabase);

        Map<String, List<String>> tempColumnsData = tempTablesData.get(tableName);

        tempColumnsData.forEach((k, v) -> {

            int indexOfData = -1;

            if (k.equalsIgnoreCase(qArr[7])) {
                String compareTo = extractDataFromString(qArr[9]);
                if (v.contains(compareTo.toLowerCase())) {
                    indexOfData = v.indexOf(compareTo.toLowerCase());
                }
            }

            if (0 <= indexOfData) {
                List<String> tempList = tempColumnsData.get(qArr[3]);
                tempList.set(indexOfData, extractDataFromString(qArr[5]));
                tempColumnsData.replace(qArr[3], tempList);
            }
        });

        tempTablesData.replace(qArr[3], tempColumnsData);
        tableNameData.replace(currentDatabase, tempTablesData);
        writeToFile();
    	Log.log(Level.INFO, "Table is now updated..."+qArr[3]);
        System.out.println("Updated.");
    }

    private static void deleteRecord(String deleteQuery) throws Exception {
    	Log.log(Level.INFO, "Deleting record from table...");
        //String deleteQuery = "DELETE FROM student WHERE name = 'Jonna';";
        int indexOfData = -1;

        String[] qArr = deleteQuery.split(" ");
        String tableName = qArr[2];
        Map<String, Map<String, List<String>>> tableData = new HashMap<>();

        tableNameData = readFromFile(tableName);
        tableData = tableNameData.get(currentDatabase);

        Map<String, List<String>> tempColumnData = new HashMap<>();
        tempColumnData = tableData.get(tableName);

        // if no conditions given, then delete entire tables' data
        if (qArr.length < 4) {
            tableData.replace(qArr[2], new HashMap<String, List<String>>());
            tableNameData.replace(currentDatabase, tableData);
        }

        for (Map.Entry<String, List<String>> entry : tempColumnData.entrySet()) {
            if (entry.getKey().equals(qArr[4])) {
                indexOfData = entry.getValue().indexOf(extractDataFromString(qArr[6]));
            }
        }

        if (indexOfData != -1) {
            for (Map.Entry<String, List<String>> entry : tempColumnData.entrySet()) {
                entry.getValue().remove(indexOfData);
            }
        }
        writeToFile();
    	Log.log(Level.INFO, "Record from table is now deleted..."+tableName);
    }

    private static String extractDataFromString(String stringExtract) {
    	Log.log(Level.INFO, "Extracting the string data...");
        stringExtract = stringExtract.replaceAll(";", "");
        int length = stringExtract.length();
        String finalString = stringExtract;
        if (stringExtract.startsWith("'") | stringExtract.startsWith("(") | stringExtract.startsWith(")") | stringExtract.startsWith("=")) {
            finalString = stringExtract.substring(1, length);
        }

        if (null != finalString && finalString.endsWith("'") | finalString.endsWith("(") | finalString.endsWith(")") | finalString.endsWith("=")) {
            finalString = finalString.substring(0, length - 2);
        }
        return finalString;
    }


    private static void writeToFile() throws Exception {
    	Log.log(Level.INFO, "Writing data to file...");
        if (tableNameData.size() > 0) {
            fileData.writeToFile(tableNameData);
        }
        else {
            throw new Exception("Cannot write to file as it is empty");
        }
        if (primaryData.size() > 0) {
            fileData.writePrimaryKeysToFile(primaryData);
        }
        else {
            throw new Exception("Cannot write to file as it is empty");
        }
        
        if(foreignKeyData.size()>0) {
        	fileData.writeForeignKeyToFile(foreignKeyData);
        } else {
            throw new Exception("Cannot write any foreign file as it is empty");
        }
    }

    private static Map<String, Map<String, Map<String, List<String>>>> readFromFile(String tableName) throws Exception {
    	Log.log(Level.INFO, "Reading data from file...");
        return fileData.readFromFile(currentDatabase, tableName);
    }

    private static Map<String, Map<String, String>> readPrimaryKeyMeta(String currentDatabase) throws Exception {
        return fileData.readPrimaryKeyMeta(currentDatabase);
    }

    private static Map<String, String> fetchPrimaryKey(String createTableQuery) {
    	Log.log(Level.INFO, "Fetching primary keys...");
        Map<String, String> tempPrimaryMap = new HashMap<>();
        if (createTableQuery.toLowerCase().contains("primary key")) {
            String[] fetchingKey = createTableQuery.toLowerCase().split("primary key");
            String[] allLeftSideData = fetchingKey[0].split(" ");
            int size = allLeftSideData.length;
            tempPrimaryMap.put(allLeftSideData[2], allLeftSideData[size - 2]);
        }
        return tempPrimaryMap;
    }

    private static String removeNotNull(String createTableQuery) {
        createTableQuery = createTableQuery.replace("NOT NULL", "");
        createTableQuery = createTableQuery.replace("not null", "");
        createTableQuery = createTableQuery.replace("Not Null", "");
        createTableQuery = createTableQuery.replaceAll("[(0-9)]", "");
        createTableQuery = createTableQuery.replaceAll("  ", " ");
        createTableQuery = createTableQuery.replaceAll(";", "");
        return createTableQuery;
    }

    private static String removePrimaryKey(String createTableQuery) {
        createTableQuery = createTableQuery.replace(" primary key", "");
        createTableQuery = createTableQuery.replace(" Primary Key", "");
        createTableQuery = createTableQuery.replace(" PRIMARY KEY", "");
        return createTableQuery;
    }
}
