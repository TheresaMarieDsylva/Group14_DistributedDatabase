package com.company.user;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import com.company.alterTable.AlterTable;
import com.company.alterTable.IAlterTable;
import com.company.createTable.CreateTable;
import com.company.createTable.ICreateTable;
import com.company.dataStorage.FileData;
import com.company.dataStorage.IFileData;
import com.company.deleteFromTable.DeleteFromTable;
import com.company.deleteFromTable.IDeleteFromTable;
import com.company.dropTable.DropTable;
import com.company.dropTable.IDropTable;
import com.company.erd.GenerateERD;
import com.company.erd.IGenerateERD;
import com.company.gcpConfiguration.GCPRemoteDBReaderWriter;
import com.company.insertIntoTable.IInsertIntoTable;
import com.company.insertIntoTable.InsertIntoTable;
import com.company.logs.Logging;
import com.company.selectFromTable.ISelectFromTable;
import com.company.selectFromTable.SelectFromTable;
import com.company.sqldump.ISQLDump;
import com.company.sqldump.SQLDump;
import com.company.updateIntoTable.IUpdateIntoTable;
import com.company.updateIntoTable.UpdateIntoTable;

public class GlobalVariablesAndContsants {

    public static final String LOCAL_INSTANCE = "local";
    public static final String REMOTE_INSTANCE = "remote";
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
    public static ICreateTable createTable = new CreateTable();
    public static IAlterTable alterTable = new AlterTable();
    public static IInsertIntoTable insertIntoTable = new InsertIntoTable();
    public static IUpdateIntoTable updateIntoTable = new UpdateIntoTable();
    public static IDeleteFromTable deleteFromTable = new DeleteFromTable();
    public static IDropTable dropTable = new DropTable();
    public static ISelectFromTable selectFromTable = new SelectFromTable();
    public static boolean IS_TRANSACTION_ENABLED = false;
	
    protected static String extractDataFromString(String stringExtract) {
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


    protected static void writeToFile() throws Exception {
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

    protected static String writeRemoteFile() {
        if (tableNameData.size() > 0) {
            return fileData.writeRemoteFile(tableNameData);
        }
        return null;
    }

    protected static Map<String, Map<String, Map<String, List<String>>>> readFromFile(String tableName) throws Exception {
    	Log.log(Level.INFO, "Reading data from file...");
        return fileData.readFromFile(currentDatabase, tableName);
    }

    protected static Map<String, Map<String, Map<String, List<String>>>> readFromFile(List<String> file, String tableName, String remoteInstance) throws Exception {
        Log.log(Level.INFO, "Reading data from remote file...");
        return fileData.readRemoteFile(file, tableName, remoteInstance);
    }

    protected static Map<String, Map<String, String>> readPrimaryKeyMeta(String currentDatabase) throws Exception {
        return fileData.readPrimaryKeyMeta(currentDatabase);
    }

    protected static Map<String, String> fetchPrimaryKey(String createTableQuery) {
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

    protected static String removeNotNull(String createTableQuery) {
        createTableQuery = createTableQuery.replace("NOT NULL", "");
        createTableQuery = createTableQuery.replace("not null", "");
        createTableQuery = createTableQuery.replace("Not Null", "");
        createTableQuery = createTableQuery.replaceAll("[(0-9)]", "");
        createTableQuery = createTableQuery.replaceAll("  ", " ");
        createTableQuery = createTableQuery.replaceAll(";", "");
        return createTableQuery;
    }

    protected static String removePrimaryKey(String createTableQuery) {
        createTableQuery = createTableQuery.replace(" primary key", "");
        createTableQuery = createTableQuery.replace(" Primary Key", "");
        createTableQuery = createTableQuery.replace(" PRIMARY KEY", "");
        return createTableQuery;
    }

    protected static Map<String, Map<String, Map<String, List<String>>>> fetchTableData (String tableName, String[] gddDataArr) throws Exception {
        if (!IS_TRANSACTION_ENABLED) {
            if (gddDataArr[1].equalsIgnoreCase(LOCAL_INSTANCE)) {
                tableNameData = readFromFile(tableName);
            }
            if (gddDataArr[1].equalsIgnoreCase(REMOTE_INSTANCE)) {
                List<String> fileLines = gcpRemoteDBReaderWriter.readFile(tableName + ".csv");
                tableNameData = readFromFile(fileLines, tableName, REMOTE_INSTANCE);
            }
        } else {
            if(!(tableNameData.size() > 0)) {
                tableNameData = readFromFile(tableName);
            }
            else if (isTableStructureNotInTransaction(tableName)) {
                addTableDataToTransaction(tableName, gddDataArr[1]);
            }
        }
        return tableNameData;
    }

    protected static boolean isTableStructureNotInTransaction(String tableName) {
        boolean result = false;
        for (Map.Entry<String, Map<String, Map<String, List<String>>>> db : tableNameData.entrySet()) {
            for (Map.Entry<String, Map<String, List<String>>> tables : db.getValue().entrySet()) {
                if (!tables.getKey().equalsIgnoreCase(tableName)) {
                    result = true;
                }
            }
        }
        return result;
    }

    protected static void addTableDataToTransaction(String tableName, String dbInstance) throws Exception {
        Map<String, Map<String, Map<String, List<String>>>> dataStructure = readFromFile(tableName);
        Map<String, List<String>> table = new HashMap<>();

        for (Map.Entry<String, Map<String, Map<String, List<String>>>> database : dataStructure.entrySet()) {
            for (Map.Entry<String, Map<String, List<String>>> tables : database.getValue().entrySet()) {
                if (tables.getKey().equalsIgnoreCase(tableName)) {
                   table = tables.getValue();
                }
            }
        }

        Map<String, Map<String, List<String>>> tables = tableNameData.get(dbInstance);
        tables.put(tableName, table);
        tableNameData.put(dbInstance, tables);
    }
}
