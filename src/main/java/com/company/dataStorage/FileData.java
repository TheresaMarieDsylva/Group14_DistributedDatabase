package com.company.dataStorage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

public class FileData implements IFileData {

    @Override
    public Map<String, Map<String, Map<String, List<String>>>> readFromFile(String database, String tableName) throws FileNotFoundException {
        int c = 1;
        String[][] tableMatrix = new String[0][0];
        Map<String, List<String>> tableValue = new HashMap<>();
        Map<String, Map<String, List<String>>> table = new HashMap<>();
        Map<String, Map<String, Map<String, List<String>>>> db = new HashMap<>();
        try {
            CSVReader csvReader = setReaderInstance(database, tableName);
            List<String[]> allData = csvReader.readAll();
            String[] columnNames = allData.get(0);
            int col = columnNames.length;
            int row = allData.size() - 1;
            tableMatrix = new String[row][col];
            List<String> rows;

            int r = 1;
            for (int i = 1; i < allData.size(); i++) {
                String[] rowVal = allData.get(i);
                c = 1;
                for (String cell : rowVal) {
                    tableMatrix[r - 1][c - 1] = cell;
                    c++;
                }
                r++;
            }

            for (int ro = 0; ro < tableMatrix.length; ro++) {
                for (int co = 0; co < tableMatrix[ro].length; co++) {
                    rows = tableValue.get(columnNames[co]);
                    if (null == rows) {
                        rows = new ArrayList<>();
                    }
                    rows.add(tableMatrix[ro][co]);
                    tableValue.put(columnNames[co], rows);
                }
            }

            table.put(tableName, tableValue);
            db.put(database, table);

        } catch (Exception ex) {
            ex.getMessage();
        }

        return db;
    }

    @Override
    public void writeToFile(Map<String, Map<String, Map<String, List<String>>>> dataStructure) {
        try {
            System.out.println("Start writing to csv file........");
            int row = 0;
            int col = 0;
            String[][] selectTableMatrix = new String[0][0];
            int c = 1;
            CSVWriter csvWriter = null;

            List<String[]> fileArray = new ArrayList<>();
            Set<String> columnNames = new LinkedHashSet<>();

            for (Map.Entry<String, Map<String, Map<String, List<String>>>> database : dataStructure.entrySet()) {
                for (Map.Entry<String, Map<String, List<String>>> table : database.getValue().entrySet()) {
                    csvWriter = setWriterInstance(database.getKey(), table.getKey());
                    col = table.getValue().size();
                    Map.Entry<String, List<String>> entry = table.getValue().entrySet().iterator().next();
                    row = entry.getValue().size();
                    selectTableMatrix = new String[row][col];
                    for (Map.Entry<String, List<String>> eachCol : table.getValue().entrySet()) {
                        columnNames.add(eachCol.getKey());
                        int r = 1;
                        for (String eachRow : eachCol.getValue()) {
                            selectTableMatrix[r - 1][c - 1] = eachRow;
                            r++;
                        }
                        c++;
                    }
                    String[] data = new String[columnNames.size()];
                    int count = 0;
                    for (String columnName : columnNames) {
                        data[count] = columnName;
                        count++;
                    }
                    fileArray.add(data);

                    for (String[] tableMatrix : selectTableMatrix) {
                        fileArray.add(tableMatrix);
                    }
                    csvWriter.writeAll(fileArray);
                }
            }
            try {
                csvWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Finished writing into csv file..........");
    }

    public boolean writePrimaryKeysToFile(Map<String, Map<String, String>> primaryData) throws Exception {
        boolean result = false;
        String primaryKeyText = "primaryKeyMeta";

        for (Map.Entry<String, Map<String, String>> database : primaryData.entrySet()) {
            String filePath = database.getKey() + "/" + primaryKeyText + ".txt";
            File file = new File(filePath);
            FileWriter outputFile = new FileWriter(file, true);
            for (Map.Entry<String, String> eachRecord : database.getValue().entrySet()) {
                if(!checkIfRecordExists(database.getKey(), eachRecord.getKey() )) {
                    outputFile.append(eachRecord.getKey()).append(" ").append(eachRecord.getValue()).append("\n");
                }
            }
            try {
                outputFile.close();
                result = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Override
    public Map<String, Map<String, String>> readPrimaryKeyMeta(String currentDatabase) throws Exception {
        Map<String, Map<String, String>> primaryKey = new HashMap<>();
        String primaryKeyText = "primaryKeyMeta";
        try {
            String filePath = currentDatabase + "/" + primaryKeyText + ".txt";
            File file = new File(filePath);
            FileReader fileReader = new FileReader(file);
            BufferedReader reader = new BufferedReader(fileReader);
            String line = reader.readLine();
            Map<String, String> record = new HashMap<>();
            while (line != null && !line.isEmpty()) {
                if (line.length() > 0) {
                    String[] eachRecord = line.split(" ");
                    record.put(eachRecord[0], eachRecord[1]);
                    line = reader.readLine();
                }
            }
            primaryKey.put(currentDatabase, record);
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return primaryKey;
    }

    private boolean checkIfRecordExists(String database, String table) throws Exception {
        Map<String, String> existingTable = readPrimaryKeyMeta(database).get(database);
        if (null != existingTable) {
            return existingTable.containsKey(table);
        }
        return false;
    }


    private CSVWriter setWriterInstance(String database, String tableName) throws IOException {
        String filePath = database + "/" + tableName + ".csv";
        File file = new File(filePath);
        FileWriter outputFile = new FileWriter(file);
        CSVWriter writer = new CSVWriter(outputFile, '|', CSVWriter.NO_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        return writer;
    }

    private CSVReader setReaderInstance(String database, String tableName) throws FileNotFoundException {
        String filePath = database + "/" + tableName + ".csv";
        File file = new File(filePath);
        FileReader fileReader = new FileReader(file);
        CSVParser parser = new CSVParserBuilder()
                .withSeparator('|')
                .withIgnoreQuotations(true)
                .build();

        CSVReader csvReader = new CSVReaderBuilder(fileReader).withSkipLines(0)
                .withCSVParser(parser)
                .build();

        return csvReader;
    }
    
    private CSVWriter setForeignKeyWriterInstance(String database) throws IOException {
        String filePath = database + "/foreignKeys.csv";
        File file = new File(filePath);
        FileWriter outputFile = new FileWriter(file);
        CSVWriter writer = new CSVWriter(outputFile, '#', CSVWriter.NO_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        return writer;
    }
    
    private CSVReader setForeignKeyReaderInstance(String database) throws FileNotFoundException {
        String filePath = database + "/foreignKeys.csv";
        File file = new File(filePath);
        FileReader fileReader = new FileReader(file);
        //CSVParser parser = new CSVParserBuilder().build();

        CSVReader csvReader = new CSVReaderBuilder(fileReader).withSkipLines(0)
                .build();

        return csvReader;
    }

	@Override
	public void writeForeignKeyToFile(Map<String, Map<String, List<String>>> foreignKeyData) {
		try {
            System.out.println("Start writing foreign key to csv file........");
            CSVWriter csvWriter = null;

            List<String[]> fileArray = new ArrayList<>();

            for (Map.Entry<String, Map<String, List<String>>> database : foreignKeyData.entrySet()) {
                for (Map.Entry<String, List<String>> table : database.getValue().entrySet()) {
                    
                	csvWriter = setForeignKeyWriterInstance(database.getKey());
                	
                	String[] tempForeignKey = new String[1];
                	String temp = "";
                	List<String> tempList = table.getValue();
                	
                	temp+=database.getKey()+" | "+table.getKey()+" | ";
                	for(int i=0; i<tempList.size(); i++) {
                		temp+=" $ "+tempList.get(i);
                	}
                	
                	tempForeignKey[0] = temp;
                	
                	fileArray.add(tempForeignKey);
                    
                    
                }
            }
            
            csvWriter.writeAll(fileArray);
            try {
                csvWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Finished writing foreign key into csv file..........");
	}
	
	 @Override
    public Map<String, Map<String, List<String>>> readForeignKeysFromFile(String database) throws FileNotFoundException {
        Map<String, List<String>> tableValue = new HashMap<>();
        Map<String, Map<String, List<String>>> dbForeignKeyData = new HashMap<>();
        
        try {
            CSVReader csvReader = setForeignKeyReaderInstance(database);
            List<String[]> allData = csvReader.readAll();
            
            //tableValue = dbForeignKeyData.get(database);
            
            for(int i=0; i<allData.size(); i++) {
            	String[] splitData = allData.get(i);
            	String[] tempData = splitData[0].split("\\|");
            	String[] colData = tempData[2].split("\\$");
            	List<String> tempList = new ArrayList<>();
            	tempList.add(colData[1].trim());
            	tempList.add(colData[2].trim());
            	tempList.add(colData[3].trim());
            	
            	tableValue.put(tempData[1].trim(), tempList);
            	
            	dbForeignKeyData.put(database, tableValue);
            }

        } catch (Exception ex) {
            ex.getMessage();
        }

        return dbForeignKeyData;
    }

	@Override
	public List<String> readSQLDumpFromFile(String currentDatabase) {
		Path path = Paths.get("SQLDump\\SQLDump.sql");
		List<String> tempSQLDumpData = new ArrayList<>();
		try {
			tempSQLDumpData = Files.readAllLines(path, Charset.defaultCharset());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return tempSQLDumpData;
	}

    @Override
    public Map<String, Map<String, Map<String, List<String>>>> readRemoteFile(List<String> fileLines, String tableName, String remoteInstance) {
        Map<String, List<String>> tableValue = new HashMap<>();
        Map<String, Map<String, List<String>>> table = new HashMap<>();
        Map<String, Map<String, Map<String, List<String>>>> db = new HashMap<>();
        int c = 1;
        String[][] tableMatrix = new String[0][0];
        try {
            String[] columnNames = fileLines.get(0).replace("|", ",").split(",");
            int col = columnNames.length;
            int row = fileLines.size() - 1;
            tableMatrix = new String[row][col];
            List<String> rows;

            int r = 1;
            for (int i = 1; i < fileLines.size(); i++) {
                String[] rowVal = fileLines.get(i).replace("|", ",").split(",");
                c = 1;
                for (String cell : rowVal) {
                    tableMatrix[r - 1][c - 1] = cell;
                    c++;
                }
                r++;
            }

            for (int ro = 0; ro < tableMatrix.length; ro++) {
                for (int co = 0; co < tableMatrix[ro].length; co++) {
                    rows = tableValue.get(columnNames[co]);
                    if (null == rows) {
                        rows = new ArrayList<>();
                    }
                    rows.add(tableMatrix[ro][co]);
                    tableValue.put(columnNames[co], rows);
                }
            }

            table.put(tableName, tableValue);
            db.put(remoteInstance, table);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return db;
    }

    public String writeRemoteFile (Map<String, Map<String, Map<String, List<String>>>> dataStructure) {
        List<String> fileLines = new ArrayList<>();
        StringBuilder stringBuilder = new StringBuilder();
        try {
            int row = 0;
            int col = 0;
            String[][] selectTableMatrix = new String[0][0];
            int c = 1;

            List<String[]> fileArray = new ArrayList<>();
            Set<String> columnNames = new LinkedHashSet<>();

            for (Map.Entry<String, Map<String, Map<String, List<String>>>> database : dataStructure.entrySet()) {
                for (Map.Entry<String, Map<String, List<String>>> table : database.getValue().entrySet()) {
                    col = table.getValue().size();
                    for (Map.Entry<String, List<String>> eachCol : table.getValue().entrySet()) {
                        if (row < eachCol.getValue().size()) {
                            row = eachCol.getValue().size();
                        }
                    }

                    selectTableMatrix = new String[row][col];
                    for (Map.Entry<String, List<String>> eachCol : table.getValue().entrySet()) {
                        columnNames.add(eachCol.getKey());
                        int r = 1;
                        for (String eachRow : eachCol.getValue()) {
                            selectTableMatrix[r - 1][c - 1] = eachRow;
                            r++;
                        }
                        c++;
                    }
                    String[] data = new String[columnNames.size()];
                    int count = 0;
                    for (String columnName : columnNames) {
                        data[count] = columnName;
                        count++;
                    }
                    fileArray.add(data);

                    fileArray.addAll(Arrays.asList(selectTableMatrix));
                }
            }

            for (String[] eachRow : fileArray) {
                for(String eachCell : eachRow) {
                    fileLines.add(eachCell+"|");
                }
            }
            for(String each : fileLines) {
                stringBuilder.append(String.join("\n", each));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return stringBuilder.toString();
    }
}
