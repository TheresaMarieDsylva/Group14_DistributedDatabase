package com.company.deleteFromTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import com.company.user.GddFile;
import com.company.user.GlobalVariablesAndContsants;

public class DeleteFromTable extends GlobalVariablesAndContsants implements IDeleteFromTable{

	public void deleteRecord(String deleteQuery) {
        String[] gddDataArr;
    	Log.log(Level.INFO, "Deleting record from table...");
        int indexOfData = -1;

        String[] qArr = deleteQuery.split(" ");
        String tableName = qArr[2];
        Map<String, Map<String, List<String>>> tableData = new HashMap<>();
        GddFile gddFile = new GddFile();
        String gddData = gddFile.getGddData(tableName);
        gddDataArr = gddData.split(",");

        try {
            tableNameData = fetchTableData(tableName, gddDataArr);
		} catch (Exception e) {
			e.printStackTrace();
		}
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
            System.out.println("Record from table is now deleted..."+tableName);
        }
        try {
            if(!IS_TRANSACTION_ENABLED) {
                writeToFile();
            }
		} catch (Exception e) {
			e.printStackTrace();
		}

    	Log.log(Level.INFO, "Record from table is now deleted..."+tableName);
    }
	
}
