package com.company.updateIntoTable;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import com.company.user.GddFile;
import com.company.user.GlobalVariablesAndContsants;

public class UpdateIntoTable extends GlobalVariablesAndContsants implements IUpdateIntoTable {

	public void updateRecord(String updateQuery) {
        String[] gddDataArr;
    	Log.log(Level.INFO, "Updating table started...");
        String[] qArr = updateQuery.split(" ");
        String tableName = qArr[1];
        GddFile gddFile = new GddFile();
        String gddData = gddFile.getGddData(tableName);
        gddDataArr = gddData.split(",");
        try {
           tableNameData = fetchTableData(tableName, gddDataArr);
		} catch (Exception e) {
			e.printStackTrace();
		}
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
        try {
            if(!IS_TRANSACTION_ENABLED) {
                writeToFile();
            }
		} catch (Exception e) {
			e.printStackTrace();
		}
    	Log.log(Level.INFO, "Table is now updated..."+qArr[3]);
        System.out.println("Updated.");
    }
	
}
