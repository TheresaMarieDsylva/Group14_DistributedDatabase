package com.company.selectFromTable;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.company.gcpConfiguration.GCPRemoteDBReaderWriter;
import com.company.user.GddFile;
import com.company.user.GlobalVariablesAndContsants;

public class SelectFromTable extends GlobalVariablesAndContsants implements ISelectFromTable{
    GCPRemoteDBReaderWriter gcpRemoteDBReaderWriter = new GCPRemoteDBReaderWriter();

	public void select(String selectQuery) {
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

        try {
            GddFile gddFile = new GddFile();
            String gddData = gddFile.getGddData(queryTableName);
            String[] gddDataArr = gddData.split(",");

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
}
