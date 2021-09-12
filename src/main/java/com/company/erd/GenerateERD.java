package com.company.erd;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;

public class GenerateERD implements IGenerateERD  {

	@Override
	public void generateERD(Map<String, Map<String, Map<String, List<String>>>> tableNameData,
			Map<String, Map<String, String>> primaryData, Map<String, Map<String, List<String>>> foreignKeyData,
			Map<String, Map<String, Map<String, String>>> dataTypeStorage) 
	{
		try {
			Path filePath = Paths.get("GeneratedERD\\ERD.txt");
			if (Files.notExists(filePath)) {
                Files.createFile(filePath);
            } else {
            	Files.delete(filePath);
            	Files.createFile(filePath);
            }
        	StringBuilder sb = new StringBuilder();
        	
        	
        	tableNameData.forEach( (k,v)->{
        		Map<String, List<String>> fkData = foreignKeyData.get(k);
        		
        		v.forEach((key, value)->{
        			String tableColumn = "";
            		String foreignKeyTable = "";
            		String foreignKeyColumn = "";
            		List<String> tempList = null;
            		
            		if(null != fkData) {
            			tempList = fkData.get(key);
            		}
            		
        			if(null != tempList) {
            			tableColumn = tempList.get(0);
            			foreignKeyTable = tempList.get(1);
            			foreignKeyColumn = tempList.get(2);
            		}
            		
            		if(!tableColumn.contentEquals("")) {
            			sb.append(key+"  ("+tableColumn+")--*----------REFERENCES------------1--> "+foreignKeyTable+ "  ("+foreignKeyColumn+")");
    					sb.append("\n");
            		}
            		else {
            			sb.append(key);
            			sb.append("\n");
            		}
        		});
        		
					
			});
            Files.write(filePath, sb.toString().getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
			
			System.out.println("Successfully created ERD diagram.");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
}
