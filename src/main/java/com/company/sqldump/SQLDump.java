package com.company.sqldump;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class SQLDump implements ISQLDump {

	@Override
	public void createSQLDump(List<String> sqlDumpData) {
		try {
			Path filePath = Paths.get("SQLDump\\SQLDump.sql");
			if (Files.notExists(filePath)) {
                Files.createFile(filePath);
            } else {
            	Files.delete(filePath);
            	Files.createFile(filePath);
            }
        	StringBuilder sb = new StringBuilder();
        	sqlDumpData.forEach( a->{
					sb.append(a);
					sb.append("\n");
			});
            Files.write(filePath, sb.toString().getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
			
			System.out.println("Successfully created dump file.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}