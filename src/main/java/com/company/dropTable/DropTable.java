package com.company.dropTable;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;

import com.company.gcpConfiguration.GCPRemoteDBReaderWriter;
import com.company.user.GddFile;
import com.company.user.GlobalVariablesAndContsants;

public class DropTable extends GlobalVariablesAndContsants implements IDropTable {

    public void drop(String dropQuery) {
        sqlDumpData.add(dropQuery);
        List<String> tableList = Arrays.asList(dropQuery.replace(";", "").split(" "));
        GddFile gdd = new GddFile();
        String isAvailable = gdd.getGddData(tableList.get(2));
        if (isAvailable != null) {
            String[] gddData = isAvailable.split(",");
            if (gddData[1].equalsIgnoreCase("remote")) {
                GCPRemoteDBReaderWriter gcpRemoteDBReaderWriter = new GCPRemoteDBReaderWriter();
                if (gcpRemoteDBReaderWriter.deleteGDDFile(gddData[1])) {
                    gdd.removeTableFromGDD(tableList.get(2));
                    Log.log(Level.INFO, "Dropped table " + tableList.get(2));
                    System.out.println("Table Dropped.");
                } else {
                    Log.log(Level.SEVERE, "Failed to drop the table " + tableList.get(2));
                }
            } else {
                String filepath = gddData[1] + "/" + gddData[2] + ".csv";
                File table = new File(filepath);
                if (table.delete()) {
                    gdd.removeTableFromGDD(tableList.get(2));
                    Log.log(Level.INFO, "Dropped table " + tableList.get(2));
                    System.out.println("Table Dropped.");
                } else {
                    Log.log(Level.SEVERE, "Failed to drop the table " + tableList.get(2));
                }
            }
        } else {
            Log.log(Level.INFO, "No table available with " + tableList.get(2));
            System.out.println("No Table " + tableList.get(2));
        }
    }
}
