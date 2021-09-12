package com.company.user;

import java.io.*;

public class GddFile {
    public boolean gddStore(String userName, String DB, String tableName){
        try {
            String path = "gdd.csv";
            File file = new File(path);
            FileWriter outputFile = new FileWriter(file,true);
            outputFile.append(userName);
            outputFile.append(",");
            outputFile.append(DB);
            outputFile.append(",");
            outputFile.append(tableName);
            outputFile.append("\n");
            outputFile.close();
            return true;
        }
        catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }
    public String getGddData(String tableName)
    {
        try{
            String path = "gdd.csv";
            File file = new File(path);
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = reader.readLine();
            while((line != null)) {
                String[] word = line.split(",");
                for(String text : word) {
                    if (text.equalsIgnoreCase(tableName.trim()))
                    {
                        return line;
                    }
                }
                System.out.println();
                line = reader.readLine();
            }
            reader.close();
            return null;
        }
        catch (Exception e){
            return "exception while getting data";
        }
    }
    public void removeTableFromGDD(String tableName) {
        try {
            String path = "gdd.csv";
            File file = new File(path);
            File tempFile = new File("temp.csv");
            BufferedReader reader = new BufferedReader(new FileReader(file));
            BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile,true));
            PrintWriter printWriter = new PrintWriter(writer);
            String line = reader.readLine();
            while ((line != null)) {
                String[] word = line.split(",");
                if (!word[2].replaceAll("[^a-zA-Z0-9]", "").equalsIgnoreCase(tableName)) {
                    System.out.println(line);
                    printWriter.println(line);
                }
                line = reader.readLine();
            }
            printWriter.flush();
            printWriter.close();
            writer.close();
            reader.close();
            file.delete();
            File newFile = new File(path);
            tempFile.renameTo(newFile);
        } catch (Exception e) {

        }
    }
}
