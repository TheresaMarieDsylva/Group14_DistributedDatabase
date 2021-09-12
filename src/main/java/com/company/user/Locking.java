package com.company.user;

import java.io.*;

public class Locking {
     public boolean setLock(String tableName) {
        try {
            String path = "lock.txt";
            File file = new File(path);
            FileWriter outputFile = new FileWriter(file, true);
            outputFile.append(tableName);
            outputFile.append("\n");
            outputFile.close();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
     }
     public boolean getLock(String tableName)
     {
         try {
             String path = "lock.txt";
             File file = new File(path);
             BufferedReader reader = new BufferedReader(new FileReader(file));
             String line = reader.readLine();
             while ((line != null)) {
                   if (line.contains(tableName)) {
                       return true;
                    }
             }
             reader.close();
             return false;
            } catch (Exception e) {
              e.printStackTrace();
              return false;
         }
     }
     public void removeLock (String tableName){
         try {
             String path = "lock.txt";
             File file = new File(path);
             File tempFile = new File("locktemp.txt");
             BufferedReader reader = new BufferedReader(new FileReader(file));
             BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile, true));
             PrintWriter printWriter = new PrintWriter(writer);
             String line = reader.readLine();
             while ((line != null)) {
               String[] word = line.split(",");
                  if (!word[2].replaceAll("[^a-zA-Z0-9]", "").equalsIgnoreCase(tableName)) {
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
              e.printStackTrace();
         }
     }   
}
