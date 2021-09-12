package com.company.user;

import java.util.HashMap;
import java.util.Scanner;
import java.util.logging.Level;

public class Main extends GlobalVariablesAndContsants{

	 

    public static void main(String[] args) throws Exception {
        System.out.println("Group 14 console app");
        Scanner sc = new Scanner(System.in);

        int choiceLogin = 0;

        System.out.println("===============================================================");
        System.out.println("Choose any of the below");
        System.out.println("1 => New User");
        System.out.println("2 => Login");
        System.out.println("===============================================================");

        choiceLogin = Integer.parseInt(sc.nextLine());

        System.out.print("Database instance local or remote=> ");
        String dbInstance = sc.nextLine();
        System.out.print("Username => ");
        String userName = sc.nextLine();
        System.out.print("Password => ");
        String password = sc.nextLine();
        switch (choiceLogin) {
            case 1:
                Authorization.userRegistration(dbInstance, userName, password);
                break;
            case 2: {
                currentDatabase = Authorization.login(dbInstance, userName, password);
                primaryData = readPrimaryKeyMeta(currentDatabase);
                break;
            }
            default:
                break;
        }

        foreignKeyData = fileData.readForeignKeysFromFile(LOCAL_INSTANCE);
        sqlDumpData = fileData.readSQLDumpFromFile(LOCAL_INSTANCE);
        boolean flag = true;
        System.out.println("===========Perform any sql operations===================");
        while (flag) {
            String input = "";
            Scanner scanner = new Scanner(System.in);
            try {
                while (scanner.hasNext()) {
                    input = scanner.nextLine();

                    if (input.equalsIgnoreCase("exit")) {
                        flag = false;
                        break;
                    }

                    String queryType = input.split(" ")[0].toLowerCase().replace(";", "");

                    if (queryType.equalsIgnoreCase("start")) {
                        System.out.println("Started transaction.....");
                        IS_TRANSACTION_ENABLED = true;
                    } else if (queryType.equalsIgnoreCase("commit") || (queryType.equalsIgnoreCase("rollback"))) {
                        IS_TRANSACTION_ENABLED = false;
                        if(queryType.equalsIgnoreCase("commit")) {
                            fileData.writeToFile(tableNameData);
                            System.out.println("Transaction commit....");
                            Log.log(Level.INFO, "Transaction committed....");
                        }
                        else {
                            tableNameData = new HashMap<>();
                            System.out.println("Transaction rollback....");
                            Log.log(Level.INFO, "Transaction rollback....");
                        }
                        System.out.println("Completed transaction....");
                    } else if (queryType.equalsIgnoreCase("create")) {
                        createTable.createTable(input, dbInstance, userName);
                    } else if (queryType.equalsIgnoreCase("insert")) {
                        insertIntoTable.insert(input);
                    } else if (queryType.equalsIgnoreCase("alter")) {
                        alterTable.alter(input);
                    } else if (queryType.equalsIgnoreCase("select")) {
                    	selectFromTable.select(input);
                    } else if (queryType.equalsIgnoreCase("drop")) {
                        dropTable.drop(input);
                    } else if (queryType.equalsIgnoreCase("update")) {
                        updateIntoTable.updateRecord(input);
                    } else if (queryType.equalsIgnoreCase("delete")) {
                        deleteFromTable.deleteRecord(input);
                    } else if (queryType.equalsIgnoreCase("erd")) {
                        generateERD.generateERD(tableNameData, primaryData, foreignKeyData, dataTypeStorage);
                    } else if (queryType.equalsIgnoreCase("sqldump")) {
                        sqlDump.createSQLDump(sqlDumpData);
                    }
                }
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
            finally{
            	scanner.close();
            	sc.close();
            }
        }
    }

    
}
