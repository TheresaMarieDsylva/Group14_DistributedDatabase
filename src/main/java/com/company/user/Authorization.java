package com.company.user;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

// https://www.javainterviewpoint.com/aes-256-encryption-and-decryption/ referred for AES.

public class Authorization {
    static List<String> databases = new ArrayList<>();

    public static List<String> getTotalDatabases() throws Exception {
        try {
            BufferedReader reader = new BufferedReader(new FileReader("userLogin.txt"));
            String line = reader.readLine();
            while (line != null) {
                if (line.length() > 0) {
                    String[] p = line.split(" ");
                    databases.add(p[0]);
                }
                line = reader.readLine();
            }
            reader.close();

        } catch (Exception ex) {
            throw new Exception("Error in fetching available databases");
        }
        return databases;
    }

    public static String login(String database, String userName, String password) {
        String db = "";
        try {
            BufferedReader reader = new BufferedReader(new FileReader("userLogin.txt"));
            String line = reader.readLine();
            while (line != null) {
                if (line.length() > 0) {
                    String[] p = line.split(" ");
                    db = p[0];
                    if (db.equalsIgnoreCase(database)) {
                        if (p[1].equals(userName)) {
                            String decryptPassword = Authorization.decrypt(p[2].getBytes());
                            if (decryptPassword.equals(password)) {
                                System.out.println("Login successful");
                            } else {
                                System.out.println("Login Failed. Please Enter Correct Details");
                            }
                        }
                    }
                }
                line = reader.readLine();
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return db;
    }

    public static void userRegistration(String dbInstance, String userName, String password){
        try {
            String encryptedPassword = Authorization.encrypt(password);
            FileWriter writeFile = new FileWriter("userLogin.txt",true);
            writeFile.append(dbInstance).append(" ").append(userName).append(" ").append(encryptedPassword).append("\n");
            writeFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String encrypt(String password) throws Exception {
        KeyGenerator keyGenerator;
        keyGenerator = KeyGenerator.getInstance("AES");
        keyGenerator.init(256);
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        byte[] key = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        byte[] iv = new byte[cipher.getBlockSize()];
        IvParameterSpec ivspec = new IvParameterSpec(iv);
        SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, ivspec);
        byte[] encryptedPassword = cipher.doFinal(password.getBytes());
        return Base64.getEncoder().encodeToString(encryptedPassword);
    }

    public static String decrypt(byte[] encryptedPassword) throws Exception {
        KeyGenerator keyGenerator;
        keyGenerator = KeyGenerator.getInstance("AES");
        keyGenerator.init(256);
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        byte[] key = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        byte[] iv = new byte[cipher.getBlockSize()];
        IvParameterSpec ivspec = new IvParameterSpec(iv);
        SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
        cipher.init(Cipher.DECRYPT_MODE, secretKey, ivspec);
        byte[] password = cipher.doFinal(Base64.getDecoder().decode(encryptedPassword));
        return new String(password);
    }
}