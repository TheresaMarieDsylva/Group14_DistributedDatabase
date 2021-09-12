package com.company.gcpConfiguration;

import java.io.File;
import java.io.InputStream;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

import com.jcraft.jsch.ChannelSftp;

public class GCPRemoteDBReaderWriter {

    public List<String> readFile(String fileName) {
        List<String> fileLines = new ArrayList<>();

        try {
            String filePath = GCPConfiguration.REMOTE_DIRECTORY + File.separator + fileName;

            ChannelSftp sftpChannel = GCPRemoteConnection.getsftpChannelOfDBToConnect();
            InputStream stream = sftpChannel.get(filePath);

            Path tempFile = Paths.get(GCPConfiguration.LOCAL_DIRECTORY + ".tmp");
            Files.copy(stream, tempFile, StandardCopyOption.REPLACE_EXISTING);
            fileLines = Files.readAllLines(tempFile);

            Files.deleteIfExists(tempFile);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return fileLines;
    }

    public void writeFile(String fileName, String fileContent) {
        try {
            String filePath = GCPConfiguration.REMOTE_DIRECTORY + File.separator + fileName;

            String tempFilePath = GCPConfiguration.REMOTE_DIRECTORY + ".tmp";
            Path tempFile = Paths.get(tempFilePath);
            Files.createFile(tempFile);
            Files.write(tempFile, fileContent.getBytes(), StandardOpenOption.TRUNCATE_EXISTING);

            ChannelSftp sftpChannel = GCPRemoteConnection.getsftpChannelOfDBToConnect();
            sftpChannel.put(tempFilePath, filePath);

            Files.deleteIfExists(tempFile);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    public void writeFile(String fileName) {
        try {
            String filePath = GCPConfiguration.REMOTE_DIRECTORY + File.separator + fileName;

            ChannelSftp sftpChannel = GCPRemoteConnection.getsftpChannelOfDBToConnect();

            if (fileName.equalsIgnoreCase("gdd.csv")) {
                sftpChannel.put(fileName, filePath);
            } else {
                fileName = "local\\" + fileName;
                sftpChannel.put(fileName, filePath);
            }

        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    public boolean deleteGDDFile(String fileName) {
        try {
            String filePath = GCPConfiguration.REMOTE_DIRECTORY + File.separator + fileName;

            ChannelSftp sftpChannel = GCPRemoteConnection.getsftpChannelOfDBToConnect();
            sftpChannel.rm(filePath);

            return true;
        } catch (Exception exception) {
            exception.printStackTrace();
            return false;
        }
    }
}