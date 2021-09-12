package com.company.dataStorage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface IFileData {
    Map<String, Map<String, Map<String, List<String>>>> readFromFile(String database, String tableName) throws FileNotFoundException;

    void writeToFile(Map<String, Map<String, Map<String, List<String>>>> dataStructure) throws IOException;

    void writeForeignKeyToFile(Map<String, Map<String, List<String>>> foreignKeyData);

    Map<String, Map<String, List<String>>> readForeignKeysFromFile(String database) throws FileNotFoundException;

    boolean writePrimaryKeysToFile(Map<String, Map<String, String>> primaryData) throws Exception;

    Map<String, Map<String, String>> readPrimaryKeyMeta(String currentDatabase) throws Exception;

	List<String> readSQLDumpFromFile(String currentDatabase);

    Map<String, Map<String, Map<String, List<String>>>> readRemoteFile(List<String> fileLines, String tableName, String remoteInstance);

    String writeRemoteFile (Map<String, Map<String, Map<String, List<String>>>> dataStructure);
}
