package com.company.erd;

import java.util.List;
import java.util.Map;

public interface IGenerateERD {

	void generateERD(Map<String, Map<String, Map<String, List<String>>>> tableNameData,
			Map<String, Map<String, String>> primaryData, Map<String, Map<String, List<String>>> foreignKeyData,
			Map<String, Map<String, Map<String, String>>> dataTypeStorage);

}
