package com.company.selectFromTable;

import java.util.List;
import java.util.Map;

public interface ISelectColumn {
    void selectColumn(Map<String, List<String>> tableValue, String column, String whereCondition);
}
