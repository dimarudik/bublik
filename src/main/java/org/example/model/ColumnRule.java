package org.example.model;

import lombok.Data;

@Data
public class ColumnRule implements Comparable<ColumnRule>{
    private String columnName;
    private String columnType;
    private String ruleDefinition;

    @Override
    public int compareTo(ColumnRule columnRule) {
        return this.getColumnName().compareTo(columnRule.columnName);
    }
}
