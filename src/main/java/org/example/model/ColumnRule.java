package org.example.model;

//@Data
public class ColumnRule implements Comparable<ColumnRule>{
    private String columnName;
    private String columnType;
    private String ruleDefinition;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public String getRuleDefinition() {
        return ruleDefinition;
    }

    public void setRuleDefinition(String ruleDefinition) {
        this.ruleDefinition = ruleDefinition;
    }

    @Override
    public int compareTo(ColumnRule columnRule) {
        return this.getColumnName().compareTo(columnRule.columnName);
    }
}
