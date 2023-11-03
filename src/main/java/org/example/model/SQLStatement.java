package org.example.model;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

//@Data
//@AllArgsConstructor
//@NoArgsConstructor
public class SQLStatement {
    private String numberColumn;
    private String fromSchemaName;
    private String fromTableName;
    private String toSchemaName;
    private String toTableName;
    private String fetchHintClause;
    private String fetchWhereClause;
    private String fromTaskName;
    private List<String> sourceColumns;
    private List<String> excludedSourceColumns;
    private List<String> targetColumns;
    private List<String> targetColumnTypes;
    private List<String> excludedTargetColumns;
    private HashSet<ColumnRule> transformToRule;
    private Map<Integer, ColumnRule> column2Rule;
    private Map<String, String> columnName2columnType;

    public SQLStatement(){}
    public SQLStatement(String numberColumn, String fromSchemaName, String fromTableName, String toSchemaName,
                        String toTableName, String fetchHintClause, String fetchWhereClause, String fromTaskName,
                        List<String> sourceColumns, List<String> excludedSourceColumns, List<String> targetColumns,
                        List<String> targetColumnTypes, List<String> excludedTargetColumns,
                        HashSet<ColumnRule> transformToRule, Map<Integer, ColumnRule> column2Rule,
                        Map<String, String> columnName2columnType) {
        this.numberColumn = numberColumn;
        this.fromSchemaName = fromSchemaName;
        this.fromTableName = fromTableName;
        this.toSchemaName = toSchemaName;
        this.toTableName = toTableName;
        this.fetchHintClause = fetchHintClause;
        this.fetchWhereClause = fetchWhereClause;
        this.fromTaskName = fromTaskName;
        this.sourceColumns = sourceColumns;
        this.excludedSourceColumns = excludedSourceColumns;
        this.targetColumns = targetColumns;
        this.targetColumnTypes = targetColumnTypes;
        this.excludedTargetColumns = excludedTargetColumns;
        this.transformToRule = transformToRule;
        this.column2Rule = column2Rule;
        this.columnName2columnType = columnName2columnType;
    }

    public String getNumberColumn() {
        return numberColumn;
    }

    public void setNumberColumn(String numberColumn) {
        this.numberColumn = numberColumn;
    }

    public String getFromSchemaName() {
        return fromSchemaName;
    }

    public void setFromSchemaName(String fromSchemaName) {
        this.fromSchemaName = fromSchemaName;
    }

    public String getFromTableName() {
        return fromTableName;
    }

    public void setFromTableName(String fromTableName) {
        this.fromTableName = fromTableName;
    }

    public String getToSchemaName() {
        return toSchemaName;
    }

    public void setToSchemaName(String toSchemaName) {
        this.toSchemaName = toSchemaName;
    }

    public String getToTableName() {
        return toTableName;
    }

    public void setToTableName(String toTableName) {
        this.toTableName = toTableName;
    }

    public String getFetchHintClause() {
        return fetchHintClause;
    }

    public void setFetchHintClause(String fetchHintClause) {
        this.fetchHintClause = fetchHintClause;
    }

    public String getFetchWhereClause() {
        return fetchWhereClause;
    }

    public void setFetchWhereClause(String fetchWhereClause) {
        this.fetchWhereClause = fetchWhereClause;
    }

    public String getFromTaskName() {
        return fromTaskName;
    }

    public void setFromTaskName(String fromTaskName) {
        this.fromTaskName = fromTaskName;
    }

    public List<String> getSourceColumns() {
        return sourceColumns;
    }

    public void setSourceColumns(List<String> sourceColumns) {
        this.sourceColumns = sourceColumns;
    }

    public List<String> getExcludedSourceColumns() {
        return excludedSourceColumns;
    }

    public void setExcludedSourceColumns(List<String> excludedSourceColumns) {
        this.excludedSourceColumns = excludedSourceColumns;
    }

    public List<String> getTargetColumns() {
        return targetColumns;
    }

    public void setTargetColumns(List<String> targetColumns) {
        this.targetColumns = targetColumns;
    }

    public List<String> getTargetColumnTypes() {
        return targetColumnTypes;
    }

    public void setTargetColumnTypes(List<String> targetColumnTypes) {
        this.targetColumnTypes = targetColumnTypes;
    }

    public List<String> getExcludedTargetColumns() {
        return excludedTargetColumns;
    }

    public void setExcludedTargetColumns(List<String> excludedTargetColumns) {
        this.excludedTargetColumns = excludedTargetColumns;
    }

    public HashSet<ColumnRule> getTransformToRule() {
        return transformToRule;
    }

    public void setTransformToRule(HashSet<ColumnRule> transformToRule) {
        this.transformToRule = transformToRule;
    }

    public Map<Integer, ColumnRule> getColumn2Rule() {
        return column2Rule;
    }

    public void setColumn2Rule(Map<Integer, ColumnRule> column2Rule) {
        this.column2Rule = column2Rule;
    }

    public Map<String, String> getColumnName2columnType() {
        return columnName2columnType;
    }

    public void setColumnName2columnType(Map<String, String> columnName2columnType) {
        this.columnName2columnType = columnName2columnType;
    }

    public List<String> getNeededSourceColumns() {
        return getFields(getSourceColumns(), getExcludedSourceColumns());
    }

    public List<String> getNeededTargetColumns() {
        return getFields(getTargetColumns(), getExcludedTargetColumns());
    }

    private synchronized List<String> getFields(List<String> columns, List<String> excludedColumns) {
        if (excludedColumns != null) {
            columns.removeAll(excludedColumns);
        }
        if (targetColumnTypes != null && !targetColumnTypes.isEmpty()) {
            setTargetColumnTypes(targetColumnTypes.subList(0, columns.size()));
        }
        return columns;
    }
}
