package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
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
