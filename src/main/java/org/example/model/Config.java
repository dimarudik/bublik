package org.example.model;

import java.util.HashSet;
import java.util.List;

public record Config(
        String numberColumn,
        String fromSchemaName,
        String fromTableName,
        String toSchemaName,
        String toTableName,
        String fetchHintClause,
        String fetchWhereClause,
        String fromTaskName,
        List<String> excludedSourceColumns,
        List<String> excludedTargetColumns,
        HashSet<ColumnRule> transformToRule
//    Map<Integer, ColumnRule> column2Rule,
//    Map<String, String> columnName2columnType
) {}
