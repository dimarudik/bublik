package org.example.model;

import java.util.Map;

public record Config(
        String numberColumn,
        String fromSchemaName,
        String fromTableName,
        String toSchemaName,
        String toTableName,
        String fetchHintClause,
        String fetchWhereClause,
        String fromTaskName,
//        HashSet<ColumnRule> transformToRule,
        Map<String, String> columnToColumn
//    Map<Integer, ColumnRule> column2Rule,
//    Map<String, String> columnName2columnType
) {}
