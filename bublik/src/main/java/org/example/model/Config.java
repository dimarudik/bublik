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
        String fromTaskWhereClause,
        Map<String, String> columnToColumn,
        Map<String, String> expressionToColumn
) {}
