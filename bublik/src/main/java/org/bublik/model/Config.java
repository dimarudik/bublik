package org.bublik.model;

import java.util.List;
import java.util.Map;

public record Config(
        String numberColumn,
        String fromSchemaName,
        String fromTableName,
        String fromTableNameAdds,
        String toSchemaName,
        String toTableName,
        String fetchHintClause,
        String fetchWhereClause,
        String fromTaskName,
        String fromTaskWhereClause,
        List<String> tryCharIfAny,
        Map<String, String> columnToColumn,
        Map<String, String> expressionToColumn
) {}
