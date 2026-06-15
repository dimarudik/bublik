package dev.bublik.core.model;

import java.util.List;
import java.util.Map;

import static dev.bublik.core.constants.Constants.DEFAULT_FETCH_WHERE_CLAUSE;

public record Config(
        String fromSchemaName,
        String fromTableName,
        String fromTableAlias,
        String fromTableAdds,
        String toSchemaName,
        String toTableName,
        String fetchHintClause,
        String fetchWhereClause,
        String fromTaskName,
        String fromTaskWhereClause,
        String timestamp,
        String withTTL,
        List<String> tryCharIfAny,
        Map<String, String> columnToColumn,
        Map<String, String> expressionToColumn,
        Map<String, List<String>> columnFromMany,
        Map<String, List<String>> asList,
        Map<String, List<String>> asSet,
        Map<String, List<KV>> asMap,
        Map<String, List<String>> asUDT,
        Map<String, String> cacheToQuery
) {

    public Config(String fromSchemaName, String fromTableName, String fromTableAlias, String fromTableAdds, String toSchemaName, String toTableName, String fetchWhereClause, List<String> tryCharIfAny, Map<String, String> columnToColumn, Map<String, String> expressionToColumn) {
        this(fromSchemaName, fromTableName, fromTableAlias, fromTableAdds, toSchemaName, toTableName, null, fetchWhereClause, null, null, null, null, tryCharIfAny, columnToColumn, expressionToColumn, null, null, null, null, null, null);
    }

    public Config(String fromSchemaName, String fromTableName, String fromTableAlias, String fromTableAdds, String toSchemaName,
            String toTableName, String fetchHintClause, String fetchWhereClause, String fromTaskName, String fromTaskWhereClause,
            String timestamp, String withTTL, List<String> tryCharIfAny, Map<String, String> columnToColumn,
            Map<String, String> expressionToColumn, Map<String, List<String>> columnFromMany, Map<String, String> cacheToQuery) {
        this(fromSchemaName, fromTableName, fromTableAlias, fromTableAdds, toSchemaName, toTableName, fetchHintClause, fetchWhereClause,
                fromTaskName, fromTaskWhereClause, timestamp, withTTL, tryCharIfAny, columnToColumn, expressionToColumn, columnFromMany,
                null, null, null, null, cacheToQuery);
    }

    public Config copy() {
        return new Config(
                this.fromSchemaName,
                this.fromTableName,
                this.fromTableAlias,
                this.fromTableAdds,
                this.toSchemaName == null ? this.fromSchemaName : this.toSchemaName,
                this.toTableName == null ? this.fromTableName : this.toTableName,
                this.fetchHintClause,
                this.fetchWhereClause == null ? DEFAULT_FETCH_WHERE_CLAUSE : this.fetchWhereClause,
                this.fromTaskName == null ? this.fromSchemaName.replaceAll("^\"|\"$", "") + "_" +
                        this.fromTableName.replaceAll("^\"|\"$", "") + "_" +
                        (this.toTableName == null ? null : this.toTableName.replaceAll("^\"|\"$", "")) + "_task": this.fromTaskName,
                this.fromTaskWhereClause,
                this.timestamp,
                this.withTTL,
                this.tryCharIfAny == null ? null : List.copyOf(this.tryCharIfAny),
                this.columnToColumn == null ? null : Map.copyOf(this.columnToColumn),
                this.expressionToColumn == null ? null : Map.copyOf(this.expressionToColumn),
                this.columnFromMany == null ? null : Map.copyOf(this.columnFromMany),
                this.asList,
                this.asSet,
                this.asMap,
                this.asUDT,
                this.cacheToQuery == null ? null : Map.copyOf(this.cacheToQuery)
        );
    }
}
