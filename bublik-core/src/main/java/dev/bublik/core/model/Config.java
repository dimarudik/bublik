package dev.bublik.core.model;

import java.util.ArrayList;
import java.util.HashMap;
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
        Map<String, Object> avroSchema,
        Map<String, String> validationRules
) {

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
                this.avroSchema == null ? null : Map.copyOf(this.avroSchema),
                this.validationRules == null ? null : Map.copyOf(this.validationRules)
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String fromSchemaName;
        private String fromTableName;
        private String fromTableAlias;
        private String fromTableAdds;
        private String toSchemaName;
        private String toTableName;
        private String fetchHintClause;
        private String fetchWhereClause;
        private String fromTaskName;
        private String fromTaskWhereClause;
        private String timestamp;
        private String withTTL;
        private List<String> tryCharIfAny = new ArrayList<>();
        private Map<String, String> columnToColumn = new HashMap<>();
        private Map<String, String> expressionToColumn = new HashMap<>();
        private Map<String, List<String>> columnFromMany = new HashMap<>();
        private Map<String, List<String>> asList = new HashMap<>();
        private Map<String, List<String>> asSet = new HashMap<>();
        private Map<String, List<KV>> asMap = new HashMap<>();
        private Map<String, List<String>> asUDT = new HashMap<>();
        private Map<String, Object> avroSchema = new HashMap<>();
        private Map<String, String> validationRules = new HashMap<>();

        public Builder from(String schema, String table) {
            this.fromSchemaName = schema;
            this.fromTableName = table;
            return this;
        }

        public Builder to(String topic) {
            this.toTableName = topic;
            return this;
        }

        public Builder to(String schema, String table) {
            this.toSchemaName = schema;
            this.toTableName = table;
            return this;
        }

        public Builder fromTableAlias(String fromTableAlias) { this.fromTableAlias = fromTableAlias; return this; }
        public Builder fromTableAdds(String fromTableAdds) { this.fromTableAdds = fromTableAdds; return this; }
        public Builder fetchHintClause(String fetchHintClause) { this.fetchHintClause = fetchHintClause; return this; }
        public Builder fetchWhereClause(String fetchWhereClause) { this.fetchWhereClause = fetchWhereClause; return this; }
        public Builder fromTaskName(String fromTaskName) { this.fromTaskName = fromTaskName; return this; }
        public Builder fromTaskWhereClause(String fromTaskWhereClause) { this.fromTaskWhereClause = fromTaskWhereClause; return this; }
        public Builder timestamp(String timestamp) { this.timestamp = timestamp; return this; }
        public Builder withTTL(String withTTL) { this.withTTL = withTTL; return this; }

        public Builder tryCharIfAny(List<String> tryCharIfAny) { this.tryCharIfAny = tryCharIfAny; return this; }
        public Builder columnToColumn(Map<String, String> columnToColumn) { this.columnToColumn = columnToColumn; return this; }
        public Builder expressionToColumn(Map<String, String> expressionToColumn) { this.expressionToColumn = expressionToColumn; return this; }
        public Builder avroSchema(Map<String, Object> avroSchema) { this.avroSchema = avroSchema; return this; }
        public Builder validationRules(Map<String, String> validationRules) { this.validationRules = validationRules; return this; }

        public Config build() {
            return new Config(
                    fromSchemaName, fromTableName, fromTableAlias, fromTableAdds,
                    toSchemaName, toTableName, fetchHintClause, fetchWhereClause,
                    fromTaskName, fromTaskWhereClause, timestamp, withTTL,
                    tryCharIfAny, columnToColumn, expressionToColumn, columnFromMany,
                    asList, asSet, asMap, asUDT, avroSchema, validationRules
            );
        }
    }
}
