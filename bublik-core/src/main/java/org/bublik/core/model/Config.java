package org.bublik.core.model;

import java.util.List;
import java.util.Map;

public record Config(
        String numberColumn,
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
        List<String> tryCharIfAny,
        Map<String, String> columnToColumn,
        Map<String, String> expressionToColumn,
        Map<String, EncryptedColumn> expressionToCrypto,
        Map<String, String> cryptoToColumn,
        Map<String, List<String>> columnFromMany,
        List<String> toPrimaryKeys
) {

    public Config copy() {
        return new Config(
                this.numberColumn,
                this.fromSchemaName,
                this.fromTableName,
                this.fromTableAlias,
                this.fromTableAdds,
                this.toSchemaName == null ? this.fromSchemaName : this.toSchemaName,
                this.toTableName == null ? this.fromTableName : this.toTableName,
                this.fetchHintClause,
                this.fetchWhereClause == null ? "1 = 1" : this.fetchWhereClause,
                this.fromTaskName == null ? this.fromSchemaName.replaceAll("^\"|\"$", "") + "_" +
                        this.fromTableName.replaceAll("^\"|\"$", "") + "_" +
                        (this.toTableName == null ? null : this.toTableName.replaceAll("^\"|\"$", "")) + "_task": this.fromTaskName,
                this.fromTaskWhereClause,
                this.tryCharIfAny == null ? null : List.copyOf(this.tryCharIfAny),
                this.columnToColumn == null ? null : Map.copyOf(this.columnToColumn),
                this.expressionToColumn == null ? null : Map.copyOf(this.expressionToColumn),
                this.expressionToCrypto == null ? null : Map.copyOf(this.expressionToCrypto),
                this.cryptoToColumn == null ? null : Map.copyOf(this.cryptoToColumn),
                this.columnFromMany == null ? null : Map.copyOf(this.columnFromMany),
                this.toPrimaryKeys == null ? null : List.copyOf(this.toPrimaryKeys)
        );
    }
}
