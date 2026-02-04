package org.bublik.core.model;

import java.util.List;

public record Column2Column (Column sourceColumn,
                             Column targetColumn,
                             String sourceExpression,
                             List<String> asList,
                             List<String> asSet,
                             List<KV> asMap,
                             List<String> asUDT) {

    public Column2Column(Column sourceColumn, Column targetColumn) {
        this(sourceColumn, targetColumn, null, null, null, null, null);
    }

    public Column2Column(Column sourceColumn, Column targetColumn, String sourceExpression) {
        this(sourceColumn, targetColumn, sourceExpression, null, null, null, null);
    }
}
