package org.bublik.core.model;

public record Column2Column (Column sourceColumn,
                             Column targetColumn,
                             String sourceExpression) {
}
