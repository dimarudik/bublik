package org.bublik.core.model;

import java.util.List;
import java.util.Map;

public record Column2Column (Column sourceColumn,
                             Column targetColumn,
                             String sourceExpression,
                             List<String> asList,
                             List<String> asSet,
                             List<KV> asMap) {
}
