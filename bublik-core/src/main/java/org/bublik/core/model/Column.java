package org.bublik.core.model;

import org.bublik.core.service.NameSyntaxService;

public record Column(Integer columnPosition, String columnName, String columnType, Integer dataType, Integer isNullable,
                     String defaultValue, String isAutoIncrement, String isGenerated, int decimalDigits,
                     String columnComment, int charOctetLength,
                     String ascOrDesc) implements NameSyntaxService, Comparable<Column> {

    @Override
    public int compareTo(Column column) {
        return columnPosition().compareTo(column.columnPosition());
    }

    public String getColumnNameWithAscOrDesc() {
        return columnName + " " + ascOrDesc;
    }
}
