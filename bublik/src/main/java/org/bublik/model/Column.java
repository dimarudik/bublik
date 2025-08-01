package org.bublik.model;

import org.bublik.service.ColumnService;
import org.bublik.service.NameSyntaxService;

public class Column implements NameSyntaxService, ColumnService, Comparable<Column> {
    private final Integer columnPosition;
    private final String columnName;
    private final String columnType;
    private final Integer dataType;
    private final Integer isNullable;
    private final String defaultValue;
    private final String isAutoIncrement;
    private final String isGenerated;
    private final int decimalDigits;
    private final String columnComment;
    private final int charOctetLength;

    public Column(Integer columnPosition, String columnName, String columnType, Integer dataType, Integer isNullable,
                  String defaultValue, String isAutoIncrement, String isGenerated, int decimalDigits,
                  String columnComment, int charOctetLength) {
        this.columnPosition = columnPosition;
        this.columnName = columnName;
        this.columnType = columnType;
        this.dataType = dataType;
        this.isNullable = isNullable;
        this.defaultValue = defaultValue;
        this.isAutoIncrement = isAutoIncrement;
        this.isGenerated = isGenerated;
        this.decimalDigits = decimalDigits;
        this.columnComment = columnComment;
        this.charOctetLength = charOctetLength;
    }

    public Integer getColumnPosition() {
        return columnPosition;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public Integer getDataType() {
        return dataType;
    }

    public Integer getIsNullable() {
        return isNullable;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public String getIsAutoIncrement() {
        return isAutoIncrement;
    }

    public String getIsGenerated() {
        return isGenerated;
    }

    public int getDecimalDigits() {
        return decimalDigits;
    }

    public String getColumnComment() {
        return columnComment;
    }

    public int getCharOctetLength() {
        return charOctetLength;
    }

    @Override
    public int compareTo(Column column) {
        return getColumnPosition().compareTo(column.getColumnPosition());
    }
}
