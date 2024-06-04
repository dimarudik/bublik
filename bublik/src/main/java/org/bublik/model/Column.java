package org.bublik.model;

import org.bublik.service.ColumnService;
import org.bublik.service.SQLSyntaxService;

public abstract class Column implements SQLSyntaxService, ColumnService {
    private final Integer columnPosition;
    private final String columnName;
    private final String columnType;

    public Column(Integer columnPosition, String columnName, String columnType) {
        this.columnPosition = columnPosition;
        this.columnName = columnName;
        this.columnType = columnType;
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
}
