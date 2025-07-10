package org.bublik.model;

import org.bublik.service.ColumnService;
import org.bublik.service.SQLSyntaxService;

public class Column implements SQLSyntaxService, ColumnService {
    private final Integer columnPosition;
    private final String columnName;
    private final String columnType;
    private final Integer dataType;

    public Column(Integer columnPosition, String columnName, String columnType, Integer dataType) {
        this.columnPosition = columnPosition;
        this.columnName = columnName;
        this.columnType = columnType;
        this.dataType = dataType;
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
}
