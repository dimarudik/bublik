package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.example.service.ColumnService;
import org.example.service.SQLSyntaxService;

@AllArgsConstructor
@Getter
public abstract class Column implements SQLSyntaxService, ColumnService {
    private Integer columnPosition;
    private String columnName;
    private String columnType;
}
