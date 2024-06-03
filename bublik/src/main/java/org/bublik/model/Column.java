package org.bublik.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.bublik.service.ColumnService;
import org.bublik.service.SQLSyntaxService;

@AllArgsConstructor
@Getter
public abstract class Column implements SQLSyntaxService, ColumnService {
    private Integer columnPosition;
    private String columnName;
    private String columnType;
}
