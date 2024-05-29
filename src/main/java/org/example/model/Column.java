package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.service.ColumnService;
import org.example.service.SQLSyntaxService;

@NoArgsConstructor
@AllArgsConstructor
@Data
public abstract class Column implements SQLSyntaxService, ColumnService {
    private Integer columnPosition;
    private String columnName;
    private String columnType;
}
