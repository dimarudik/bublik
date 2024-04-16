package org.example.model;

import lombok.Data;
import org.example.service.SQLSyntaxService;

@Data
public abstract class Column implements SQLSyntaxService {
    private String columnName;
    private String columnType;
}
