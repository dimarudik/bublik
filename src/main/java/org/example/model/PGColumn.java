package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class PGColumn {
    private String columnName;
    private String columnType;
}
