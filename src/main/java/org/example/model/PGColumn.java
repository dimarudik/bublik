package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
public class PGColumn extends Column {
    public PGColumn(Integer columnPosition, String columnName, String columnType) {
        super(columnPosition, columnName, columnType);
    }
}
