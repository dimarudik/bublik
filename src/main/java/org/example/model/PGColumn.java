package org.example.model;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class PGColumn extends Column {
    public PGColumn(Integer columnPosition, String columnName, String columnType) {
        super(columnPosition, columnName, columnType);
    }
}
