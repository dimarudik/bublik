package org.bublikcassandra.model;

import org.bublik.model.Column;

public class CassandraColumn extends Column {
    public CassandraColumn(Integer columnPosition, String columnName, String columnType) {
        super(columnPosition, columnName, columnType);
    }
}
