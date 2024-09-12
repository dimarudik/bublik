package org.bublik.storage.cassandraaddons;

public class CSPartitionKey {
    private final String type;
    private final String columnName;

    public CSPartitionKey(String type, String columnName) {
        this.type = type;
        this.columnName = columnName;
    }

    public String getType() {
        return type;
    }

    public String getColumnName() {
        return columnName;
    }
}
