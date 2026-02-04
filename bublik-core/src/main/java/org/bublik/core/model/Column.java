package org.bublik.core.model;

import org.bublik.core.service.NameSyntaxService;

import java.util.List;

public record Column(Integer columnPosition,
                     String columnName,
                     String columnType,
                     Integer dataType,
                     Integer isNullable,
                     String defaultValue,
                     String isAutoIncrement,
                     String isGenerated,
                     int decimalDigits,
                     String columnComment,
                     int charOctetLength,
                     String ascOrDesc,
                     boolean isStatic,
                     boolean isPartitionKey,
                     boolean isClusteringKey,
                     UdtType udtType) implements NameSyntaxService, Comparable<Column> {

    public record UdtType(String typeName, List<Column> columns) {}

    public Column(String columnName, String columnType) {
        this(0, columnName, columnType, null, null, null, null, null, 0, null, 0,  null, false, false, false, null);
    }

    public Column(Integer columnPosition,
                  String columnName,
                  String columnType,
                  String ascOrDesc,
                  boolean isStatic,
                  boolean isPartitionKey,
                  boolean isClusteringKey,
                  UdtType udtType) {
        this(columnPosition, columnName, columnType, null, null, null, null, null, 0, null, 0,  ascOrDesc, isStatic, isPartitionKey, isClusteringKey, udtType);
    }

    public Column(Integer columnPosition,
                  String columnName,
                  String columnType,
                  Integer dataType,
                  Integer isNullable,
                  String defaultValue,
                  String isAutoIncrement,
                  String isGenerated,
                  int decimalDigits,
                  String columnComment,
                  int charOctetLength,
                  String ascOrDesc,
                  boolean isStatic,
                  boolean isPartitionKey,
                  boolean isClusteringKey) {
        this(columnPosition, columnName, columnType, dataType, isNullable, defaultValue, isAutoIncrement, isGenerated, decimalDigits, columnComment, charOctetLength, ascOrDesc, isStatic, isPartitionKey, isClusteringKey, null);
    }

    @Override
    public int compareTo(Column column) {
        return columnPosition().compareTo(column.columnPosition());
    }

    public String getColumnNameWithAscOrDesc() {
        return columnName + " " + ascOrDesc;
    }

    public String getColumnNameWithoutQuotes() {
        return getWordWithoutQuotes(columnName);
    }
}
