package dev.bublik.core.model;

import dev.bublik.core.service.NameSyntaxService;

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
                     UdtType udtType,
                     boolean isFrozen,
                     boolean isCollection) implements NameSyntaxService, Comparable<Column> {

    public record UdtType(String typeName, List<Column> columns) {}

    public Column(String columnName, String columnType) {
        this(0, columnName, columnType, null, null, null, null, null, 0, null, 0,  null, false, false, false, null, false, false);
    }

    public Column(Integer columnPosition,
                  String columnName) {
        this(columnPosition, columnName, null, null, null, null, null, null, 0, null, 0,  null, false, false, false, null, false, false);
    }

    // For MSSQL
    public Column(Integer columnPosition,
                  String columnName,
                  String columnType,
                  Integer isNullable,
                  int charOctetLength,
                  String ascOrDesc) {
        this(columnPosition, columnName, columnType, null, isNullable, null, null, null, 0, null, charOctetLength,  ascOrDesc, false, false, false, null, false, false);
    }

    public Column(Integer columnPosition,
                  String columnName,
                  String columnType,
                  String defaultValue) {
        this(columnPosition, columnName, columnType, null, null, defaultValue, null, null, 0, null, 0,  null, false, false, false, null, false, false);
    }

    public Column(Integer columnPosition,
                  String columnName,
                  String columnType,
                  String ascOrDesc,
                  boolean isStatic,
                  boolean isPartitionKey,
                  boolean isClusteringKey,
                  UdtType udtType,
                  boolean isFrozen,
                  boolean isCollection) {
        this(columnPosition, columnName, columnType, null, null, null, null, null, 0, null, 0,  ascOrDesc, isStatic, isPartitionKey, isClusteringKey, udtType, isFrozen, isCollection);
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
        this(columnPosition, columnName, columnType, dataType, isNullable, defaultValue, isAutoIncrement, isGenerated, decimalDigits, columnComment, charOctetLength, ascOrDesc, isStatic, isPartitionKey, isClusteringKey, null, false, false);
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

    public boolean isDescendingBy_0_1() {
        return ascOrDesc.equals("1");
    }

    public boolean nullable() {
        return isNullable != null && isNullable == 1;
    }

    public String fromColumnNameWithType() {
        return fromColumnName() + " " + getColumnType();
    }

    public String toColumnNameWithType() {
        return toColumnName() + " " + getColumnType();
    }

    public String getColumnType() {
        return  columnType + " " +
                (charOctetLength > 0 &&
                        (columnType.equals("varchar")  ||
                                columnType.equals("char")  ||
                                columnType.equals("nchar") ||
                                columnType.equals("nvarchar"))
                        ? "(" + charOctetLength + ")" : "");
    }

    public String fromColumnName() {
        return "from_" + columnName();
    }

    public String toColumnName() {
        return "to_" + columnName();
    }
}
