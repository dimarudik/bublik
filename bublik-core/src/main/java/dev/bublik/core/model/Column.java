package dev.bublik.core.model;

import dev.bublik.core.service.NameSyntaxService;

import java.util.List;

import static dev.bublik.core.constants.Constants.FROM;
import static dev.bublik.core.constants.Constants.TO;

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

    public String getNameWithDigitAscOrDesc() {
        return columnName + " " + ascOrDesc;
    }

    public String getAscOrDesc() {
        return ascOrDesc.equals("0") ? "ASC" : "DESC";
    }

    public String getNameWithAscOrDesc() {
        return columnName + " " + (ascOrDesc.equals("0") ? "ASC" : "DESC");
    }

    public String getNameWithoutQuotes() {
        return getWordWithoutQuotes(columnName);
    }

    public boolean isDescendingBy_0_1() {
        return ascOrDesc.equals("1");
    }

    public boolean nullable() {
        return isNullable != null && isNullable == 1;
    }

    public String fromNameWithType() {
        return fromName() + " " + getType();
    }

    public String toNameWithType() {
        return toName() + " " + getType();
    }

    public String getType() {
        return  columnType + " " +
                (charOctetLength > 0 &&
                        (columnType.equals("varchar")  ||
                                columnType.equals("char")  ||
                                columnType.equals("nchar") ||
                                columnType.equals("nvarchar"))
                        ? "(" + charOctetLength + ")" : "");
    }

    public String fromName() {
        return FROM + columnName();
    }

    public String toName() {
        return TO + columnName();
    }


    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Integer columnPosition = 0;
        private String columnName;
        private String columnType;
        private Integer dataType;
        private Integer isNullable;
        private String defaultValue;
        private String isAutoIncrement;
        private String isGenerated;
        private int decimalDigits = 0;
        private String columnComment;
        private int charOctetLength = 0;
        private String ascOrDesc = "0";
        private boolean isStatic = false;
        private boolean isPartitionKey = false;
        private boolean isClusteringKey = false;
        private UdtType udtType;
        private boolean isFrozen = false;
        private boolean isCollection = false;

        public Builder columnPosition(Integer columnPosition) {
            this.columnPosition = columnPosition;
            return this;
        }

        public Builder columnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public Builder columnType(String columnType) {
            this.columnType = columnType;
            return this;
        }

        public Builder dataType(Integer dataType) {
            this.dataType = dataType;
            return this;
        }

        public Builder isNullable(Integer isNullable) {
            this.isNullable = isNullable;
            return this;
        }

        public Builder defaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder isAutoIncrement(String isAutoIncrement) {
            this.isAutoIncrement = isAutoIncrement;
            return this;
        }

        public Builder isGenerated(String isGenerated) {
            this.isGenerated = isGenerated;
            return this;
        }

        public Builder decimalDigits(int decimalDigits) {
            this.decimalDigits = decimalDigits;
            return this;
        }

        public Builder columnComment(String columnComment) {
            this.columnComment = columnComment;
            return this;
        }

        public Builder charOctetLength(int charOctetLength) {
            this.charOctetLength = charOctetLength;
            return this;
        }

        public Builder ascOrDesc(String ascOrDesc) {
            this.ascOrDesc = ascOrDesc;
            return this;
        }

        public Builder isStatic(boolean isStatic) {
            this.isStatic = isStatic;
            return this;
        }

        public Builder isPartitionKey(boolean isPartitionKey) {
            this.isPartitionKey = isPartitionKey;
            return this;
        }

        public Builder isClusteringKey(boolean isClusteringKey) {
            this.isClusteringKey = isClusteringKey;
            return this;
        }

        public Builder udtType(UdtType udtType) {
            this.udtType = udtType;
            return this;
        }

        public Builder isFrozen(boolean isFrozen) {
            this.isFrozen = isFrozen;
            return this;
        }

        public Builder isCollection(boolean isCollection) {
            this.isCollection = isCollection;
            return this;
        }

        public Column build() {
            if (columnName == null || columnName.isBlank()) {
                throw new IllegalStateException("Missing required field: columnName must be set before calling build()");
            }
            return new Column(
                    columnPosition,
                    columnName,
                    columnType,
                    dataType,
                    isNullable,
                    defaultValue,
                    isAutoIncrement,
                    isGenerated,
                    decimalDigits,
                    columnComment,
                    charOctetLength,
                    ascOrDesc,
                    isStatic,
                    isPartitionKey,
                    isClusteringKey,
                    udtType,
                    isFrozen,
                    isCollection
            );
        }
    }
}
