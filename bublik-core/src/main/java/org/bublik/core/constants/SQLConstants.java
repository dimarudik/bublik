package org.bublik.core.constants;

public abstract class SQLConstants {
    public static String getTableName(String schemaName, String tableName) {
        return schemaName + "." + tableName;
        }
}
