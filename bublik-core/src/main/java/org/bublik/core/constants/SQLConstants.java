package org.bublik.core.constants;

public abstract class SQLConstants {
//    public static final String LABEL_ORACLE = SourceContext.Oracle.toString();
//    public static final String LABEL_POSTGRESQL = SourceContext.PostgreSQL.toString();
//    public static final double ROWS_IN_CHUNK = 100000d;
    public static String getTableName(String schemaName, String tableName) {
        return schemaName + "." + tableName;
        }
}
