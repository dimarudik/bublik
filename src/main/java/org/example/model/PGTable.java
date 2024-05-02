package org.example.model;

import java.sql.Connection;
import java.util.Map;

public class PGTable extends Table {
    @Override
    public boolean exists(Connection connection) {
        return false;
    }

    @Override
    public String getFinalTableName(boolean withQuotes) {
        return null;
    }

    @Override
    public String getFinalSchemaName() {
        return "";
    }

    @Override
    public String getHintClause() {
        return " ";
    }

    @Override
    public String getTaskName() {
        return getFinalTableName(false).toUpperCase() + "_TASK";
    }

    @Override
    public Map<String, String> getColumnToColumn(Connection connection) {
        return Map.of();
    }
}
