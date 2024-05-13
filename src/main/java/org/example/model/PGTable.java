package org.example.model;

import lombok.NoArgsConstructor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@NoArgsConstructor
public class PGTable extends Table {
    public PGTable(String schemaName, String tableName) {
        super(schemaName, tableName);
    }

    @Override
    public boolean exists(Connection connection) throws SQLException {
        if (tableExistsCache().contains(this.getTableName())) {
            return true;
        }
        ResultSet tablesLowCase = connection.getMetaData().getTables(
                null,
                this.getSchemaName().toLowerCase(),
                getFinalTableName(false),
                null);
        if (!tablesLowCase.next()) {
            tablesLowCase.close();
            return false;
        }
        tablesLowCase.close();
        tableExistsCache().add(this.getTableName());
        return true;
    }

    @Override
    public String getFinalTableName(boolean withQuotes) {
        String tableName = withQuotes ? getTableName() : getWordWithoutQuotes(getTableName());
        return  isCaseSensitiveWord(getTableName()) ? tableName : getTableName().toLowerCase();
    }

    @Override
    public String getFinalSchemaName() {
        return getSchemaName().toLowerCase();
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
    public Map<String, String> getColumnToColumn(Connection connection) throws SQLException {
        Map<String, String> map = new HashMap<>();
        ResultSet columnsLowCase = connection.getMetaData().getColumns(
                null,
                getFinalSchemaName(),
                getFinalTableName(false),
                null);
        while (columnsLowCase.next()) {
            String columnName = columnsLowCase.getString(4);
            String finalColumnName = columnName.equals(columnName.toLowerCase()) ? columnName : "\"" + columnName + "\"";
//            String finalColumnName = columnName;
            map.put(finalColumnName, finalColumnName);
        }
        return map;
    }
}
