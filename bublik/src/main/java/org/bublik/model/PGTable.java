package org.bublik.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PGTable extends Table {
    private static final Logger log = LoggerFactory.getLogger(PGTable.class);

    public PGTable(){}
    public PGTable(String schemaName, String tableName) {
        super(schemaName, tableName);
    }

    @Override
    public boolean exists(Connection connection) throws SQLException {
/*
        if (tableExistsCache().contains(getFinalTableName(false))) {
            return true;
        }
*/
        ResultSet tablesLowCase = connection.getMetaData().getTables(
                null,
                getFinalSchemaName(false),
                getFinalTableName(false),
                null);
        if (!tablesLowCase.next()) {
            tablesLowCase.close();
            return false;
        }
        tablesLowCase.close();
        tableExistsCache().add(getFinalTableName(false));
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

    public String getFinalSchemaName(boolean withQuotes) {
        String schemaName = withQuotes ? getSchemaName() : getWordWithoutQuotes(getSchemaName());
        return  isCaseSensitiveWord(getSchemaName()) ? schemaName : getSchemaName().toLowerCase();
    }

    @Override
    public String getHintClause() {
        return " ";
    }

    @Override
    public List<Column> getPKColumns(Connection connection) throws SQLException {
        List<Column> pkColumns = new ArrayList<>();
        ResultSet rs = connection.getMetaData().getPrimaryKeys(
                null, getFinalSchemaName(), getFinalTableName(false));
        while (rs.next()) {
            Integer keySeq = rs.getInt("KEY_SEQ");
            String pkName = rs.getString("PK_NAME");
            String columnName = rs.getString("COLUMN_NAME");
//            log.info("{}: {} - {}", pkName, keySeq, columnName);
            pkColumns.add(new Column(keySeq, columnName, null, null));
        }
        return pkColumns;
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
            map.put(finalColumnName, finalColumnName);
        }
        return map;
    }
}
